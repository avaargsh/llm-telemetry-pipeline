package genaiobs

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
)

// OTLPCredential binds a bearer secret to one observation tenant, not a billing principal.
type OTLPCredential struct{ Token, TenantID string }
type otlpPrincipal struct {
	hash  [32]byte
	scope Scope
}
type otlpHTTPHandler struct {
	admitter   ObservationAdmitter
	principals []otlpPrincipal
	slots      chan struct{}
}

// NewOTLPHTTPHandler constructs an opt-in prefiltered JSON receiver. The caller
// must configure TLS, HTTP timeouts and header/connection limits before serving.
// A successful response acknowledges volatile queue admission only.
func NewOTLPHTTPHandler(q *Queue, creds []OTLPCredential, concurrent int) (http.Handler, error) {
	return NewOTLPHTTPHandlerWithAdmitter(q, creds, concurrent)
}

// NewDurableOTLPHTTPHandler admits authenticated OTLP projections directly to
// PostgreSQL. A 200 response acknowledges the durable transaction commit.
func NewDurableOTLPHTTPHandler(store *DurableStore, creds []OTLPCredential, concurrent int) (http.Handler, error) {
	return NewOTLPHTTPHandlerWithAdmitter(store, creds, concurrent)
}

// NewOTLPHTTPHandlerWithAdmitter shares wire/auth limits across volatile and
// durable admission backends. The backend must provide atomic batch admission.
func NewOTLPHTTPHandlerWithAdmitter(admitter ObservationAdmitter, creds []OTLPCredential, concurrent int) (http.Handler, error) {
	if admitter == nil || admitter.MaxBatchSize() < 1 || len(creds) == 0 || len(creds) > 128 || concurrent < 1 || concurrent > 64 {
		return nil, errors.New("invalid observation receiver configuration")
	}
	h := &otlpHTTPHandler{admitter: admitter, slots: make(chan struct{}, concurrent)}
	seen := make(map[[32]byte]bool, len(creds))
	for _, c := range creds {
		if !validOTLPToken(c.Token) || !identifier(c.TenantID, 128) {
			return nil, errors.New("invalid observation credential")
		}
		hash := sha256.Sum256([]byte(c.Token))
		if seen[hash] {
			return nil, errors.New("duplicate observation credential")
		}
		seen[hash] = true
		h.principals = append(h.principals, otlpPrincipal{hash: hash, scope: Scope{TenantID: strings.Clone(c.TenantID)}})
	}
	return h, nil
}
func validOTLPToken(s string) bool {
	if len(s) < 32 || len(s) > 512 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 33 || s[i] > 126 || s[i] == ',' {
			return false
		}
	}
	return true
}
func (h *otlpHTTPHandler) authenticate(header http.Header) (Scope, bool) {
	values := header.Values("Authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return Scope{}, false
	}
	token := strings.TrimPrefix(values[0], "Bearer ")
	if !validOTLPToken(token) {
		return Scope{}, false
	}
	digest := sha256.Sum256([]byte(token))
	var scope Scope
	matched := false
	for _, p := range h.principals {
		if subtle.ConstantTimeCompare(digest[:], p.hash[:]) == 1 {
			scope = p.scope
			matched = true
		}
	}
	return scope, matched
}
func (h *otlpHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.URL.Path != "/v1/traces" {
		otlpHTTPError(w, 404, "not found")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		otlpHTTPError(w, 405, "method not allowed")
		return
	}
	scope, ok := h.authenticate(r.Header)
	if !ok {
		w.Header().Set("WWW-Authenticate", `Bearer realm="genai-observations"`)
		otlpHTTPError(w, 401, "unauthorized")
		return
	}
	media := r.Header.Values("Content-Type")
	if len(media) != 1 {
		otlpHTTPError(w, 415, "unsupported media type")
		return
	}
	mt, params, err := mime.ParseMediaType(media[0])
	if err != nil || mt != "application/json" || (len(params) > 0 && (len(params) != 1 || !strings.EqualFold(params["charset"], "utf-8"))) {
		otlpHTTPError(w, 415, "unsupported media type")
		return
	}
	enc := r.Header.Values("Content-Encoding")
	if len(enc) > 1 || (len(enc) == 1 && !strings.EqualFold(enc[0], "identity")) {
		otlpHTTPError(w, 415, "unsupported content encoding")
		return
	}
	if r.ContentLength > MaxOTLPJSONBytes {
		otlpHTTPError(w, 413, "request exceeds limit")
		return
	}
	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	default:
		otlpHTTPOverload(w)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxOTLPJSONBytes))
	if err != nil {
		var limit *http.MaxBytesError
		if errors.As(err, &limit) {
			otlpHTTPError(w, 413, "request exceeds limit")
		} else {
			otlpHTTPError(w, 400, "invalid request")
		}
		return
	}
	spans, err := DecodeOTLPJSON(body)
	if errors.Is(err, ErrOTLPJSONLimit) {
		otlpHTTPError(w, 413, "request exceeds limit")
		return
	}
	if err != nil {
		otlpHTTPError(w, 400, "invalid request")
		return
	}
	// Queue capacity is immutable; a batch larger than capacity cannot succeed on retry.
	if len(spans) > h.admitter.MaxBatchSize() {
		otlpHTTPError(w, 413, "batch exceeds queue capacity")
		return
	}
	if r.Context().Err() != nil {
		otlpHTTPError(w, 503, "request canceled")
		return
	}
	idempotencyHeaders := r.Header.Values("Idempotency-Key")
	if len(idempotencyHeaders)>1 {
		otlpHTTPError(w,http.StatusBadRequest,"invalid idempotency key")
		return
	}
	var admitErr error
	if len(idempotencyHeaders)==1 {
		key:=idempotencyHeaders[0]
		if !validIdempotencyKey(key) || len(spans)!=1 {
			otlpHTTPError(w,http.StatusBadRequest,"invalid idempotency key")
			return
		}
		idempotent,ok:=h.admitter.(IdempotentObservationAdmitter)
		if !ok {
			otlpHTTPError(w,http.StatusNotImplemented,"durable idempotency is not supported")
			return
		}
		admitErr=idempotent.AdmitBatchWithIdempotencyKey(r.Context(),scope,Version,spans,key)
	} else {
		admitErr=h.admitter.AdmitBatch(r.Context(),scope,Version,spans)
	}
	if admitErr!=nil {
		switch {
		case errors.Is(admitErr,ErrQueueFull),errors.Is(admitErr,ErrDurableQueueFull):
			otlpHTTPOverload(w)
		case errors.Is(admitErr,ErrObservationConflict):
			otlpHTTPError(w,http.StatusConflict,"observation identity conflict")
		case errors.Is(admitErr,ErrInvalid),errors.Is(admitErr,ErrInvalidBatchSize),errors.Is(admitErr,ErrUnsupportedVersion),errors.Is(admitErr,ErrInvalidIdempotencyKey):
			otlpHTTPError(w,http.StatusBadRequest,"invalid observation")
		default:
			// Database and other backend failures are retryable service errors,
			// never mislabeled as malformed client input.
			otlpHTTPOverload(w)
		}
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "{}\n")
}
func otlpHTTPOverload(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "1")
	otlpHTTPError(w, 503, "observation receiver overloaded")
}
func otlpHTTPError(w http.ResponseWriter, status int, message string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Message string `json:"message"`
	}{message})
}
