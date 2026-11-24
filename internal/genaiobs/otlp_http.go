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
	queue      *Queue
	principals []otlpPrincipal
	slots      chan struct{}
}

// NewOTLPHTTPHandler constructs an opt-in prefiltered JSON receiver. The caller
// must configure TLS, HTTP timeouts and header/connection limits before serving.
// A successful response acknowledges volatile queue admission only.
func NewOTLPHTTPHandler(q *Queue, creds []OTLPCredential, concurrent int) (http.Handler, error) {
	if q == nil || len(q.slots) == 0 || len(creds) == 0 || len(creds) > 128 || concurrent < 1 || concurrent > 64 {
		return nil, errors.New("invalid observation receiver configuration")
	}
	h := &otlpHTTPHandler{queue: q, slots: make(chan struct{}, concurrent)}
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
	if len(spans) > len(h.queue.slots) {
		otlpHTTPError(w, 413, "batch exceeds queue capacity")
		return
	}
	if r.Context().Err() != nil {
		otlpHTTPError(w, 503, "request canceled")
		return
	}
	if err := h.queue.SubmitBatch(scope, Version, spans); err != nil {
		if errors.Is(err, ErrQueueFull) {
			otlpHTTPOverload(w)
		} else {
			otlpHTTPError(w, 400, "invalid request")
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
