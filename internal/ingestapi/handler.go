// Package ingestapi accepts authenticated, sealed usage events into the
// authoritative PostgreSQL Ledger + Outbox. Kafka delivery is asynchronous.
package ingestapi

import (
    "bytes"
    "context"
    "crypto/sha256"
    "crypto/subtle"
    "encoding/json"
    "errors"
    "io"
    "mime"
    "net/http"
    "strings"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
    "github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
)

// MaxBodyBytes bounds a single JSON event; larger requests are rejected.
const MaxBodyBytes = 64 << 10

// Credential binds an opaque bearer secret to exactly one tenant/project.
// Never put real tokens in source control or log this struct.
type Credential struct {
    Token string `json:"token"`
    TenantID string `json:"tenant_id"`
    ProjectID string `json:"project_id"`
}

type Recorder interface {
    Record(context.Context, event.UsageEvent) (ledger.Outcome, error)
}

type principal struct {
    tokenHash [32]byte
    tenantID string
    projectID string
}

type handler struct {
    recorder Recorder
    principals []principal
}

// NewHandler requires explicit credential bindings. A SHA-256 payload seal
// detects mutation but does NOT authenticate a caller; bearer auth does.
func NewHandler(recorder Recorder, credentials []Credential) (http.Handler, error) {
    if recorder == nil || len(credentials) == 0 {
        return nil, errors.New("recorder and at least one credential are required")
    }
    principals, err := newPrincipals(credentials)
    if err != nil { return nil, err }
    return &handler{recorder: recorder, principals: principals}, nil
}

func newPrincipals(credentials []Credential) ([]principal, error) {
    if len(credentials) == 0 { return nil, errors.New("at least one credential is required") }
    principals := make([]principal, 0, len(credentials))
    seen := make(map[[32]byte]struct{}, len(credentials))
    for _, credential := range credentials {
        if len(credential.Token) < 32 || strings.TrimSpace(credential.Token) != credential.Token ||
            strings.ContainsAny(credential.Token, " \t\r\n") ||
            strings.TrimSpace(credential.TenantID) == "" ||
            strings.TrimSpace(credential.ProjectID) == "" {
            return nil, errors.New("invalid credential: secret must be at least 32 bytes and scope must be non-empty")
        }
        digest := sha256.Sum256([]byte(credential.Token))
        if _, exists := seen[digest]; exists {
            return nil, errors.New("duplicate credential token")
        }
        seen[digest] = struct{}{}
        principals = append(principals, principal{tokenHash: digest, tenantID: credential.TenantID, projectID: credential.ProjectID})
    }
    return principals, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Cache-Control", "no-store")
    w.Header().Set("X-Content-Type-Options", "nosniff")
    switch r.URL.Path {
    case "/healthz":
        if r.Method != http.MethodGet {
            w.Header().Set("Allow", http.MethodGet)
            writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
            return
        }
        writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
    case "/v1/usage-events":
        if r.Method != http.MethodPost {
            w.Header().Set("Allow", http.MethodPost)
            writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
            return
        }
        h.ingest(w, r)
    default:
        writeError(w, http.StatusNotFound, "not_found")
    }
}

func (h *handler) authenticate(header http.Header) *principal {
    return authenticate(h.principals, header)
}

func authenticate(principals []principal, header http.Header) *principal {
    values := header.Values("Authorization")
    if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
        return nil
    }
    token := strings.TrimPrefix(values[0], "Bearer ")
    if token == "" || strings.ContainsAny(token, " \t\r\n") {
        return nil
    }
    digest := sha256.Sum256([]byte(token))
    var matched *principal
    // Always check all configured hashes; do not short-circuit on a match.
    for i := range principals {
        if subtle.ConstantTimeCompare(digest[:], principals[i].tokenHash[:]) == 1 {
            matched = &principals[i]
        }
    }
    return matched
}

func (h *handler) ingest(w http.ResponseWriter, r *http.Request) {
    principal := h.authenticate(r.Header)
    if principal == nil {
        w.Header().Set("WWW-Authenticate", "Bearer realm=\"usage-ingest\"")
        writeError(w, http.StatusUnauthorized, "unauthorized")
        return
    }
    mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
    if err != nil || mediaType != "application/json" {
        writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type")
        return
    }
    raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
    if err != nil {
        writeDecodeError(w, err)
        return
    }
    decoder := json.NewDecoder(bytes.NewReader(raw))
    decoder.DisallowUnknownFields()
    var usage event.UsageEvent
    if err := decoder.Decode(&usage); err != nil {
        writeDecodeError(w, err)
        return
    }
    var trailing any
    if err := decoder.Decode(&trailing); err != io.EOF {
        if err == nil {
            writeError(w, http.StatusBadRequest, "multiple_json_values")
        } else {
            writeDecodeError(w, err)
        }
        return
    }
    if err := rejectAmbiguousMembers(raw); err != nil {
        writeDecodeError(w, err)
        return
    }
    if usage.TenantID != principal.tenantID || usage.ProjectID != principal.projectID {
        writeError(w, http.StatusForbidden, "scope_mismatch")
        return
    }
    if err := usage.Validate(); err != nil {
        writeError(w, http.StatusBadRequest, "invalid_usage_event")
        return
    }
    outcome, err := h.recorder.Record(r.Context(), usage)
    if errors.Is(err, ledger.ErrConflict) {
        writeError(w, http.StatusConflict, "event_identity_conflict")
        return
    }
    if err != nil {
        // Ambiguous PostgreSQL COMMIT failures are intentionally NOT reported
        // as success. Clients retry the *unchanged* event_id + payload_hash.
        w.Header().Set("Retry-After", "1")
        writeError(w, http.StatusServiceUnavailable, "ledger_unavailable_retry_same_event")
        return
    }
    switch outcome {
    case ledger.Created:
        writeJSON(w, http.StatusCreated, map[string]string{"event_id": usage.EventID, "result": "created"})
    case ledger.Duplicate:
        writeJSON(w, http.StatusOK, map[string]string{"event_id": usage.EventID, "result": "duplicate"})
    default:
        w.Header().Set("Retry-After", "1")
        writeError(w, http.StatusServiceUnavailable, "ledger_outcome_unknown")
    }
}


// rejectAmbiguousMembers requires every canonical v1 member exactly once.
// In particular, encoding/json otherwise accepts null or absent scalar fields
// as zero values and case-folded aliases. Typed decoding and the existing
// UsageEvent digest remain independent checks.
func rejectAmbiguousMembers(raw []byte) error {
    d := json.NewDecoder(bytes.NewReader(raw))
    start, err := d.Token()
    if err != nil || start != json.Delim('{') {
        return errors.New("usage event must be a JSON object")
    }
    remaining := map[string]bool{
        "event_id": true, "event_type": true, "schema_version": true, "tenant_id": true,
        "project_id": true, "request_id": true, "model": true, "prompt_tokens": true,
        "completion_tokens": true, "latency_ms": true, "status": true, "timestamp": true, "payload_hash": true,
    }
    for d.More() {
        token, err := d.Token()
        if err != nil {
            return err
        }
        key, ok := token.(string)
        if !ok || !remaining[key] {
            return errors.New("unknown, noncanonical or duplicate JSON member")
        }
        delete(remaining, key)
        var value json.RawMessage
        if err := d.Decode(&value); err != nil {
            return err
        }
        if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
            return errors.New("null usage event member")
        }
    }
    end, err := d.Token()
    if err != nil || end != json.Delim('}') {
        return errors.New("incomplete JSON object")
    }
    if len(remaining) != 0 {
        return errors.New("missing required usage event member")
    }
    return nil
}

func writeDecodeError(w http.ResponseWriter, err error) {
    var maxErr *http.MaxBytesError
    if errors.As(err, &maxErr) {
        writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large")
        return
    }
    writeError(w, http.StatusBadRequest, "invalid_json")
}

func writeError(w http.ResponseWriter, status int, code string) {
    writeJSON(w, status, map[string]string{"error": code})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(status)
    // The response contains only fixed strings and serializable identifiers.
    _ = json.NewEncoder(w).Encode(payload)
}
