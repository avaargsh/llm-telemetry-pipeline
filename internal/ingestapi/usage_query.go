package ingestapi

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
)

type UsageReader interface {
	UsageSummary(context.Context, string, string, time.Time, time.Time) (ledger.UsageSummary, error)
	UsageByModel(context.Context, string, string, time.Time, time.Time) ([]ledger.ModelUsage, error)
}

type usageQueryHandler struct {
	reader UsageReader
	principals []principal
}

// NewUsageQueryHandler exposes only read operations. Supply a separate set of
// query credentials; ingestion credentials are not implicitly accepted.
func NewUsageQueryHandler(reader UsageReader, credentials []Credential) (http.Handler, error) {
	if reader == nil { return nil, errors.New("usage reader is required") }
	principals, err := newPrincipals(credentials)
	if err != nil { return nil, err }
	return &usageQueryHandler{reader: reader, principals: principals}, nil
}

func (h *usageQueryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.URL.Path != "/v1/usage-summary" && r.URL.Path != "/v1/usage-summary/models" {
		writeError(w, http.StatusNotFound, "not_found")
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	p := authenticate(h.principals, r.Header)
	if p == nil {
		w.Header().Set("WWW-Authenticate", "Bearer realm=\"usage-query\"")
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	// No tenant/project override and no ambiguous duplicate parameters.
	if len(r.URL.RawQuery) > 512 {
		writeError(w, http.StatusBadRequest, "invalid_usage_window")
		return
	}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || len(q) != 2 || len(q["from"]) != 1 || len(q["to"]) != 1 {
		writeError(w, http.StatusBadRequest, "invalid_usage_window")
		return
	}
	from, fromErr := time.Parse(time.RFC3339, q.Get("from"))
	to, toErr := time.Parse(time.RFC3339, q.Get("to"))
	if fromErr != nil || toErr != nil || from.IsZero() || to.IsZero() || !to.After(from) || to.Sub(from) > ledger.MaxUsageWindow {
		writeError(w, http.StatusBadRequest, "invalid_usage_window")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if r.URL.Path == "/v1/usage-summary/models" {
		models, err := h.reader.UsageByModel(ctx, p.tenantID, p.projectID, from.UTC(), to.UTC())
		if errors.Is(err, ledger.ErrUsageModelLimit) {
			writeError(w, http.StatusUnprocessableEntity, "usage_model_limit_exceeded")
			return
		}
		if err != nil {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusServiceUnavailable, "ledger_query_unavailable")
			return
		}
		writeJSON(w, http.StatusOK, models)
		return
	}
	result, err := h.reader.UsageSummary(ctx, p.tenantID, p.projectID, from.UTC(), to.UTC())
	if err != nil {
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "ledger_query_unavailable")
		return
	}
	writeJSON(w, http.StatusOK, result)
}
