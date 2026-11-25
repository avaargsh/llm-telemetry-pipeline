package ingestapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
)

type summaryReader struct {
	tenant, project string
	from, to time.Time
	calls int
}

func (r *summaryReader) UsageSummary(_ context.Context, tenant, project string, from, to time.Time) (ledger.UsageSummary, error) {
	r.tenant, r.project, r.from, r.to = tenant, project, from, to
	r.calls++
	return ledger.UsageSummary{RequestCount: 2, PromptTokens: "20", CompletionTokens: "6", TotalTokens: "26"}, nil
}

func TestUsageQueryCredentialScopeAndWindow(t *testing.T) {
	r := &summaryReader{}
	h, err := NewUsageQueryHandler(r, []Credential{{Token: testToken, TenantID: "tenant-a", ProjectID: "project-a"}})
	if err != nil { t.Fatal(err) }
	path := "/v1/usage-summary?from=2026-11-21T00:00:00Z&to=2026-11-21T01:00:00Z"
	for _, tc := range []struct{ path, token, method string; status int }{
		{path, "", http.MethodGet, 401},
		{path, otherToken, http.MethodGet, 401},
		{path+"&tenant_id=other", testToken, http.MethodGet, 400},
		{path+"&project_id=other", testToken, http.MethodGet, 400},
		{path+"&from=2026-11-21T00:00:00Z", testToken, http.MethodGet, 400},
		{"/v1/usage-summary?from=2026-11-21T00:00:00Z&to=2027-01-01T00:00:00Z", testToken, http.MethodGet, 400},
		{path, testToken, http.MethodPost, 405},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		if tc.token != "" { req.Header.Set("Authorization", "Bearer "+tc.token) }
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != tc.status || r.calls != 0 { t.Fatalf("status=%d calls=%d want=%d", rr.Code, r.calls, tc.status) }
	}
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != 200 || r.calls != 1 || r.tenant != "tenant-a" || r.project != "project-a" || r.to.Sub(r.from) != time.Hour {
		t.Fatalf("status=%d reader=%+v", rr.Code, r)
	}
	var got ledger.UsageSummary
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil || got.TotalTokens != "26" || got.RequestCount != 2 {
		t.Fatalf("body=%s err=%v", rr.Body.String(), err)
	}
	if rr.Header().Get("Cache-Control") != "no-store" { t.Fatal("missing no-store") }
}
