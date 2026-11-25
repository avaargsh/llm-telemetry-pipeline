package ingestapi

import (
	"context"
	"encoding/json"
	"errors"
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
	modelErr error
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


func (r *summaryReader) UsageByModel(_ context.Context, tenant, project string, from, to time.Time) ([]ledger.ModelUsage, error) {
	r.tenant, r.project, r.from, r.to = tenant, project, from, to
	r.calls++
	if r.modelErr != nil { return nil, r.modelErr }
	return []ledger.ModelUsage{{Model: "model-a", UsageSummary: ledger.UsageSummary{RequestCount: 1, PromptTokens: "10", CompletionTokens: "3", TotalTokens: "13"}}}, nil
}

func TestUsageQueryModelBreakdown(t *testing.T) {
	r := &summaryReader{}
	h, err := NewUsageQueryHandler(r, []Credential{{Token: testToken, TenantID: "tenant-a", ProjectID: "project-a"}})
	if err != nil { t.Fatal(err) }
	path := "/v1/usage-summary/models?from=2026-11-21T00:00:00Z&to=2026-11-21T01:00:00Z"
	for _, tc := range []struct{ path, token string; status int }{
		{path, "", 401},
		{path+"&tenant_id=other", testToken, 400},
		{path+"&project_id=other", testToken, 400},
		{path, testToken, 200},
	} {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		if tc.token != "" { req.Header.Set("Authorization", "Bearer "+tc.token) }
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != tc.status { t.Fatalf("status=%d want=%d body=%s", rr.Code, tc.status, rr.Body.String()) }
		if tc.status == 200 {
			var got []ledger.ModelUsage
			if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil || len(got) != 1 || got[0].Model != "model-a" || got[0].TotalTokens != "13" {
				t.Fatalf("body=%s err=%v", rr.Body.String(), err)
			}
		}
	}
	if r.calls != 1 || r.tenant != "tenant-a" || r.project != "project-a" { t.Fatalf("reader=%+v", r) }
}

func TestUsageQueryModelErrorsAreExplicitAndRedacted(t *testing.T) {
	for _, tc := range []struct {
		err error
		status int
		code string
	}{
		{ledger.ErrUsageModelLimit, http.StatusUnprocessableEntity, "usage_model_limit_exceeded"},
		{errors.New("private database detail"), http.StatusServiceUnavailable, "ledger_query_unavailable"},
	} {
		r := &summaryReader{modelErr: tc.err}
		h, err := NewUsageQueryHandler(r, []Credential{{Token: testToken, TenantID: "tenant-a", ProjectID: "project-a"}})
		if err != nil { t.Fatal(err) }
		req := httptest.NewRequest(http.MethodGet, "/v1/usage-summary/models?from=2026-11-21T00:00:00Z&to=2026-11-21T01:00:00Z", nil)
		req.Header.Set("Authorization", "Bearer "+testToken)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		var body map[string]string
		if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil { t.Fatal(err) }
		if rr.Code != tc.status || body["error"] != tc.code || len(body) != 1 || r.calls != 1 {
			t.Fatalf("status=%d body=%s calls=%d", rr.Code, rr.Body.String(), r.calls)
		}
	}
}

func (r *summaryReader) UsageSeries(_ context.Context, tenant, project string, from, to time.Time, granularity string) ([]ledger.UsageBucket, error) {
	r.tenant, r.project, r.from, r.to = tenant, project, from, to
	r.calls++
	if granularity != "hour" { return nil, errors.New("unexpected granularity") }
	return []ledger.UsageBucket{{BucketStart: from, UsageSummary: ledger.UsageSummary{RequestCount: 1, PromptTokens: "10", CompletionTokens: "3", TotalTokens: "13"}}}, nil
}

func TestUsageQueryTimeSeries(t *testing.T) {
	r := &summaryReader{}
	h, err := NewUsageQueryHandler(r, []Credential{{Token: testToken, TenantID: "tenant-a", ProjectID: "project-a"}})
	if err != nil { t.Fatal(err) }
	path := "/v1/usage-summary/series?from=2026-11-21T00:00:00Z&to=2026-11-21T01:00:00Z"
	for _, tc := range []struct{ path, token string; status int }{
		{path+"&granularity=hour", "", 401},
		{path, testToken, 400},
		{path+"&granularity=minute", testToken, 400},
		{path+"&granularity=day", testToken, 400},
		{path+"&granularity=hour&granularity=day", testToken, 400},
		{path+"&granularity=hour&tenant_id=other", testToken, 400},
		{path+"&granularity=hour", testToken, 200},
	} {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		if tc.token != "" { req.Header.Set("Authorization", "Bearer "+tc.token) }
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != tc.status { t.Fatalf("status=%d want=%d body=%s", rr.Code, tc.status, rr.Body.String()) }
		if tc.status == 200 {
			var got []ledger.UsageBucket
			if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil || len(got) != 1 || !got[0].BucketStart.Equal(r.from) || got[0].TotalTokens != "13" {
				t.Fatalf("body=%s err=%v", rr.Body.String(), err)
			}
		}
	}
	if r.calls != 1 || r.tenant != "tenant-a" || r.project != "project-a" { t.Fatalf("reader=%+v", r) }
}
