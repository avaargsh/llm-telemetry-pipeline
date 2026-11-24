package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWorkerMetricsAreBoundedAndDoNotExposeTenantLabels(t *testing.T) {
	m := &workerMetrics{}
	m.acknowledged.Store(3)
	m.retryable.Store(2)
	m.terminal.Store(1)
	m.errors.Store(4)
	m.idle.Store(5)
	m.queueLength.Store(7)
	m.queueReady.Store(true)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()
	m.handler(nil).ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d", recorder.Code)
	}
	body := recorder.Body.String()
	for _, want := range []string{
		"llm_telemetry_genai_worker_acknowledged_total 3",
		"llm_telemetry_genai_worker_retryable_total 2",
		"llm_telemetry_genai_worker_terminal_total 1",
		"llm_telemetry_genai_worker_errors_total 4",
		"llm_telemetry_genai_worker_idle_polls_total 5",
		"llm_telemetry_genai_worker_queue_length 7",
		"llm_telemetry_genai_worker_queue_length_available 1",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
	for _, forbidden := range []string{"tenant_id=", "trace_id=", "request_id=", "model="} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("metrics contain unbounded identity label %q", forbidden)
		}
	}
}

func TestWorkerHealthEndpoint(t *testing.T) {
	m := &workerMetrics{}
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recorder := httptest.NewRecorder()
	m.handler(nil).ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK || recorder.Body.String() != "ok\n" {
		t.Fatalf("health response status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}
