package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const receiverTestToken = "receiver-test-token-012345678901234567890123456789"

func writeCredentials(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "credentials.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadCredentialsUsesStrictTenantBoundJSON(t *testing.T) {
	path := writeCredentials(t, "[{\"token\":\""+receiverTestToken+"\",\"tenant_id\":\"tenant-a\"}]")
	credentials, err := loadCredentials(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(credentials) != 1 || credentials[0].Token != receiverTestToken || credentials[0].TenantID != "tenant-a" {
		t.Fatalf("unexpected credentials projection: %+v", credentials)
	}
}

func TestLoadCredentialsRejectsMalformedUnknownAndTrailingJSON(t *testing.T) {
	for name, body := range map[string]string{
		"empty": "[]",
		"malformed": "[{\"token\":",
		"duplicate object field": "[{\"token\":\""+receiverTestToken+"\",\"token\":\""+receiverTestToken+"\",\"tenant_id\":\"tenant-a\"}]",
		"unknown field": "[{\"token\":\""+receiverTestToken+"\",\"tenant_id\":\"tenant-a\",\"billing_role\":\"admin\"}]",
		"trailing value": "[{\"token\":\""+receiverTestToken+"\",\"tenant_id\":\"tenant-a\"}] {}",
		"invalid tenant": "[{\"token\":\""+receiverTestToken+"\",\"tenant_id\":\"tenant with spaces\"}]",
		"short token": "[{\"token\":\"short\",\"tenant_id\":\"tenant-a\"}]",
	} {
		t.Run(name, func(t *testing.T) {
			if credentials, err := loadCredentials(writeCredentials(t, body)); err == nil || credentials != nil {
				t.Fatalf("invalid credential file accepted: credentials=%+v err=%v", credentials, err)
			}
		})
	}
}

func TestValidateTLSConfigRequiresTLSForRemoteListeners(t *testing.T) {
	for _, addr := range []string{":4318", "0.0.0.0:4318", "[::]:4318", "192.0.2.10:4318"} {
		if err := validateTLSConfig(addr, "", ""); err == nil {
			t.Fatalf("remote listener %q accepted without TLS", addr)
		}
	}
	if err := validateTLSConfig("127.0.0.1:4318", "", ""); err != nil {
		t.Fatalf("loopback test listener should allow plain HTTP: %v", err)
	}
	if err := validateTLSConfig("localhost:4318", "", ""); err != nil {
		t.Fatalf("localhost test listener should allow plain HTTP: %v", err)
	}
	if err := validateTLSConfig(":4318", "/tls/tls.crt", ""); err == nil {
		t.Fatal("incomplete TLS key pair accepted")
	}
	if err := validateTLSConfig(":4318", "/tls/tls.crt", "/tls/tls.key"); err != nil {
		t.Fatalf("complete TLS key pair rejected: %v", err)
	}
}

func TestReceiverMetricsAreBoundedAndClassifyAdmissionResults(t *testing.T) {
	metrics := &receiverMetrics{}
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable, http.StatusBadRequest} {
		handler := metrics.instrument(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
		}))
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/traces", nil))
		if recorder.Code != status {
			t.Fatalf("status=%d want=%d", recorder.Code, status)
		}
	}
	recorder := httptest.NewRecorder()
	metrics.metricsHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, want := range []string{
		"llm_telemetry_genai_receiver_admission_requests_total 3",
		"llm_telemetry_genai_receiver_admission_accepted_total 1",
		"llm_telemetry_genai_receiver_admission_rejected_total 1",
		"llm_telemetry_genai_receiver_admission_failed_total 1",
		"llm_telemetry_genai_receiver_admission_duration_seconds_count 3",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
	for _, forbidden := range []string{"tenant_id=", "trace_id=", "request_id=", "model=", receiverTestToken} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("metrics leaked high-cardinality or secret value %q", forbidden)
		}
	}
}

func TestCredentialFilePathMustBeConfigured(t *testing.T) {
	if credentials, err := loadCredentials(""); err == nil || credentials != nil {
		t.Fatalf("empty path accepted: %+v %v", credentials, err)
	}
}

func TestLoadCredentialsAcceptsTenantIdentifiersSupportedByObservationContract(t *testing.T) {
	path := writeCredentials(t, "[{\"token\":\""+receiverTestToken+"\",\"tenant_id\":\"_tenant-a\"}]")
	credentials, err := loadCredentials(path)
	if err != nil || len(credentials) != 1 || credentials[0].TenantID != "_tenant-a" {
		t.Fatalf("existing observation identifier rejected: credentials=%v err=%v", credentials, err)
	}
}

func TestLoadCredentialsAcceptsGroupReadableProjectedSecret(t *testing.T) {
	path := writeCredentials(t, "[{\"token\":\""+receiverTestToken+"\",\"tenant_id\":\"tenant-a\"}]")
	if err := os.Chmod(path, 0o440); err != nil {
		t.Fatal(err)
	}
	if credentials, err := loadCredentials(path); err != nil || len(credentials) != 1 {
		t.Fatalf("group-readable projected Secret rejected: credentials=%v err=%v", credentials, err)
	}
}

func TestLoadCredentialsRejectsWorldReadableSecret(t *testing.T) {
	path := writeCredentials(t, "[{\"token\":\""+receiverTestToken+"\",\"tenant_id\":\"tenant-a\"}]")
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if credentials, err := loadCredentials(path); err == nil || credentials != nil {
		t.Fatalf("world-readable credential file accepted: credentials=%v err=%v", credentials, err)
	}
}

func TestLoadCredentialsRejectsNoncanonicalJSONKeys(t *testing.T) {
	for name, body := range map[string]string{
		"token case collision": `[{"token":"` + receiverTestToken + `","TOKEN":"` + receiverTestToken + `","tenant_id":"tenant-a"}]`,
		"tenant case collision": `[{"token":"` + receiverTestToken + `","tenant_id":"tenant-a","TENANT_ID":"tenant-b"}]`,
		"single uppercase alias": `[{"TOKEN":"` + receiverTestToken + `","tenant_id":"tenant-a"}]`,
	} {
		t.Run(name, func(t *testing.T) {
			credentials, err := loadCredentials(writeCredentials(t, body))
			if err == nil || len(credentials) != 0 {
				t.Fatal("noncanonical credential JSON key accepted")
			}
		})
	}
}
