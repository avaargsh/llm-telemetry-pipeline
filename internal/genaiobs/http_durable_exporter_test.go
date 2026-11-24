package genaiobs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

const durableHTTPTestToken = "m11-6-test-token-012345678901234567890123456789"

func httpExporterObservation() Observation {
	input := int64(0)
	output := int64(9007199254740993)
	return Observation{
		SchemaVersion: Version,
		TenantID: "tenant-a",
		TraceID: "0123456789abcdef0123456789abcdef",
		SpanID: "0123456789abcdef",
		RequestID: "request-42",
		AttemptID: "attempt-7",
		Provider: "openai",
		Operation: "chat",
		RequestModel: "gpt-4.1",
		ResponseModel: "gpt-4.1",
		Status: "ok",
		InputTokens: &input,
		OutputTokens: &output,
	}
}

func TestHTTPDurableExporterSendsOTLPAndStableIdempotencyKey(t *testing.T) {
	want := httpExporterObservation()
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/traces" {
			t.Errorf("unexpected request target: %s %s", r.Method, r.URL.Path)
		}
		if values := r.Header.Values("Idempotency-Key"); len(values) != 1 || values[0] != "genaiobs/77" {
			t.Errorf("unexpected Idempotency-Key values: %#v", values)
		}
		if r.Header.Get("Authorization") != "Bearer "+durableHTTPTestToken {
			t.Errorf("missing authenticated receiver credential")
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected content type: %q", r.Header.Get("Content-Type"))
		}
		var err error
		gotBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	exporter, err := NewHTTPDurableExporter(server.URL+"/v1/traces", durableHTTPTestToken, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err := exporter.Export(context.Background(), Delivery{ID: 77, Observation: want}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(gotBody), want.TenantID) || strings.Contains(string(gotBody), "prompt") || strings.Contains(string(gotBody), "completion") {
		t.Fatalf("exported OTLP payload contains tenant scope or raw content: %s", gotBody)
	}
	spans, err := DecodeOTLPJSON(gotBody)
	if err != nil || len(spans) != 1 {
		t.Fatalf("decode exported OTLP payload: spans=%d err=%v", len(spans), err)
	}
	got, err := NormalizeInference(Scope{TenantID: want.TenantID}, Version, spans[0])
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("exported projection changed normalized fields:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestHTTPDurableExporterClassifiesHTTPResponses(t *testing.T) {
	for _, tc := range []struct {
		name string
		status int
		permanent bool
		wantErr bool
	}{
		{name: "accepted", status: http.StatusAccepted},
		{name: "no content", status: http.StatusNoContent},
		{name: "request timeout", status: http.StatusRequestTimeout, wantErr: true},
		{name: "rate limited", status: http.StatusTooManyRequests, wantErr: true},
		{name: "server error", status: http.StatusServiceUnavailable, wantErr: true},
		{name: "bad request", status: http.StatusBadRequest, permanent: true, wantErr: true},
		{name: "unauthorized", status: http.StatusUnauthorized, permanent: true, wantErr: true},
		{name: "conflict", status: http.StatusConflict, permanent: true, wantErr: true},
		{name: "redirect", status: http.StatusTemporaryRedirect, permanent: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.status == http.StatusTemporaryRedirect {
					w.Header().Set("Location", "/redirect-target")
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, "private-response-secret")
			}))
			defer server.Close()
			exporter, err := NewHTTPDurableExporter(server.URL+"/v1/traces", durableHTTPTestToken, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			err = exporter.Export(context.Background(), Delivery{ID: 1, Observation: httpExporterObservation()})
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v wantErr=%v", err, tc.wantErr)
			}
			var permanent permanentExporterError
			if errors.As(err, &permanent) != tc.permanent {
				t.Fatalf("permanent classification=%v want=%v err=%v", errors.As(err, &permanent), tc.permanent, err)
			}
			if err != nil && strings.Contains(err.Error(), "private-response-secret") {
				t.Fatal("remote response body leaked into exporter error")
			}
		})
	}
}

func TestHTTPDurableExporterRejectsUnsafeConfiguration(t *testing.T) {
	for _, endpoint := range []string{
		"http://example.com/v1/traces",
		"https://example.com/v1/traces?tenant=unsafe",
		"https://user:password@example.com/v1/traces",
		"/v1/traces",
		"https://example.com/other",
	} {
		if exporter, err := NewHTTPDurableExporter(endpoint, durableHTTPTestToken, nil); err == nil || exporter != nil {
			t.Fatalf("accepted unsafe endpoint %q", endpoint)
		}
	}
	if exporter, err := NewHTTPDurableExporter("https://example.com/v1/traces", "short", nil); err == nil || exporter != nil {
		t.Fatal("accepted short receiver credential")
	}
}

func TestOTLPHTTPIdempotencyKeyRequiresDurableReceiver(t *testing.T) {
	q, _ := NewQueue(4)
	h, err := NewOTLPHTTPHandler(q, []OTLPCredential{{Token: receiverToken, TenantID: "trusted"}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		body string
		keys []string
		want int
	}{
		{name: "volatile backend cannot acknowledge durable key", body: string(wireBody(wireSpan)), keys: []string{"genaiobs/1"}, want: http.StatusNotImplemented},
		{name: "one key cannot identify a batch", body: string(wireBody(wireSpan+","+wireSpan)), keys: []string{"genaiobs/1"}, want: http.StatusBadRequest},
		{name: "duplicate key header", body: string(wireBody(wireSpan)), keys: []string{"genaiobs/1", "genaiobs/1"}, want: http.StatusBadRequest},
		{name: "invalid key", body: string(wireBody(wireSpan)), keys: []string{"bad key"}, want: http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := receiverRequest(strings.NewReader(tc.body))
			for _, key := range tc.keys {
				r.Header.Add("Idempotency-Key", key)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("status=%d want=%d body=%s", w.Code, tc.want, w.Body.String())
			}
			if q.Len() != 0 {
				t.Fatal("rejected idempotency request mutated volatile queue")
			}
		})
	}
}
