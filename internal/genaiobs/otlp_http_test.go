package genaiobs

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const receiverToken = "test-only-012345678901234567890123456789"

func receiverRequest(body io.Reader) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/v1/traces", body)
	r.Header.Set("Authorization", "Bearer "+receiverToken)
	r.Header.Set("Content-Type", "application/json")
	return r
}

func TestOTLPHTTPAdmission(t *testing.T) {
	q, _ := NewQueue(1)
	h, err := NewOTLPHTTPHandler(q, []OTLPCredential{{Token: receiverToken, TenantID: "trusted"}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, body string
		change     func(*http.Request)
		status     int
	}{
		{"unauthenticated", wireSpan, func(r *http.Request) { r.Header.Del("Authorization") }, 401},
		{"duplicate auth", wireSpan, func(r *http.Request) { r.Header.Add("Authorization", "Bearer "+receiverToken) }, 401},
		{"media", wireSpan, func(r *http.Request) { r.Header.Set("Content-Type", "application/x-protobuf") }, 415},
		{"encoding", wireSpan, func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }, 415},
		{"method", wireSpan, func(r *http.Request) { r.Method = "GET" }, 405},
		{"path", wireSpan, func(r *http.Request) { r.URL.Path = "/v1/usage-events" }, 404},
		{"malformed", "{", nil, 400},
		{"oversized", strings.Repeat("x", MaxOTLPJSONBytes+1), func(r *http.Request) { r.ContentLength = -1 }, 413},
		{"permanent batch limit", string(wireBody(wireSpan + "," + wireSpan)), nil, 413},
		{"accepted", string(wireBody(wireSpan)), nil, 200},
		{"saturated", string(wireBody(wireSpan)), nil, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := receiverRequest(strings.NewReader(tc.body))
			if tc.change != nil {
				tc.change(r)
			}
			w := httptest.NewRecorder()
			before := q.Len()
			h.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status %d want %d", w.Code, tc.status)
			}
			if w.Header().Get("Content-Type") != "application/json" {
				t.Fatal("missing JSON content type")
			}
			if tc.status == 200 {
				if strings.TrimSpace(w.Body.String()) != "{}" || q.Len() != before+1 {
					t.Fatal("invalid success")
				}
			} else if q.Len() != before {
				t.Fatal("rejection mutated queue")
			}
			if tc.status == 503 && w.Header().Get("Retry-After") != "1" {
				t.Fatal("missing backoff")
			}
			if strings.Contains(w.Body.String(), receiverToken) || strings.Contains(w.Body.String(), "secret") {
				t.Fatal("leaked request data")
			}
		})
	}
	o, ok := q.Pop()
	if !ok || o.TenantID != "trusted" || o.InputTokens == nil || *o.InputTokens != 9007199254740993 {
		t.Fatalf("bad observation: %+v", o)
	}
}

type gatedBody struct{ entered, release chan struct{} }

func (b *gatedBody) Read([]byte) (int, error) { close(b.entered); <-b.release; return 0, io.EOF }
func (b *gatedBody) Close() error             { return nil }

func TestOTLPHTTPConcurrency(t *testing.T) {
	q, _ := NewQueue(2)
	h, err := NewOTLPHTTPHandler(q, []OTLPCredential{{Token: receiverToken, TenantID: "trusted"}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	b := &gatedBody{make(chan struct{}), make(chan struct{})}
	done := make(chan struct{})
	defer func() { close(b.release); <-done }()
	go func() { defer close(done); h.ServeHTTP(httptest.NewRecorder(), receiverRequest(b)) }()
	select {
	case <-b.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("body read not entered")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, receiverRequest(strings.NewReader(string(wireBody(wireSpan)))))
	if w.Code != 503 || q.Len() != 0 {
		t.Fatal("concurrency limit bypassed")
	}
}

func TestOTLPHTTPConfig(t *testing.T) {
	q, _ := NewQueue(1)
	good := OTLPCredential{Token: receiverToken, TenantID: "trusted"}
	for _, tc := range []struct {
		q     *Queue
		creds []OTLPCredential
		n     int
	}{
		{nil, []OTLPCredential{good}, 1}, {q, nil, 1}, {q, []OTLPCredential{good}, 0}, {q, []OTLPCredential{good}, 65}, {q, []OTLPCredential{good, good}, 1}, {q, []OTLPCredential{{Token: "short", TenantID: "trusted"}}, 1}, {q, []OTLPCredential{{Token: receiverToken, TenantID: "bad scope"}}, 1},
	} {
		if h, err := NewOTLPHTTPHandler(tc.q, tc.creds, tc.n); err == nil || h != nil {
			t.Fatal("invalid configuration accepted")
		}
	}
}
