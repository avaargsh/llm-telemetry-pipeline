package clickhouse

import (
    "context"
    "net/http"
    "net/http/httptest"
    "testing"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
)

// This is a fail-closed request-contract regression: a caller must not be
// able to weaken the durable consumer's sink ACK through endpoint query flags.
func TestInsertOverridesUnsafeAsyncEndpointSettings(t *testing.T) {
    called := false
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        called = true
        q := r.URL.Query()
        if got := q.Get("async_insert"); got != "0" {
            t.Errorf("unsafe ClickHouse async_insert=%q, expected 0", got)
        }
        if got := q.Get("wait_for_async_insert"); got != "1" {
            t.Errorf("unsafe ClickHouse wait_for_async_insert=%q, expected 1", got)
        }
        if got := q.Get("wait_end_of_query"); got != "1" {
            t.Errorf("unsafe ClickHouse wait_end_of_query=%q, expected 1", got)
        }
        if got := q.Get("database"); got != "telemetry" {
            t.Errorf("database scope changed: %q", got)
        }
        w.WriteHeader(http.StatusOK)
    }))
    defer server.Close()

    // The URL may originate in deployment configuration, not in vetted code.
    // Even if these unsafe flags are supplied, HTTPWriter must enforce its
    // own synchronous, confirmed-ACK contract on the wire.
    sink := &HTTPWriter{
        Endpoint: server.URL + "?async_insert=1&wait_for_async_insert=0&wait_end_of_query=0",
        Database: "telemetry",
    }
    if err := sink.Insert(context.Background(), []event.UsageEvent{testEvent(t)}); err != nil {
        t.Fatal(err)
    }
    if !called {
        t.Fatal("ClickHouse HTTP sink was not exercised")
    }
}
