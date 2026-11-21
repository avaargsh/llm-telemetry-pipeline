package clickhouse

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "path/filepath"
    "strconv"
    "strings"
    "testing"
    "time"
)

type asyncACKEvidence struct {
    SchemaVersion int `json:"schema_version"`
    HeadSHA string `json:"head_sha"`
    TestName string `json:"test_name"`
    TableEngine string `json:"table_engine"`
    WaitForFlushStatus int `json:"wait_for_flush_http_status"`
    WaitForFlushACKMS int64 `json:"wait_for_flush_ack_ms"`
    WaitForFlushVisibleAtACK bool `json:"wait_for_flush_visible_at_ack"`
    FireAndForgetStatus int `json:"fire_and_forget_http_status"`
    FireAndForgetACKMS int64 `json:"fire_and_forget_ack_ms"`
    FireAndForgetVisibleAtACK bool `json:"fire_and_forget_visible_at_ack"`
    FireAndForgetEventuallyVisible bool `json:"fire_and_forget_eventually_visible"`
    FireAndForgetObservability string `json:"fire_and_forget_observability"`
    Verdict string `json:"verdict"`
}

// Async buffered acknowledgement is not a Kafka offset commit criterion.
// This test uses a disposable real MergeTree table rather than mocking ACKs.
// A missing row immediately after wait=0 is permitted, NOT required;
// real flush scheduling can vary across ClickHouse versions and CI hosts.
func TestRealClickHouseAsyncACKBoundary(t *testing.T) {
    endpoint := strings.TrimRight(os.Getenv("TEST_CLICKHOUSE_HTTP"), "/")
    if endpoint == "" {
        t.Skip("TEST_CLICKHOUSE_HTTP not configured")
    }
    table := "m5_async_ack_" + strconv.FormatInt(time.Now().UnixNano(), 10)
    ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
    defer cancel()
    client := &http.Client{Timeout: 25 * time.Second}

    // This helper records the real HTTP status and never retries INSERTs.
    // Retries belong at the application protocol layer and must use stable IDs.
    do := func(statement string, settings url.Values) (int, string, time.Duration, error) {
        options := url.Values{"database": {"telemetry"}}
        for key, values := range settings {
            for _, value := range values { options.Add(key, value) }
        }
        request, err := http.NewRequestWithContext(
            ctx, http.MethodPost, endpoint+"/?"+options.Encode(), strings.NewReader(statement))
        if err != nil { return 0, "", 0, err }
        request.SetBasicAuth("telemetry", "localdev-only")
        request.Header.Set("Content-Type", "text/plain; charset=utf-8")
        began := time.Now()
        response, err := client.Do(request)
        elapsed := time.Since(began)
        if err != nil { return 0, "", elapsed, err }
        defer response.Body.Close()
        body, readErr := io.ReadAll(io.LimitReader(response.Body, 8192))
        return response.StatusCode, strings.TrimSpace(string(body)), elapsed, readErr
    }
    exec := func(statement string, settings url.Values) time.Duration {
        t.Helper()
        status, body, elapsed, err := do(statement, settings)
        if err != nil || status != http.StatusOK {
            t.Fatalf("ClickHouse status=%d err=%v body=%q", status, err, body)
        }
        return elapsed
    }
    readCount := func(id string) int {
        t.Helper()
        status, body, _, err := do(fmt.Sprintf(
            "SELECT count() FROM telemetry.%s WHERE event_id='%s' FORMAT TabSeparatedRaw", table, id), nil)
        if err != nil || status != http.StatusOK {
            t.Fatalf("ClickHouse read status=%d err=%v body=%q", status, err, body)
        }
        count, err := strconv.Atoi(body)
        if err != nil { t.Fatalf("invalid ClickHouse count %q: %v", body, err) }
        return count
    }
    exec(fmt.Sprintf(
        "CREATE TABLE telemetry.%s (event_id String, tokens UInt64) ENGINE=MergeTree ORDER BY event_id", table), nil)
    t.Cleanup(func() {
        cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
        defer stop()
        req, err := http.NewRequestWithContext(cleanupCtx, http.MethodPost,
            endpoint+"/?database=telemetry",
            strings.NewReader("DROP TABLE IF EXISTS telemetry."+table))
        if err != nil { t.Error(err); return }
        req.SetBasicAuth("telemetry", "localdev-only")
        resp, err := client.Do(req)
        if err != nil { t.Errorf("drop async test table: %v", err); return }
        defer resp.Body.Close()
        if resp.StatusCode != http.StatusOK {
            body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
            t.Errorf("drop async test table failed %d: %s", resp.StatusCode, body)
        }
    })

    // Disable adaptive flush only inside this disposable experiment to make
    // wait=0 buffer acceptance observably different in many test runs. Do not
    // assume that wait=0 is invisible immediately: a flush may occur sooner.
    options := url.Values{
        "async_insert": {"1"},
        "async_insert_use_adaptive_busy_timeout": {"0"},
        "async_insert_busy_timeout_ms": {"3000"},
        "async_insert_max_data_size": {"100000000"},
        "async_insert_max_query_number": {"10000"},
    }
    strict := url.Values{}
    for k,v := range options { strict[k] = append([]string(nil), v...) }
    strict.Set("wait_for_async_insert", "1")
    waited := exec(fmt.Sprintf(
        "INSERT INTO telemetry.%s FORMAT JSONEachRow
{"event_id":"waited","tokens":17}
", table), strict)
    if got := readCount("waited"); got != 1 {
        t.Fatalf("wait_for_async_insert=1 ACK did not imply query-visible row; count=%d", got)
    }

    fire := url.Values{}
    for k,v := range options { fire[k] = append([]string(nil), v...) }
    fire.Set("wait_for_async_insert", "0")
    fastACK := exec(fmt.Sprintf(
        "INSERT INTO telemetry.%s FORMAT JSONEachRow
{"event_id":"fire_and_forget","tokens":19}
", table), fire)
    atACK := readCount("fire_and_forget")
    if atACK > 1 { t.Fatalf("unexpected duplicate data before retry: %d", atACK) }

    // A wait=0 HTTP 200 is only a buffer ACK. Poll until the row is visible
    // (or fail), without using an unverified ACK to commit a Kafka offset.
    visible := atACK == 1
    deadline := time.Now().Add(25 * time.Second)
    for !visible && time.Now().Before(deadline) {
        time.Sleep(250*time.Millisecond)
        visible = readCount("fire_and_forget") == 1
    }
    if !visible { t.Fatal("async wait=0 request was ACKed but never became visible within 25s") }

    evidence := asyncACKEvidence{
        SchemaVersion: 1,
        HeadSHA: os.Getenv("GITHUB_SHA"),
        TestName: "real_clickhouse_async_ack_boundary",
        TableEngine: "MergeTree",
        WaitForFlushStatus: 200,
        WaitForFlushACKMS: waited.Milliseconds(),
        WaitForFlushVisibleAtACK: true,
        FireAndForgetStatus: 200,
        FireAndForgetACKMS: fastACK.Milliseconds(),
        FireAndForgetVisibleAtACK: atACK == 1,
        FireAndForgetEventuallyVisible: true,
        FireAndForgetObservability: "wait=0 is a buffered ACK, not confirmation of storage; visibility timing is observational only",
        Verdict: "PASS",
    }
    if path := os.Getenv("M5_ASYNC_EVIDENCE_PATH"); path != "" {
        data, err := json.MarshalIndent(evidence, "", "  ")
        if err != nil { t.Fatal(err) }
        if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil { t.Fatal(err) }
        if err := os.WriteFile(path, append(data, '\n'), 0644); err != nil { t.Fatal(err) }
    }
    t.Logf("PASS async ACK boundary: wait=1 visible at ACK; wait=0 visible at ACK=%t, eventually visible=true; no wait=0 Kafka ACK assumption",
        atACK == 1)
}
