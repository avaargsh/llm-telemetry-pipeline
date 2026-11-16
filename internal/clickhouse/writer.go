package clickhouse

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "strings"
    "time"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
)

const insertSQL = "INSERT INTO inference_events_raw (event_id,event_time,tenant_id,project_id,request_id,model,prompt_tokens,completion_tokens,latency_ms,status) FORMAT JSONEachRow\n"

// HTTPWriter writes a complete validated batch synchronously. A timeout is an
// UNKNOWN outcome: callers must replay rather than assume the insert failed.
type HTTPWriter struct {
    Endpoint string
    Database string
    User string
    Password string
    Client *http.Client
}

type eventRow struct {
    EventID string `json:"event_id"`
    EventTime string `json:"event_time"`
    TenantID string `json:"tenant_id"`
    ProjectID string `json:"project_id"`
    RequestID string `json:"request_id"`
    Model string `json:"model"`
    PromptTokens int64 `json:"prompt_tokens"`
    CompletionTokens int64 `json:"completion_tokens"`
    LatencyMS int64 `json:"latency_ms"`
    Status string `json:"status"`
}

func (w *HTTPWriter) Insert(ctx context.Context, events []event.UsageEvent) error {
    if len(events) == 0 { return nil }
    u, err := url.Parse(w.Endpoint)
    if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
        return fmt.Errorf("invalid ClickHouse endpoint: %q", w.Endpoint)
    }
    q := u.Query()
    db := w.Database
    if db == "" { db = "telemetry" }
    q.Set("database", db)
    q.Set("async_insert", "0")
    q.Set("wait_end_of_query", "1")
    u.RawQuery = q.Encode()
    var buf bytes.Buffer
    buf.WriteString(insertSQL)
    encoder := json.NewEncoder(&buf)
    for _, e := range events {
        if err := e.Validate(); err != nil { return fmt.Errorf("event %s: %w", e.EventID, err) }
        row := eventRow{
            EventID:e.EventID, EventTime:e.Timestamp.UTC().Format("2006-01-02 15:04:05.000"),
            TenantID:e.TenantID, ProjectID:e.ProjectID, RequestID:e.RequestID,
            Model:e.Model, PromptTokens:e.PromptTokens, CompletionTokens:e.CompletionTokens,
            LatencyMS:e.LatencyMS, Status:e.Status,
        }
        if err := encoder.Encode(row); err != nil { return err }
    }
    req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), &buf)
    if err != nil { return err }
    req.Header.Set("Content-Type", "text/plain; charset=utf-8")
    if w.User != "" { req.SetBasicAuth(w.User, w.Password) }
    client := w.Client
    if client == nil { client = &http.Client{Timeout: 30*time.Second} }
    resp, err := client.Do(req)
    if err != nil { return fmt.Errorf("ClickHouse insert outcome unknown: %w", err) }
    defer resp.Body.Close()
    body, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
    if readErr != nil { return fmt.Errorf("ClickHouse response incomplete; outcome unknown: %w", readErr) }
    if resp.StatusCode < 200 || resp.StatusCode >= 300 || strings.Contains(string(body), "Code: ") {
        return fmt.Errorf("ClickHouse insert returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
    }
    return nil
}
