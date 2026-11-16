package clickhouse

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "testing"
    "time"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
)

func runClickHouseSQL(t *testing.T, statement string) string {
    t.Helper()
    endpoint:=os.Getenv("TEST_CLICKHOUSE_HTTP")
    if endpoint=="" {t.Skip("TEST_CLICKHOUSE_HTTP not set")}
    ctx,cancel:=context.WithTimeout(context.Background(),15*time.Second)
    defer cancel()
    req,err:=http.NewRequestWithContext(ctx,http.MethodPost,endpoint+"?database=telemetry",strings.NewReader(statement))
    if err!=nil{t.Fatal(err)}
    req.SetBasicAuth("telemetry","localdev-only")
    resp,err:=http.DefaultClient.Do(req)
    if err!=nil{t.Fatal(err)}
    defer resp.Body.Close()
    body,err:=io.ReadAll(io.LimitReader(resp.Body,512*1024))
    if err!=nil{t.Fatal(err)}
    if resp.StatusCode!=200{t.Fatalf("ClickHouse status=%d SQL=%s: %s",resp.StatusCode,statement,string(body))}
    return strings.TrimSpace(string(body))
}

func TestAnalyticsReplayDeduplicatesAndConflictingHashIsQuarantined(t *testing.T){
    endpoint:=os.Getenv("TEST_CLICKHOUSE_HTTP")
    if endpoint==""{t.Skip("TEST_CLICKHOUSE_HTTP not set")}
    seed:=fmt.Sprintf("analytics-%d",time.Now().UnixNano())
    e,err:=event.SyntheticUsageEvent(seed,0,time.Date(2026,11,16,11,0,0,0,time.UTC))
    if err!=nil{t.Fatal(err)}
    w:=&HTTPWriter{Endpoint:endpoint,Database:"telemetry",User:"telemetry",Password:"localdev-only"}
    for i:=0;i<2;i++{if err:=w.Insert(context.Background(),[]event.UsageEvent{e});err!=nil{t.Fatal(err)}}
    selector:=fmt.Sprintf("event_id=toUUID('%s')",e.EventID)
    raw:=runClickHouseSQL(t,"SELECT count() FROM inference_events_raw WHERE "+selector+" FORMAT TabSeparatedRaw")
    if raw!="2"{t.Fatalf("expected two replayed raw rows, got %q",raw)}
    dedup:=runClickHouseSQL(t,"SELECT count(), sum(prompt_tokens+completion_tokens), sum(raw_copies) FROM inference_events_dedup_v1 WHERE "+selector+" FORMAT TabSeparatedRaw")
    wanted:=fmt.Sprintf("1\t%d\t2",e.PromptTokens+e.CompletionTokens)
    if dedup!=wanted{t.Fatalf("duplicate changed analytical totals; got %q expected %q",dedup,wanted)}
    errors:=runClickHouseSQL(t,"SELECT count() FROM inference_events_integrity_v1 WHERE "+selector+" FORMAT TabSeparatedRaw")
    if errors!="0"{t.Fatalf("clean duplicates must not be integrity errors: %q",errors)}

    // Simulate a corrupted raw row that bypassed the Go event seal. A query
    // must quarantine its Event ID, not arbitrarily select one token value.
    malicious:=`INSERT INTO inference_events_raw
      (event_id,event_time,tenant_id,project_id,request_id,model,prompt_tokens,completion_tokens,latency_ms,status,payload_hash)
      SELECT event_id,event_time,tenant_id,project_id,request_id,model,
             prompt_tokens+1,completion_tokens,latency_ms,status,
             toFixedString(repeat('f',64),64)
      FROM inference_events_raw WHERE `+selector+` LIMIT 1`
    runClickHouseSQL(t,malicious)
    dedupCount:=runClickHouseSQL(t,"SELECT count() FROM inference_events_dedup_v1 WHERE "+selector+" FORMAT TabSeparatedRaw")
    if dedupCount!="0"{t.Fatalf("conflicting hash was silently accepted: %q",dedupCount)}
    details:=runClickHouseSQL(t,"SELECT raw_copies,hash_versions FROM inference_events_integrity_v1 WHERE "+selector+" FORMAT TabSeparatedRaw")
    if details!="3\t2"{t.Fatalf("expected one visible integrity conflict, got %q",details)}
}

func TestAnalyticsRejectsLegacyRowMissingPayloadHash(t *testing.T){
    if os.Getenv("TEST_CLICKHOUSE_HTTP")==""{t.Skip("TEST_CLICKHOUSE_HTTP not set")}
    e,err:=event.SyntheticUsageEvent("legacy-"+strconv.FormatInt(time.Now().UnixNano(),10),1,
        time.Date(2026,11,16,11,0,0,0,time.UTC))
    if err!=nil{t.Fatal(err)}
    // Legacy writer omitted payload_hash; ClickHouse FixedString defaults to
    // an all-zero padded value. This row must not appear in dedup analytics.
    statement:=fmt.Sprintf(`INSERT INTO inference_events_raw
    (event_id,event_time,tenant_id,project_id,request_id,model,prompt_tokens,completion_tokens,latency_ms,status)
    VALUES (toUUID('%s'),toDateTime64('2026-11-16 11:00:00',3,'UTC'),
    'tenant_legacy','project_legacy','legacy_req','Qwen3-32B',123,45,100,'success')`,e.EventID)
    runClickHouseSQL(t,statement)
    selector:=fmt.Sprintf("event_id=toUUID('%s')",e.EventID)
    if got:=runClickHouseSQL(t,"SELECT count() FROM inference_events_dedup_v1 WHERE "+selector+" FORMAT TabSeparatedRaw");got!="0"{
       t.Fatalf("unsealed legacy row included in trusted projection: %s",got)
    }
    if got:=runClickHouseSQL(t,"SELECT missing_hash_rows FROM inference_events_integrity_v1 WHERE "+selector+" FORMAT TabSeparatedRaw");got!="1"{
       t.Fatalf("legacy row not surfaced as missing hash: %s",got)
    }
}
