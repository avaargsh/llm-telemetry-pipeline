package clickhouse

import (
    "context"
    "encoding/json"
    "io"
    "net/http"
    "net/http/httptest"
    "strings"
    "testing"
    "time"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
)

func testEvent(t *testing.T) event.UsageEvent {
    t.Helper()
    e,err:=event.SyntheticUsageEvent("test",0,time.Date(2026,11,16,11,0,0,0,time.UTC))
    if err!=nil {t.Fatal(err)}
    return e
}

func TestInsertUsesSynchronousJSONEachRow(t *testing.T) {
    var query string
    var rows []map[string]any
    server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
        if r.Method!=http.MethodPost {t.Errorf("method: %s",r.Method)}
        if u,_,ok:=r.BasicAuth();!ok || u!="telemetry" {t.Error("missing auth")}
        query=r.URL.RawQuery
        data,err:=io.ReadAll(r.Body);if err!=nil{t.Error(err);return}
        lines:=strings.Split(strings.TrimSpace(string(data)),"\n")
        if lines[0]!=strings.TrimSpace(insertSQL){t.Errorf("unexpected insert: %s",lines[0])}
        for _,line:=range lines[1:] {
           var row map[string]any
           if err:=json.Unmarshal([]byte(line),&row);err!=nil{t.Error(err)}
           rows=append(rows,row)
        }
    }));defer server.Close()
    w:=HTTPWriter{Endpoint:server.URL,Database:"telemetry",User:"telemetry",Password:"localdev-only"}
    if err:=w.Insert(context.Background(),[]event.UsageEvent{testEvent(t)});err!=nil{t.Fatal(err)}
    if !strings.Contains(query,"async_insert=0") || !strings.Contains(query,"wait_end_of_query=1") {t.Fatalf("unsafe insert options: %s",query)}
    if len(rows)!=1 || rows[0]["model"]!="Qwen3-32B" || rows[0]["event_time"]!="2026-11-16 11:00:00.000" {t.Fatalf("wrong rows: %+v",rows)}
    if _,ok:=rows[0]["payload_hash"];ok {t.Fatal("payload_hash not in raw table schema")}
}

func TestInsertNeverAcceptsInvalidEvents(t *testing.T) {
    called:=false
    server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,_ *http.Request){called=true}));defer server.Close()
    e:=testEvent(t);e.PromptTokens++
    if err:=(&HTTPWriter{Endpoint:server.URL}).Insert(context.Background(),[]event.UsageEvent{e});err==nil {t.Fatal("tampered event accepted")}
    if called {t.Fatal("HTTP request sent before validation")}
}

func TestInsertErrorIsNotTreatedAsSuccess(t *testing.T) {
    server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,_ *http.Request){http.Error(w,"Code: 241 memory limit",500)}));defer server.Close()
    if err:=(&HTTPWriter{Endpoint:server.URL}).Insert(context.Background(),[]event.UsageEvent{testEvent(t)});err==nil {t.Fatal("HTTP 500 accepted")}
}
