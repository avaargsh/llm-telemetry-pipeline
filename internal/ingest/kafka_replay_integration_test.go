package ingest

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "net/http"
    "net/http/httptest"
    "os"
    "strings"
    "testing"
    "time"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/clickhouse"
    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
    "github.com/segmentio/kafka-go"
)

type injectedCommitFailure struct{ calls int }
func (c *injectedCommitFailure) Commit(context.Context, []Message) error {
    c.calls++
    return errors.New("injected failure after ClickHouse ACK and before Kafka offset commit")
}

type kafkaCommitterM3 struct{ reader *kafka.Reader }
func (c kafkaCommitterM3) Commit(ctx context.Context, messages []Message) error {
    commits:=make([]kafka.Message,0,len(messages))
    for _,m:=range messages {
        commits=append(commits,kafka.Message{Topic:m.Topic,Partition:int(m.Partition),Offset:m.Offset})
    }
    return c.reader.CommitMessages(ctx,commits...)
}

func m3Reader(broker,topic,group string) *kafka.Reader {
    return kafka.NewReader(kafka.ReaderConfig{
        Brokers:[]string{broker},Topic:topic,GroupID:group,
        StartOffset:kafka.FirstOffset,CommitInterval:0,MinBytes:1,
        MaxBytes:10e6,MaxWait:250*time.Millisecond,
    })
}

func m3Query(t *testing.T,endpoint,sql string)string{
    t.Helper()
    ctx,cancel:=context.WithTimeout(context.Background(),12*time.Second)
    defer cancel()
    req,err:=http.NewRequestWithContext(ctx,http.MethodPost,endpoint+"?database=telemetry",strings.NewReader(sql))
    if err!=nil{t.Fatal(err)}
    req.SetBasicAuth("telemetry","localdev-only")
    resp,err:=http.DefaultClient.Do(req)
    if err!=nil{t.Fatal(err)}
    defer resp.Body.Close()
    raw,err:=io.ReadAll(io.LimitReader(resp.Body,512*1024))
    if err!=nil{t.Fatal(err)}
    if resp.StatusCode!=200{t.Fatalf("ClickHouse %s: %s",resp.Status,string(raw))}
    return strings.TrimSpace(string(raw))
}

func m3Fetch(t *testing.T,r *kafka.Reader) kafka.Message {
    t.Helper()
    ctx,cancel:=context.WithTimeout(context.Background(),30*time.Second)
    defer cancel()
    msg,err:=r.FetchMessage(ctx)
    if err!=nil{t.Fatalf("Kafka fetch: %v",err)}
    return msg
}

func m3LostACKProxy(t *testing.T,clickhouseURL string)(*clickhouse.HTTPWriter,func()){
    t.Helper()
    proxy:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
        upstream,err:=http.NewRequestWithContext(r.Context(),r.Method,clickhouseURL+r.URL.RequestURI(),r.Body)
        if err!=nil{t.Errorf("proxy request: %v",err);return}
        upstream.Header=r.Header.Clone()
        resp,err:=http.DefaultClient.Do(upstream)
        if err!=nil{t.Errorf("ClickHouse upstream: %v",err);return}
        _,err=io.Copy(io.Discard,resp.Body)
        resp.Body.Close()
        if err!=nil || resp.StatusCode!=200 {
            t.Errorf("ClickHouse write was not confirmed before ACK loss: status=%d err=%v",resp.StatusCode,err)
            http.Error(w,"upstream failure",http.StatusBadGateway)
            return
        }
        // The ClickHouse server has acknowledged the INSERT; intentionally
        // close the client socket before a single response byte reaches it.
        conn,_,err:=w.(http.Hijacker).Hijack()
        if err!=nil{t.Errorf("hijack: %v",err);return}
        _=conn.Close()
    }))
    return &clickhouse.HTTPWriter{
        Endpoint:proxy.URL,Database:"telemetry",User:"telemetry",Password:"localdev-only",
        Client:&http.Client{Timeout:5*time.Second},
    },proxy.Close
}

// TestKafkaCrashWindows uses REAL Kafka group commits and REAL ClickHouse.
// It never injects faults into a production worker CLI.
func TestKafkaCrashWindowsPreserveLogicalUsage(t *testing.T){
    broker:=os.Getenv("TEST_KAFKA_BROKER")
    chURL:=os.Getenv("TEST_CLICKHOUSE_HTTP")
    if broker=="" || chURL=="" {t.Skip("requires TEST_KAFKA_BROKER and TEST_CLICKHOUSE_HTTP")}
    const topic="usage.critical.v1"
    group:=fmt.Sprintf("m3a-%d",time.Now().UnixNano())
    producer:=&kafka.Writer{
        Addr:kafka.TCP(broker),Topic:topic,RequiredAcks:kafka.RequireAll,
        Balancer:&kafka.Hash{},Async:false,AllowAutoTopicCreation:false,
    }
    defer producer.Close()
    goodSink:=&clickhouse.HTTPWriter{
        Endpoint:chURL,Database:"telemetry",User:"telemetry",Password:"localdev-only",
    }
    for caseIndex,scenario:=range []string{"offset_commit_failed","clickhouse_ack_lost"}{
        t.Run(scenario,func(t *testing.T){
            e,err:=event.SyntheticUsageEvent(fmt.Sprintf("%s-%d",group,caseIndex),1,
                time.Date(2026,11,16,11,0,0,0,time.UTC))
            if err!=nil{t.Fatal(err)}
            payload,err:=json.Marshal(e)
            if err!=nil{t.Fatal(err)}
            ctx,cancel:=context.WithTimeout(context.Background(),30*time.Second)
            defer cancel()
            if err:=producer.WriteMessages(ctx,kafka.Message{Key:[]byte(e.EventID),Value:payload});err!=nil{t.Fatal(err)}
            first:=m3Reader(broker,topic,group)
            msg:=m3Fetch(t,first)
            if string(msg.Value)!=string(payload){first.Close();t.Fatalf("unexpected message at %d: got %s",msg.Offset,string(msg.Value))}
            source:=Message{Topic:msg.Topic,Partition:int32(msg.Partition),Offset:msg.Offset,Value:msg.Value}
            sink:=goodSink
            if scenario=="clickhouse_ack_lost"{
                unknown,stop:=m3LostACKProxy(t,chURL)
                defer stop()
                sink=unknown
            }
            // First consumer processes the record but never commits source
            // offsets. Acknowledge uncertainty and exit exactly as production
            // must do on failed commits or unknown ClickHouse results.
            attemptedCommit:=&injectedCommitFailure{}
            err=ProcessBatch(ctx,[]Message{source},sink,attemptedCommit)
            if err==nil {
                first.Close();t.Fatal("expected failure before Kafka commit")
            }
            if scenario=="clickhouse_ack_lost" && attemptedCommit.calls!=0{
                first.Close();t.Fatal("client mistakenly committed Kafka offset despite unknown ClickHouse ACK")
            }
            if scenario=="offset_commit_failed" && attemptedCommit.calls!=1{
                first.Close();t.Fatal("the injected Kafka commit failure was not actually exercised")
            }
            if err:=first.Close();err!=nil{t.Fatal(err)}
            selector:=fmt.Sprintf("event_id=toUUID('%s')",e.EventID)
            if rows:=m3Query(t,chURL,"SELECT count() FROM inference_events_raw WHERE "+selector+" FORMAT TabSeparatedRaw");rows!="1"{
                t.Fatalf("first ClickHouse insert did not persist before failure: raw=%s",rows)
            }
            second:=m3Reader(broker,topic,group)
            replay:=m3Fetch(t,second)
            if replay.Partition!=msg.Partition || replay.Offset!=msg.Offset{
                second.Close()
                t.Fatalf("Kafka did not replay uncommitted record: before=%d/%d after=%d/%d",
                    msg.Partition,msg.Offset,replay.Partition,replay.Offset)
            }
            replayMsg:=Message{Topic:replay.Topic,Partition:int32(replay.Partition),Offset:replay.Offset,Value:replay.Value}
            if err:=ProcessBatch(ctx,[]Message{replayMsg},goodSink,kafkaCommitterM3{second});err!=nil{
                second.Close();t.Fatal(err)
            }
            if err:=second.Close();err!=nil{t.Fatal(err)}
            if rows:=m3Query(t,chURL,"SELECT count() FROM inference_events_raw WHERE "+selector+" FORMAT TabSeparatedRaw");rows!="2"{
                t.Fatalf("raw duplicates not proven: %s",rows)
            }
            got:=m3Query(t,chURL,"SELECT count(), sum(prompt_tokens+completion_tokens), sum(raw_copies) FROM inference_events_dedup_v1 WHERE "+selector+" FORMAT TabSeparatedRaw")
            want:=fmt.Sprintf("1\t%d\t2",e.PromptTokens+e.CompletionTokens)
            if got!=want{t.Fatalf("replay inflated trusted event usage: got=%q want=%q",got,want)}
            bad:=m3Query(t,chURL,"SELECT count() FROM inference_events_integrity_v1 WHERE "+selector+" FORMAT TabSeparatedRaw")
            if bad!="0"{t.Fatalf("identical replay incorrectly marked as integrity conflict: %s",bad)}
            t.Logf("PASS %s: original Kafka offset=%d, raw ClickHouse rows=2, logical events=1, token usage=%d",
                scenario,msg.Offset,e.PromptTokens+e.CompletionTokens)
        })
    }
}
