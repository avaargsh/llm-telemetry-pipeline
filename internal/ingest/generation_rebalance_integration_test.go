package ingest

import (
    "context"
    "encoding/json"
    "fmt"
    "os"
    "strings"
    "sync"
    "testing"
    "time"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/clickhouse"
    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
    "github.com/segmentio/kafka-go"
)

// firstGenerationSink performs a REAL ClickHouse write and then simulates a
// long-running handler. It returns success only AFTER its generation has
// been revoked. Broker-side commits must never occur from that old owner.
type firstGenerationSink struct{
    real Inserter
    mu sync.Mutex
    blocked map[string]bool
    inserted chan string
    revoked chan string
}
func(s *firstGenerationSink)Insert(ctx context.Context,es []event.UsageEvent)error{
    if err:=s.real.Insert(ctx,es);err!=nil{return err}
    if len(es)!=1{return fmt.Errorf("test requires one event per batch")}
    id:=es[0].EventID
    s.mu.Lock()
    first:=!s.blocked[id]
    s.blocked[id]=true
    s.mu.Unlock()
    if first{
        s.inserted<-id
        <-ctx.Done()
        s.revoked<-id
        // The server already persisted the batch. A stale handler can still
        // return success; only generation fencing must block its offset commit.
    }
    return nil
}
type commitEvent struct{
    worker string
    id string
}
func waitM3B(t *testing.T,ctx context.Context,events <-chan string,description string)string{
    t.Helper()
    select{
    case value:=<-events:return value
    case <-ctx.Done():t.Fatalf("timeout waiting for %s: %v",description,ctx.Err());return ""
    }
}
func testEventForRebalance(t *testing.T,seed string,index int)event.UsageEvent{
    t.Helper()
    e,err:=event.SyntheticUsageEvent(seed,index,time.Date(2026,11,16,11,0,0,0,time.UTC))
    if err!=nil{t.Fatal(err)}
    return e
}

func TestRealRebalanceRejectsStaleCommitsAndReplays(t *testing.T){
    broker,chURL:=os.Getenv("TEST_KAFKA_BROKER"),os.Getenv("TEST_CLICKHOUSE_HTTP")
    if broker==""||chURL==""{t.Skip("requires TEST_KAFKA_BROKER and TEST_CLICKHOUSE_HTTP")}
    const topic="m3b.rebalance.v1"
    ctx,stop:=context.WithTimeout(context.Background(),110*time.Second)
    defer stop()
    seed:=fmt.Sprintf("m3b-%d",time.Now().UnixNano())
    events:=[]event.UsageEvent{
        testEventForRebalance(t,seed,0),
        testEventForRebalance(t,seed,1),
    }
    // Publish one event per explicit partition. The group starts with only
    // worker A; both partitions are assigned to it, and both INSERTs block
    // AFTER a confirmed ClickHouse write.
    for partition,e:=range events{
        conn,err:=kafka.DialLeader(ctx,"tcp",broker,topic,partition)
        if err!=nil{t.Fatal(err)}
        if err:=conn.SetWriteDeadline(time.Now().Add(10*time.Second));err!=nil{conn.Close();t.Fatal(err)}
        raw,err:=json.Marshal(e);if err!=nil{conn.Close();t.Fatal(err)}
        _,err=conn.WriteMessages(kafka.Message{Key:[]byte(e.EventID),Value:raw})
        _=conn.Close()
        if err!=nil{t.Fatalf("produce partition %d: %v",partition,err)}
    }

    sink:=&clickhouse.HTTPWriter{Endpoint:chURL,Database:"telemetry",
        User:"telemetry",Password:"localdev-only"}
    blocked:=&firstGenerationSink{
        real:sink,blocked:make(map[string]bool),
        inserted:make(chan string,2),revoked:make(chan string,2),
    }
    commits:=make(chan commitEvent,16)
    group:=fmt.Sprintf("m3b-rebalance-%d",time.Now().UnixNano())
    a:=GroupWorker{
        Brokers:[]string{broker},Topic:topic,GroupID:group,
        BatchSize:1,FetchWait:300*time.Millisecond,Sink:blocked,
        OnCommitted:func(m Message){
            var e event.UsageEvent
            if err:=json.Unmarshal(m.Value,&e);err==nil{commits<-commitEvent{"A",e.EventID}}
        },
    }
    b:=GroupWorker{
        Brokers:[]string{broker},Topic:topic,GroupID:group,
        BatchSize:1,FetchWait:300*time.Millisecond,Sink:sink,
        OnCommitted:func(m Message){
            var e event.UsageEvent
            if err:=json.Unmarshal(m.Value,&e);err==nil{commits<-commitEvent{"B",e.EventID}}
        },
    }
    workerCtx,cancelWorkers:=context.WithCancel(ctx)
    defer cancelWorkers()
    results:=make(chan error,2)
    go func(){results<-a.Run(workerCtx)}()

    seenInserted:=map[string]bool{}
    for len(seenInserted)<2{
        id:=waitM3B(t,ctx,blocked.inserted,"both first-generation inserts")
        seenInserted[id]=true
    }
    // A has written both physical rows but has not committed either offset.
    for _,e:=range events{
        selector:=fmt.Sprintf("event_id=toUUID('%s')",e.EventID)
        if got:=m3Query(t,chURL,"SELECT count() FROM inference_events_raw WHERE "+
            selector+" FORMAT TabSeparatedRaw");got!="1"{
            t.Fatalf("first insert did not persist for %s: %s",e.EventID,got)
        }
    }
    go func(){results<-b.Run(workerCtx)}()
    // The coordinator must revoke A's old generation so the blocked
    // handlers complete. Neither is allowed to commit from the stale gen.
    revoked:=map[string]bool{}
    for len(revoked)<2{
        revoked[waitM3B(t,ctx,blocked.revoked,"generation revocation")]=true
    }
    committed:=map[string]string{}
    bCommits:=0
    for len(committed)<2||bCommits==0{
        select{
        case c:=<-commits:
            committed[c.id]=c.worker
            if c.worker=="B"{bCommits++}
        case err:=<-results:
            if err!=nil && workerCtx.Err()==nil{t.Fatalf("worker stopped before recovery: %v",err)}
        case <-ctx.Done():
            t.Fatalf("rebalance never recovered all events: %v, commits=%v",ctx.Err(),committed)
        }
    }
    cancelWorkers()
    for i:=0;i<2;i++{
        select{
        case err:=<-results:
            if err!=nil&&err!=context.Canceled{t.Logf("worker stopped: %v",err)}
        case <-time.After(15*time.Second):
            t.Fatal("worker did not stop on context cancellation")
        }
    }
    report:=map[string]any{
        "scenario":"two_workers_two_partitions_revoke_after_clickhouse_ack",
        "group_id":group,"initial_insertions":2,"revoked_inflight_handlers":len(revoked),
        "post_rebalance_commits":len(committed),"worker_b_commits":bCommits,
        "events":[]map[string]any{},
    }
    for _,e:=range events{
        selector:=fmt.Sprintf("event_id=toUUID('%s')",e.EventID)
        raw:=m3Query(t,chURL,"SELECT count() FROM inference_events_raw WHERE "+selector+" FORMAT TabSeparatedRaw")
        expected:=fmt.Sprintf("1\t%d\t2",e.PromptTokens+e.CompletionTokens)
        projection:=m3Query(t,chURL,"SELECT count(), sum(prompt_tokens+completion_tokens), sum(raw_copies) "+
            "FROM inference_events_dedup_v1 WHERE "+selector+" FORMAT TabSeparatedRaw")
        conflicts:=m3Query(t,chURL,"SELECT count() FROM inference_events_integrity_v1 WHERE "+selector+" FORMAT TabSeparatedRaw")
        if raw!="2"||projection!=expected||conflicts!="0"{
            t.Fatalf("event=%s raw=%s trusted=%q expected=%q conflicts=%s",
                e.EventID,raw,projection,expected,conflicts)
        }
        report["events"]=append(report["events"].([]map[string]any),map[string]any{
            "event_id":e.EventID,"raw_copies":2,"logical_events":1,
            "tokens":e.PromptTokens+e.CompletionTokens,"integrity_conflicts":0,
        })
    }
    if path:=os.Getenv("M3B_REPORT_PATH");path!=""{
        buf,err:=json.MarshalIndent(report,"","  ")
        if err!=nil{t.Fatal(err)}
        if err:=os.WriteFile(path,buf,0600);err!=nil{t.Fatal(err)}
    }
    t.Logf("PASS M3b: A lost both in-flight assignments, B committed >=1, all events replayed, raw copies=2, trusted logical events=1 each; group=%s",group)
}
