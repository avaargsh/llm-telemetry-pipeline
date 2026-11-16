// usage-ledger-ingest stores usage.finalized events in PostgreSQL, never in
// process memory alone. The outbox relay publishes them independently.
package main

import (
    "bufio"
    "context"
    "database/sql"
    "encoding/json"
    "flag"
    "log"
    "os"
    "os/signal"
    "syscall"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
    "github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
    _ "github.com/jackc/pgx/v5/stdlib"
)
func main(){
    dsn:=flag.String("dsn","postgres://telemetry:localdev-only@localhost:15432/telemetry?sslmode=disable","PostgreSQL DSN (local dev)")
    flag.Parse()
    ctx,stop:=signal.NotifyContext(context.Background(),syscall.SIGINT,syscall.SIGTERM);defer stop()
    db,err:=sql.Open("pgx",*dsn);if err!=nil{log.Fatal(err)};defer db.Close()
    if err:=db.PingContext(ctx);err!=nil{log.Fatal(err)}
    store:=&ledger.Store{DB:db}
    scan:=bufio.NewScanner(os.Stdin);scan.Buffer(make([]byte,64*1024),8<<20)
    created,duplicates:=0,0
    for scan.Scan(){
        var e event.UsageEvent
        if err:=json.Unmarshal(scan.Bytes(),&e);err!=nil{log.Fatal(err)}
        outcome,err:=store.Record(ctx,e);if err!=nil{log.Fatalf("record event %s: %v",e.EventID,err)}
        if outcome==ledger.Created{created++} else {duplicates++}
    }
    if err:=scan.Err();err!=nil{log.Fatal(err)}
    log.Printf("ledger persisted created=%d duplicates=%d; Kafka delivery is asynchronous",created,duplicates)
}
