// reconciler compares PostgreSQL authoritative usage with the ClickHouse
// analytical projection. It never repairs, commits Kafka offsets or bills.
package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "flag"
    "fmt"
    "os"
    "time"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/reconcile"
    _ "github.com/jackc/pgx/v5/stdlib"
)

func run() int {
    flags:=flag.NewFlagSet("reconciler",flag.ContinueOnError)
    startText:=flags.String("start","","inclusive UTC RFC3339 event-time boundary")
    endText:=flags.String("end","","exclusive UTC RFC3339 event-time boundary")
    grace:=flags.Duration("grace",3*time.Minute,"grace after Outbox published_at")
    max:=flags.Int("max-records",1000,"bounded results per source, max 10000")
    dsn:=flags.String("dsn",os.Getenv("POSTGRES_DSN"),"PostgreSQL DSN (prefer POSTGRES_DSN env)")
    endpoint:=flags.String("clickhouse",os.Getenv("CLICKHOUSE_HTTP"),"ClickHouse HTTP endpoint (prefer CLICKHOUSE_HTTP env)")
    user:=flags.String("clickhouse-user",os.Getenv("CLICKHOUSE_USER"),"ClickHouse user")
    password:=flags.String("clickhouse-password",os.Getenv("CLICKHOUSE_PASSWORD"),"ClickHouse password (prefer env)")
    if err:=flags.Parse(os.Args[1:]);err!=nil{fmt.Fprintln(os.Stderr,err);return 1}
    if *dsn==""||*endpoint==""||*startText==""||*endText==""{
        fmt.Fprintln(os.Stderr,"POSTGRES_DSN, CLICKHOUSE_HTTP, --start and --end are required")
        return 1
    }
    start,err:=time.Parse(time.RFC3339Nano,*startText)
    if err!=nil{fmt.Fprintf(os.Stderr,"invalid --start: %v\n",err);return 1}
    end,err:=time.Parse(time.RFC3339Nano,*endText)
    if err!=nil{fmt.Fprintf(os.Stderr,"invalid --end: %v\n",err);return 1}
    ctx,cancel:=context.WithTimeout(context.Background(),90*time.Second)
    defer cancel()
    db,err:=sql.Open("pgx",*dsn)
    if err!=nil{fmt.Fprintf(os.Stderr,"PostgreSQL connection: %v\n",err);return 1}
    defer db.Close()
    if err:=db.PingContext(ctx);err!=nil{fmt.Fprintf(os.Stderr,"PostgreSQL unavailable: %v\n",err);return 1}
    report,err:=reconcile.Run(ctx,reconcile.Options{
        Start:start.UTC(),End:end.UTC(),Now:time.Now().UTC(),
        Grace:*grace,MaxRecords:*max,
    },reconcile.PostgreSQL{DB:db},reconcile.ClickHouseHTTP{
        Endpoint:*endpoint,Database:"telemetry",User:*user,Password:*password,
    })
    if err!=nil{fmt.Fprintf(os.Stderr,"reconciliation incomplete or failed: %v\n",err);return 1}
    enc:=json.NewEncoder(os.Stdout)
    enc.SetIndent("","  ")
    if err:=enc.Encode(report);err!=nil{fmt.Fprintf(os.Stderr,"report output: %v\n",err);return 1}
    switch report.Result {
    case "PASS":return 0
    case "FAIL":return 2
    case "INCOMPLETE":return 3
    default:fmt.Fprintln(os.Stderr,"invalid report result");return 1
    }
}
func main(){os.Exit(run())}
