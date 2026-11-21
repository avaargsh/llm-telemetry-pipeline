// usage-ingest-api exposes a scoped HTTP boundary for the authoritative
// PostgreSQL usage Ledger + Outbox; it never writes usage directly to Kafka.
package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "errors"
    "flag"
    "fmt"
    "io"
    "log"
    "net"
    "net/http"
    "os"
    "os/signal"
    "strings"
    "syscall"
    "time"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/ingestapi"
    "github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
    _ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
    if err := run(); err != nil { log.Fatal(err) }
}

func run() error {
    listen := flag.String("listen", "127.0.0.1:8085", "HTTP listen address; non-loopback requires TLS")
    dsn := flag.String("dsn", os.Getenv("DATABASE_URL"), "PostgreSQL DSN (or DATABASE_URL)")
    cert := flag.String("tls-cert", "", "TLS certificate path")
    key := flag.String("tls-key", "", "TLS private key path")
    flag.Parse()
    if *dsn == "" {return errors.New("DATABASE_URL or --dsn is required")}
    if (*cert == "") != (*key == "") {return errors.New("tls-cert and tls-key must be configured together")}
    if requiresTLS(*listen) && *cert == "" {return errors.New("non-loopback binding requires TLS; use a loopback listener for development")}
    raw := os.Getenv("USAGE_INGEST_CREDENTIALS_JSON")
    if raw == "" {return errors.New("USAGE_INGEST_CREDENTIALS_JSON is required")}
    var credentials []ingestapi.Credential
    decoder := json.NewDecoder(strings.NewReader(raw))
    decoder.DisallowUnknownFields()
    if err := decoder.Decode(&credentials); err != nil {return fmt.Errorf("invalid credentials configuration: %w", err)}
    var extra any
    if err := decoder.Decode(&extra); err == nil {
        return errors.New("credentials configuration contains trailing JSON")
    } else if err != io.EOF {
        return errors.New("credentials configuration contains trailing content")
    }
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()
    db,err:=sql.Open("pgx",*dsn)
    if err!=nil{return fmt.Errorf("open PostgreSQL: %w",err)}
    defer db.Close()
    db.SetMaxOpenConns(20)
    db.SetMaxIdleConns(10)
    db.SetConnMaxLifetime(30*time.Minute)
    pingCtx, cancel := context.WithTimeout(ctx,5*time.Second)
    defer cancel()
    if err:=db.PingContext(pingCtx);err!=nil{return fmt.Errorf("PostgreSQL readiness: %w",err)}
    handler,err:=ingestapi.NewHandler(&ledger.Store{DB:db},credentials)
    if err!=nil{return fmt.Errorf("invalid ingest configuration: %w",err)}
    server:=&http.Server{Addr:*listen,Handler:handler,ReadHeaderTimeout:5*time.Second,ReadTimeout:10*time.Second,WriteTimeout:20*time.Second,IdleTimeout:60*time.Second,MaxHeaderBytes:8<<10}
    go func(){
        <-ctx.Done()
        shutdownCtx, done := context.WithTimeout(context.Background(),5*time.Second)
        defer done()
        _=server.Shutdown(shutdownCtx)
    }()
    log.Printf("usage ingestion listening on %s (PostgreSQL is authoritative; Kafka publication is asynchronous)",*listen)
    if *cert!=""{err=server.ListenAndServeTLS(*cert,*key)}else{err=server.ListenAndServe()}
    if errors.Is(err,http.ErrServerClosed){return nil}
    return err
}

func requiresTLS(addr string)bool{
    host,_,err:=net.SplitHostPort(addr)
    if err!=nil{return true}
    if strings.EqualFold(host,"localhost"){return false}
    ip:=net.ParseIP(host)
    return ip==nil||!ip.IsLoopback()
}
