// usage-query-api exposes a read-only project-scoped ledger summary.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
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
	listen := flag.String("listen", "127.0.0.1:8086", "HTTP address; non-loopback requires TLS")
	cert := flag.String("tls-cert", "", "TLS certificate")
	key := flag.String("tls-key", "", "TLS private key")
	flag.Parse()
	host, _, err := net.SplitHostPort(*listen)
	if err != nil { return errors.New("invalid listen address") }
	ip := net.ParseIP(host)
	if (*cert == "") != (*key == "") || ((ip == nil || !ip.IsLoopback()) && *cert == "") {
		return errors.New("remote listeners require TLS certificate and key")
	}
	dsn := os.Getenv("USAGE_QUERY_DATABASE_URL")
	if dsn == "" { return errors.New("USAGE_QUERY_DATABASE_URL is required") }
	db, err := sql.Open("pgx", dsn)
	if err != nil { return errors.New("open query database failed") }
	defer db.Close()
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(30*time.Minute)
	credential := ingestapi.Credential{
		Token: os.Getenv("USAGE_QUERY_TOKEN"),
		TenantID: os.Getenv("USAGE_QUERY_TENANT_ID"),
		ProjectID: os.Getenv("USAGE_QUERY_PROJECT_ID"),
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	reader, err := ledger.NewScopedUsageReader(checkCtx, db, credential.TenantID, credential.ProjectID)
	cancel()
	if err != nil { return errors.New("query database scope readiness failed") }
	h, err := ingestapi.NewUsageQueryHandler(reader, []ingestapi.Credential{credential})
	if err != nil { return err }
	server := &http.Server{Addr: *listen, Handler: h, ReadHeaderTimeout: 5*time.Second,
		ReadTimeout: 10*time.Second, WriteTimeout: 10*time.Second, IdleTimeout: 30*time.Second, MaxHeaderBytes: 8<<10}
	go func() {
		<-ctx.Done()
		shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = server.Shutdown(shutdown)
	}()
	log.Printf("usage query listening on %s", *listen)
	if *cert != "" { err = server.ListenAndServeTLS(*cert, *key) } else { err = server.ListenAndServe() }
	if errors.Is(err, http.ErrServerClosed) { return nil }
	return err
}
