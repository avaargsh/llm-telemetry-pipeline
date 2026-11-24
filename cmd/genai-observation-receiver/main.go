// genai-observation-receiver is the independently deployable durable OTLP receiver.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/genaiobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const maxCredentialFileBytes = 64 << 10

var tenantIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@-]{0,127}$`)

type credentialFileEntry struct {
	Token    string `json:"token"`
	TenantID string `json:"tenant_id"`
}

func validCredentialToken(token string) bool {
	if len(token) < 32 || len(token) > 512 {
		return false
	}
	for i := 0; i < len(token); i++ {
		if token[i] < 33 || token[i] > 126 || token[i] == ',' {
			return false
		}
	}
	return true
}

// loadCredentials accepts a small, strict JSON file. Errors deliberately never
// include file contents, tenant identifiers or bearer credentials.
func loadCredentials(path string) ([]genaiobs.OTLPCredential, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("credentials file path is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("could not open credentials file")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o027 != 0 {
		return nil, errors.New("credentials file must be a regular file with restricted permissions")
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxCredentialFileBytes+1))
	if err != nil || len(raw) > maxCredentialFileBytes {
		return nil, errors.New("credentials file is invalid or too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var entries []credentialFileEntry
	if err := decoder.Decode(&entries); err != nil {
		return nil, errors.New("credentials file contains invalid JSON")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("credentials file contains trailing JSON")
	}
	if len(entries) == 0 || len(entries) > 128 {
		return nil, errors.New("credentials file must contain between 1 and 128 entries")
	}
	credentials := make([]genaiobs.OTLPCredential, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if !validCredentialToken(entry.Token) || !tenantIDPattern.MatchString(entry.TenantID) {
			return nil, errors.New("credentials file contains an invalid entry")
		}
		if _, exists := seen[entry.Token]; exists {
			return nil, errors.New("credentials file contains a duplicate credential")
		}
		seen[entry.Token] = struct{}{}
		credentials = append(credentials, genaiobs.OTLPCredential{Token: entry.Token, TenantID: entry.TenantID})
	}
	return credentials, nil
}

func validateTLSConfig(addr, certFile, keyFile string) error {
	if (certFile == "") != (keyFile == "") {
		return errors.New("TLS certificate and key must be configured together")
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return errors.New("invalid receiver listen address")
	}
	loopback := strings.EqualFold(host, "localhost")
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		loopback = true
	}
	remote := host == "" || !loopback
	if remote && certFile == "" {
		return errors.New("TLS is required for non-loopback receiver listeners")
	}
	return nil
}

type responseStatusWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseStatusWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseStatusWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

type receiverMetrics struct {
	requests       atomic.Uint64
	accepted       atomic.Uint64
	rejected       atomic.Uint64
	failed         atomic.Uint64
	durationNanos  atomic.Uint64
	durationCount  atomic.Uint64
}

func (m *receiverMetrics) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &responseStatusWriter{ResponseWriter: w}
		next.ServeHTTP(recorder, r)
		status := recorder.status
		if status == 0 {
			status = http.StatusOK
		}
		m.requests.Add(1)
		switch {
		case status >= 200 && status < 300:
			m.accepted.Add(1)
		case status >= 500:
			m.failed.Add(1)
		default:
			m.rejected.Add(1)
		}
		m.durationNanos.Add(uint64(time.Since(started).Nanoseconds()))
		m.durationCount.Add(1)
	})
}

func (m *receiverMetrics) metricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		fmt.Fprintln(w, "# HELP llm_telemetry_genai_receiver_up Receiver process is running.")
		fmt.Fprintln(w, "# TYPE llm_telemetry_genai_receiver_up gauge")
		fmt.Fprintln(w, "llm_telemetry_genai_receiver_up 1")
		fmt.Fprintln(w, "# HELP llm_telemetry_genai_receiver_admission_requests_total OTLP admission requests.")
		fmt.Fprintln(w, "# TYPE llm_telemetry_genai_receiver_admission_requests_total counter")
		fmt.Fprintf(w, "llm_telemetry_genai_receiver_admission_requests_total %d\n", m.requests.Load())
		fmt.Fprintln(w, "# HELP llm_telemetry_genai_receiver_admission_accepted_total Accepted durable admissions.")
		fmt.Fprintln(w, "# TYPE llm_telemetry_genai_receiver_admission_accepted_total counter")
		fmt.Fprintf(w, "llm_telemetry_genai_receiver_admission_accepted_total %d\n", m.accepted.Load())
		fmt.Fprintln(w, "# HELP llm_telemetry_genai_receiver_admission_rejected_total Rejected client admissions.")
		fmt.Fprintln(w, "# TYPE llm_telemetry_genai_receiver_admission_rejected_total counter")
		fmt.Fprintf(w, "llm_telemetry_genai_receiver_admission_rejected_total %d\n", m.rejected.Load())
		fmt.Fprintln(w, "# HELP llm_telemetry_genai_receiver_admission_failed_total Failed durable admissions.")
		fmt.Fprintln(w, "# TYPE llm_telemetry_genai_receiver_admission_failed_total counter")
		fmt.Fprintf(w, "llm_telemetry_genai_receiver_admission_failed_total %d\n", m.failed.Load())
		fmt.Fprintln(w, "# HELP llm_telemetry_genai_receiver_admission_duration_seconds Admission latency.")
		fmt.Fprintln(w, "# TYPE llm_telemetry_genai_receiver_admission_duration_seconds summary")
		fmt.Fprintf(w, "llm_telemetry_genai_receiver_admission_duration_seconds_count %d\n", m.durationCount.Load())
		fmt.Fprintf(w, "llm_telemetry_genai_receiver_admission_duration_seconds_sum %.9f\n", float64(m.durationNanos.Load())/float64(time.Second))
	})
}

func openReceiverDB(dsn string) (*sql.DB, error) {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid observation database configuration")
	}
	db := stdlib.OpenDB(*config, stdlib.OptionAfterConnect(func(ctx context.Context, conn *pgx.Conn) error {
		if _, err := conn.Exec(ctx, "SET ROLE llm_telemetry_observation_receiver"); err != nil {
			return errors.New("cannot assume observation receiver database role")
		}
		if _, err := conn.Exec(ctx, "SET search_path TO pg_catalog, public"); err != nil {
			return errors.New("cannot set restricted database search path")
		}
		return nil
	}))
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return db, nil
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func intEnv(name string, fallback, min, max int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < min || value > max {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return value, nil
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value < time.Second || value > 2*time.Minute {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return value, nil
}

func run() error {
	dsn := strings.TrimSpace(os.Getenv("GENAI_OBSERVATION_DATABASE_URL"))
	if dsn == "" {
		return errors.New("GENAI_OBSERVATION_DATABASE_URL is required")
	}
	credentialsPath := strings.TrimSpace(os.Getenv("GENAI_OBSERVATION_CREDENTIALS_FILE"))
	credentials, err := loadCredentials(credentialsPath)
	if err != nil {
		return err
	}
	listenAddr := envOr("GENAI_OBSERVATION_LISTEN_ADDR", ":4318")
	certFile := strings.TrimSpace(os.Getenv("GENAI_OBSERVATION_TLS_CERT_FILE"))
	keyFile := strings.TrimSpace(os.Getenv("GENAI_OBSERVATION_TLS_KEY_FILE"))
	if err := validateTLSConfig(listenAddr, certFile, keyFile); err != nil {
		return err
	}
	capacity, err := intEnv("GENAI_OBSERVATION_QUEUE_CAPACITY", 10000, 1, genaiobs.MaxDurableQueueCapacity)
	if err != nil {
		return err
	}
	concurrent, err := intEnv("GENAI_OBSERVATION_MAX_CONCURRENT", 8, 1, 64)
	if err != nil {
		return err
	}
	requestTimeout, err := durationEnv("GENAI_OBSERVATION_HTTP_TIMEOUT", 15*time.Second)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	db, err := openReceiverDB(dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	pingCtx, pingCancel := context.WithTimeout(ctx, 10*time.Second)
	err = db.PingContext(pingCtx)
	pingCancel()
	if err != nil {
		return errors.New("observation PostgreSQL is unavailable or receiver role is not granted")
	}
	store, err := genaiobs.NewDurableStore(db, capacity)
	if err != nil {
		return fmt.Errorf("invalid durable observation store configuration: %w", err)
	}
	otlpHandler, err := genaiobs.NewDurableOTLPHTTPHandler(store, credentials, concurrent)
	if err != nil {
		return errors.New("invalid durable observation receiver configuration")
	}
	metrics := &receiverMetrics{}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		readyCtx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := db.PingContext(readyCtx); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ready\n")
	})
	mux.Handle("/metrics", metrics.metricsHandler())
	mux.Handle("/v1/traces", metrics.instrument(otlpHandler))
	mux.Handle("/", http.NotFoundHandler())

	server := &http.Server{
		Addr: listenAddr, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: requestTimeout,
		WriteTimeout: requestTimeout + 5*time.Second,
		IdleTimeout: 60 * time.Second,
		MaxHeaderBytes: 16 << 10,
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}
	serverErrors := make(chan error, 1)
	go func() {
		if certFile != "" {
			serverErrors <- server.ListenAndServeTLS(certFile, keyFile)
			return
		}
		serverErrors <- server.ListenAndServe()
	}()
	log.Printf("GenAI observation receiver listening on %s (TLS=%t)", listenAddr, certFile != "")
	select {
	case serverErr := <-serverErrors:
		if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) {
			return errors.New("observation receiver HTTP server failed")
		}
		return nil
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return errors.New("observation receiver graceful shutdown failed")
		}
		return nil
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
