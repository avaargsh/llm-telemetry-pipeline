// genai-observation-worker continuously exports durable, non-authoritative
// GenAI observations. It never writes usage_ledger or event_outbox.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/genaiobs"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type workerMetrics struct {
	acknowledged atomic.Uint64
	retryable    atomic.Uint64
	terminal     atomic.Uint64
	errors       atomic.Uint64
	idle         atomic.Uint64
	queueLength  atomic.Int64
	queueReady   atomic.Bool
}

func (m *workerMetrics) handler(db *sql.DB) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		fmt.Fprintln(w, "# HELP llm_telemetry_genai_worker_up Worker process is running.")
		fmt.Fprintln(w, "# TYPE llm_telemetry_genai_worker_up gauge")
		fmt.Fprintln(w, "llm_telemetry_genai_worker_up 1")
		fmt.Fprintln(w, "# HELP llm_telemetry_genai_worker_acknowledged_total Receiver 2xx responses followed by durable sender ACK.")
		fmt.Fprintln(w, "# TYPE llm_telemetry_genai_worker_acknowledged_total counter")
		fmt.Fprintf(w, "llm_telemetry_genai_worker_acknowledged_total %d\n", m.acknowledged.Load())
		fmt.Fprintln(w, "# HELP llm_telemetry_genai_worker_retryable_total Persisted retry outcomes.")
		fmt.Fprintln(w, "# TYPE llm_telemetry_genai_worker_retryable_total counter")
		fmt.Fprintf(w, "llm_telemetry_genai_worker_retryable_total %d\n", m.retryable.Load())
		fmt.Fprintln(w, "# HELP llm_telemetry_genai_worker_terminal_total Rows moved to terminal state.")
		fmt.Fprintln(w, "# TYPE llm_telemetry_genai_worker_terminal_total counter")
		fmt.Fprintf(w, "llm_telemetry_genai_worker_terminal_total %d\n", m.terminal.Load())
		fmt.Fprintln(w, "# HELP llm_telemetry_genai_worker_errors_total Database or worker operational errors.")
		fmt.Fprintln(w, "# TYPE llm_telemetry_genai_worker_errors_total counter")
		fmt.Fprintf(w, "llm_telemetry_genai_worker_errors_total %d\n", m.errors.Load())
		fmt.Fprintln(w, "# HELP llm_telemetry_genai_worker_idle_polls_total Polls that found no eligible row.")
		fmt.Fprintln(w, "# TYPE llm_telemetry_genai_worker_idle_polls_total counter")
		fmt.Fprintf(w, "llm_telemetry_genai_worker_idle_polls_total %d\n", m.idle.Load())
		fmt.Fprintln(w, "# HELP llm_telemetry_genai_worker_queue_length Current queued rows, including terminal rows.")
		fmt.Fprintln(w, "# TYPE llm_telemetry_genai_worker_queue_length gauge")
		fmt.Fprintf(w, "llm_telemetry_genai_worker_queue_length %d\n", m.queueLength.Load())
		fmt.Fprintln(w, "# HELP llm_telemetry_genai_worker_queue_length_available Whether the last queue-length sample succeeded.")
		fmt.Fprintln(w, "# TYPE llm_telemetry_genai_worker_queue_length_available gauge")
		if m.queueReady.Load() {
			fmt.Fprintln(w, "llm_telemetry_genai_worker_queue_length_available 1")
		} else {
			fmt.Fprintln(w, "llm_telemetry_genai_worker_queue_length_available 0")
		}
	})
	return mux
}

func requiredEnv(name string) (string, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return value, nil
}

func intEnv(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return value, nil
}

func floatEnv(name string, fallback float64) (float64, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return value, nil
}

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	host = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			return r
		}
		return '-'
	}, host)
	return fmt.Sprintf("genaiobs-worker/%s/%d", host, os.Getpid())
}

func run() error {
	dsn, err := requiredEnv("GENAI_OBSERVATION_DATABASE_URL")
	if err != nil {
		return err
	}
	receiverURL, err := requiredEnv("GENAI_OBSERVATION_RECEIVER_URL")
	if err != nil {
		return err
	}
	receiverToken, err := requiredEnv("GENAI_OBSERVATION_RECEIVER_TOKEN")
	if err != nil {
		return err
	}
	capacity, err := intEnv("GENAI_OBSERVATION_QUEUE_CAPACITY", 10000)
	if err != nil {
		return err
	}
	pollInterval, err := durationEnv("GENAI_OBSERVATION_POLL_INTERVAL", time.Second)
	if err != nil {
		return err
	}
	lease, err := durationEnv("GENAI_OBSERVATION_LEASE", 30*time.Second)
	if err != nil {
		return err
	}
	requestTimeout, err := durationEnv("GENAI_OBSERVATION_HTTP_TIMEOUT", 10*time.Second)
	if err != nil {
		return err
	}
	maxAttempts, err := intEnv("GENAI_OBSERVATION_MAX_ATTEMPTS", 5)
	if err != nil {
		return err
	}
	initialBackoff, err := durationEnv("GENAI_OBSERVATION_INITIAL_BACKOFF", time.Second)
	if err != nil {
		return err
	}
	maxBackoff, err := durationEnv("GENAI_OBSERVATION_MAX_BACKOFF", 30*time.Second)
	if err != nil {
		return err
	}
	jitter, err := floatEnv("GENAI_OBSERVATION_RETRY_JITTER", 0.2)
	if err != nil {
		return err
	}
	metricsAddr := strings.TrimSpace(os.Getenv("GENAI_OBSERVATION_METRICS_ADDR"))
	if metricsAddr == "" {
		metricsAddr = "127.0.0.1:9109"
	}
	workerID := strings.TrimSpace(os.Getenv("GENAI_OBSERVATION_WORKER_ID"))
	if workerID == "" {
		workerID = defaultWorkerID()
	}
	if pollInterval < 100*time.Millisecond || pollInterval > time.Minute {
		return errors.New("poll interval must be between 100ms and 1m")
	}
	if requestTimeout <= 0 || lease <= requestTimeout+time.Second {
		return errors.New("lease must exceed the positive HTTP timeout by at least 1s")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return errors.New("open observation PostgreSQL connection")
	}
	defer db.Close()
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	pingCtx, pingCancel := context.WithTimeout(ctx, 10*time.Second)
	err = db.PingContext(pingCtx)
	pingCancel()
	if err != nil {
		return errors.New("observation PostgreSQL is unavailable")
	}

	store, err := genaiobs.NewDurableStore(db, capacity)
	if err != nil {
		return fmt.Errorf("invalid durable observation store configuration: %w", err)
	}
	exporter, err := genaiobs.NewHTTPDurableExporter(receiverURL, receiverToken, &http.Client{Timeout: requestTimeout})
	if err != nil {
		return fmt.Errorf("invalid durable receiver configuration: %w", err)
	}
	policy := genaiobs.RetryPolicy{
		MaxAttempts: maxAttempts, InitialBackoff: initialBackoff,
		MaxBackoff: maxBackoff, Jitter: jitter,
	}
	worker, err := genaiobs.NewDurableWorker(store, exporter, workerID, lease, policy)
	if err != nil {
		return fmt.Errorf("invalid durable worker configuration: %w", err)
	}

	metrics := &workerMetrics{}
	server := &http.Server{
		Addr: metricsAddr, Handler: metrics.handler(db),
		ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second,
	}
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.ListenAndServe()
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("GenAI durable observation worker started; metrics=%s", metricsAddr)
	lastQueueSample := time.Time{}
	for {
		select {
		case serverErr := <-serverErrors:
			if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) {
				return fmt.Errorf("worker metrics server failed: %w", serverErr)
			}
			return nil
		default:
		}
		if ctx.Err() != nil {
			return nil
		}
		delivered, runErr := worker.RunOnce(ctx)
		switch {
		case delivered:
			metrics.acknowledged.Add(1)
		case errors.Is(runErr, genaiobs.ErrRetryScheduled):
			metrics.retryable.Add(1)
		case errors.Is(runErr, genaiobs.ErrPermanentFailure), errors.Is(runErr, genaiobs.ErrRetryExhausted):
			metrics.terminal.Add(1)
			log.Printf("GenAI observation entered terminal state: %v", runErr)
		case runErr != nil && !errors.Is(runErr, context.Canceled):
			metrics.errors.Add(1)
			log.Printf("GenAI observation worker operation failed: %v", runErr)
		case runErr == nil:
			metrics.idle.Add(1)
		}

		if time.Since(lastQueueSample) >= 5*time.Second {
			sampleCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			length, sampleErr := store.Len(sampleCtx)
			cancel()
			if sampleErr != nil {
				metrics.queueReady.Store(false)
				if ctx.Err() == nil {
					metrics.errors.Add(1)
				}
			} else {
				metrics.queueLength.Store(int64(length))
				metrics.queueReady.Store(true)
			}
			lastQueueSample = time.Now()
		}

		if !delivered {
			timer := time.NewTimer(pollInterval)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return nil
			case serverErr := <-serverErrors:
				if !timer.Stop() {
					<-timer.C
				}
				if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) {
					return fmt.Errorf("worker metrics server failed: %w", serverErr)
				}
				return nil
			case <-timer.C:
			}
		}
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
