package outbox

import (
    "bytes"
    "context"
    "database/sql"
    "encoding/json"
    "errors"
    "fmt"
    "os"
    "path/filepath"
    "testing"
    "time"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
    "github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
    "github.com/segmentio/kafka-go"
    _ "github.com/jackc/pgx/v5/stdlib"
)

// TestRelayKafkaACKCrashWindowRealBroker models the precise ambiguity window:
// Kafka has ACKed, but the Relay process dies BEFORE MarkPublished. After the
// lease expires, a replacement worker must replay the same sealed event.
// Two physical Kafka messages are legal; two authoritative ledger rows are not.
//
// Requires an isolated PostgreSQL schema and a one-partition Kafka topic.
func TestRelayKafkaACKCrashWindowRealBroker(t *testing.T) {
    dsn := os.Getenv("TEST_POSTGRES_DSN")
    broker := os.Getenv("TEST_KAFKA_BROKER")
    topic := os.Getenv("TEST_M5B_KAFKA_TOPIC")
    if dsn == "" || broker == "" || topic == "" {
        t.Skip("TEST_POSTGRES_DSN, TEST_KAFKA_BROKER and TEST_M5B_KAFKA_TOPIC required")
    }

    ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
    defer cancel()
    db, err := sql.Open("pgx", dsn)
    if err != nil { t.Fatal(err) }
    t.Cleanup(func() { _ = db.Close() })
    if err := db.PingContext(ctx); err != nil { t.Fatal(err) }

    seed := fmt.Sprintf("m5b-ack-crash-%d", time.Now().UnixNano())
    usage, err := event.SyntheticUsageEvent(seed, 0, time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC))
    if err != nil { t.Fatal(err) }
    t.Cleanup(func() {
        _, _ = db.Exec("DELETE FROM event_outbox WHERE event_id=$1::uuid", usage.EventID)
        _, _ = db.Exec("DELETE FROM usage_ledger WHERE event_id=$1::uuid", usage.EventID)
    })
    outcome, err := (&ledger.Store{DB: db}).Record(ctx, usage)
    if err != nil || outcome != ledger.Created { t.Fatalf("initial ledger outcome=%q error=%v", outcome, err) }

    // The default Outbox topic has six partitions. Retarget this isolated
    // test row to a *disposable one-partition topic*, avoiding unrelated
    // messages and making both physical copies directly observable.
    if _, err := db.ExecContext(ctx,
        "UPDATE event_outbox SET topic=$1 WHERE event_id=$2::uuid",
        topic, usage.EventID,
    ); err != nil { t.Fatal(err) }

    store := &Store{DB: db}
    first, err := store.Claim(ctx, "m5b-worker-a", 60*time.Second, 1)
    if err != nil || len(first) != 1 { t.Fatalf("first claim count=%d err=%v", len(first), err) }
    rowA := first[0]
    if rowA.EventID != usage.EventID || rowA.Attempts != 1 || rowA.Topic != topic {
        t.Fatalf("unexpected initial claim: %+v", rowA)
    }

    writer := &kafka.Writer{
        Addr: kafka.TCP(broker),
        Topic: topic,
        Balancer: &kafka.Hash{},
        RequiredAcks: kafka.RequireAll,
        Async: false,
        AllowAutoTopicCreation: false,
    }
    defer writer.Close()
    publish := func(row Row) {
        t.Helper()
        if err := writer.WriteMessages(ctx, kafka.Message{
            Key: []byte(row.EventID), Value: append([]byte(nil), row.Payload...),
        }); err != nil { t.Fatalf("Kafka write failed (ACK unproven): %v", err) }
    }

    // Kafka ACK was received; simulate Relay crash before its SQL update.
    publish(rowA)
    var published bool
    if err := db.QueryRowContext(ctx,
        "SELECT published_at IS NOT NULL FROM event_outbox WHERE outbox_id=$1",
        rowA.ID).Scan(&published); err != nil { t.Fatal(err) }
    if published { t.Fatal("row became published without MarkPublished; crash window not exercised") }

    // Advance *only this test lease* using SQL, avoiding a minute-long sleep.
    // No production lease duration, clock, or retry policy is changed.
    if _, err := db.ExecContext(ctx,
        "UPDATE event_outbox SET lease_until=now()-interval '1 second' WHERE outbox_id=$1",
        rowA.ID,
    ); err != nil { t.Fatal(err) }
    second, err := store.Claim(ctx, "m5b-worker-b", 60*time.Second, 1)
    if err != nil || len(second) != 1 { t.Fatalf("replacement claim count=%d err=%v", len(second), err) }
    rowB := second[0]
    if rowB.ID != rowA.ID || rowB.EventID != rowA.EventID || rowB.Attempts != rowA.Attempts+1 {
        t.Fatalf("fencing generation was not advanced: old=%+v new=%+v", rowA, rowB)
    }
    if err := store.MarkPublished(ctx, rowA, "m5b-worker-a"); !errors.Is(err, ErrLeaseLost) {
        t.Fatalf("old worker was not fenced: %v", err)
    }

    // Duplicate Kafka publication is expected. The replacement, not the
    // expired lease holder, may then mark the Outbox row delivered.
    publish(rowB)
    if err := store.MarkPublished(ctx, rowB, "m5b-worker-b"); err != nil { t.Fatal(err) }
    if err := store.MarkPublished(ctx, rowB, "m5b-worker-b"); !errors.Is(err, ErrLeaseLost) {
        t.Fatalf("published row allowed a repeated acknowledgement: %v", err)
    }

    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{broker}, Topic: topic, Partition: 0,
        StartOffset: kafka.FirstOffset,
        MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 250 * time.Millisecond,
    })
    defer reader.Close()
    for i := 0; i < 2; i++ {
        message, err := reader.FetchMessage(ctx)
        if err != nil { t.Fatalf("read physical Kafka copy %d: %v", i+1, err) }
        if !bytes.Equal(message.Value, rowA.Payload) {
            t.Fatalf("Kafka copy %d has changed payload", i+1)
        }
        var decoded event.UsageEvent
        if err := json.Unmarshal(message.Value, &decoded); err != nil { t.Fatal(err) }
        if err := decoded.Validate(); err != nil || decoded.EventID != usage.EventID {
            t.Fatalf("invalid Kafka copy %d: event_id=%s validation=%v", i+1, decoded.EventID, err)
        }
    }

    var ledgerRows, outboxRows, attempts int
    if err := db.QueryRowContext(ctx,
        "SELECT count(*) FROM usage_ledger WHERE event_id=$1::uuid",
        usage.EventID).Scan(&ledgerRows); err != nil { t.Fatal(err) }
    if err := db.QueryRowContext(ctx,
        "SELECT count(*) FROM event_outbox WHERE event_id=$1::uuid",
        usage.EventID).Scan(&outboxRows); err != nil { t.Fatal(err) }
    if err := db.QueryRowContext(ctx,
        "SELECT attempts,published_at IS NOT NULL FROM event_outbox WHERE event_id=$1::uuid",
        usage.EventID).Scan(&attempts, &published); err != nil { t.Fatal(err) }
    if ledgerRows != 1 || outboxRows != 1 || attempts != 2 || !published {
        t.Fatalf("ledger=%d outbox=%d attempts=%d published=%t", ledgerRows, outboxRows, attempts, published)
    }

    evidence := struct {
        SchemaVersion int    `json:"schema_version"`
        Verdict       string `json:"verdict"`
        Scenario      string `json:"scenario"`
        LedgerRows    int    `json:"ledger_rows"`
        OutboxRows    int    `json:"outbox_rows"`
        KafkaCopies   int    `json:"kafka_physical_copies"`
        LeaseAttempts int    `json:"lease_attempts"`
        StaleFenced   bool   `json:"stale_worker_fenced"`
        Published     bool   `json:"outbox_published"`
        EventID       string `json:"event_id"`
        GithubSHA     string `json:"github_sha,omitempty"`
    }{
        SchemaVersion: 1, Verdict: "PASS",
        Scenario: "Kafka ACK received, Relay crash before Outbox MarkPublished",
        LedgerRows: ledgerRows, OutboxRows: outboxRows,
        KafkaCopies: 2, LeaseAttempts: attempts,
        StaleFenced: true, Published: published,
        EventID: usage.EventID, GithubSHA: os.Getenv("GITHUB_SHA"),
    }
    if path := os.Getenv("M5B_EVIDENCE_PATH"); path != "" {
        if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil { t.Fatal(err) }
        data, err := json.MarshalIndent(evidence, "", "  ")
        if err != nil { t.Fatal(err) }
        if err := os.WriteFile(path, append(data, '\n'), 0644); err != nil { t.Fatal(err) }
    }
    t.Logf("PASS: Kafka physical copies=2, PostgreSQL Ledger=1 Outbox=1, stale owner fenced, replacement published")
}
