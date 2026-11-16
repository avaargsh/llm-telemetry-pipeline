package event

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// UsageEvent is immutable once recorded. A repeated event_id must have the same payload_hash.
// Token charging is NOT implemented here; the authoritative ledger enforces uniqueness.
type UsageEvent struct {
	EventID          string    `json:"event_id"`
	EventType        string    `json:"event_type"`
	SchemaVersion    int       `json:"schema_version"`
	TenantID         string    `json:"tenant_id"`
	ProjectID        string    `json:"project_id"`
	RequestID        string    `json:"request_id"`
	Model            string    `json:"model"`
	PromptTokens     int64     `json:"prompt_tokens"`
	CompletionTokens int64     `json:"completion_tokens"`
	LatencyMS        int64     `json:"latency_ms"`
	Status           string    `json:"status"`
	Timestamp        time.Time `json:"timestamp"`
	PayloadHash      string    `json:"payload_hash"`
}

func (e UsageEvent) validateFields() error {
	if !uuidPattern.MatchString(e.EventID) {
		return errors.New("invalid event_id UUID")
	}
	if e.SchemaVersion != 1 || e.EventType != "usage.finalized" {
		return errors.New("unsupported event schema or type")
	}
	if strings.TrimSpace(e.TenantID) == "" || strings.TrimSpace(e.ProjectID) == "" || strings.TrimSpace(e.RequestID) == "" || strings.TrimSpace(e.Model) == "" {
		return errors.New("tenant_id, project_id, request_id and model are required")
	}
	if e.PromptTokens < 0 || e.CompletionTokens < 0 || e.LatencyMS < 0 {
		return errors.New("usage and latency cannot be negative")
	}
	if e.Status != "success" && e.Status != "error" {
		return errors.New("invalid status")
	}
	if e.Timestamp.IsZero() {
		return errors.New("timestamp required")
	}
	return nil
}

// Digest covers the full canonical JSON of event fields except payload_hash itself.
func (e UsageEvent) Digest() (string, error) {
	if err := e.validateFields(); err != nil {
		return "", err
	}
	e.PayloadHash = ""
	data, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func (e *UsageEvent) Seal() error {
	h, err := e.Digest()
	if err != nil {
		return err
	}
	e.PayloadHash = h
	return nil
}

func (e UsageEvent) Validate() error {
	h, err := e.Digest()
	if err != nil {
		return err
	}
	if e.PayloadHash != h {
		return errors.New("payload_hash mismatch")
	}
	return nil
}

// SyntheticUsageEvent returns the same record for the same logical seed/index/start.
// This enables deterministic replay and duplicate-insertion experiments.
func SyntheticUsageEvent(seed string, index int, start time.Time) (UsageEvent, error) {
	if seed == "" || index < 0 || start.IsZero() {
		return UsageEvent{}, errors.New("seed, non-negative index and start are required")
	}
	start = start.UTC()
	input := fmt.Sprintf("%s|%s|%d", seed, start.Format(time.RFC3339Nano), index)
	sum := sha256.Sum256([]byte(input))
	b := sum[:16]
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	eventID := fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
	e := UsageEvent{
		EventID: eventID, EventType: "usage.finalized", SchemaVersion: 1,
		TenantID:  fmt.Sprintf("tenant_%03d", index%4+1),
		ProjectID: fmt.Sprintf("project_%03d", index%8+1),
		RequestID: fmt.Sprintf("%s_req_%09d", seed, index),
		Model:     "Qwen3-32B", PromptTokens: int64(200 + index%1024),
		CompletionTokens: int64(25 + index%512), LatencyMS: int64(100 + index%3000),
		Status: "success", Timestamp: start.Add(time.Duration(index) * 10 * time.Millisecond),
	}
	if index%20 == 19 {
		e.Status = "error"
	}
	if err := e.Seal(); err != nil {
		return UsageEvent{}, err
	}
	return e, nil
}
