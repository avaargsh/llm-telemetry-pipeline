// Package genaiobs projects bounded, non-authoritative inference metadata.
package genaiobs

import (
	"errors"
	"strings"
)

const Version = "genai-semconv/v0.1.0"

var (
	ErrInvalid            = errors.New("invalid inference observation")
	ErrUnsupportedVersion = errors.New("unsupported GenAI convention version")
)

// Scope must be supplied by authenticated ingress, never from span attributes.
type Scope struct{ TenantID string }

type Attribute struct {
	Key   string
	Value any
}

// Span is a local typed envelope, not OTLP wire format. Request/attempt IDs
// are optional caller-provided correlation metadata, not billing identities.
type Span struct {
	TraceID, SpanID, RequestID, AttemptID, Status string
	Attributes                                    []Attribute
}

type Observation struct {
	SchemaVersion string `json:"schema_version"`
	TenantID      string `json:"tenant_id"`
	TraceID       string `json:"trace_id"`
	SpanID        string `json:"span_id"`
	RequestID     string `json:"request_id,omitempty"`
	AttemptID     string `json:"attempt_id,omitempty"`
	Provider      string `json:"provider"`
	Operation     string `json:"operation"`
	RequestModel  string `json:"request_model,omitempty"`
	ResponseModel string `json:"response_model,omitempty"`
	Status        string `json:"status"`
	InputTokens   *int64 `json:"input_tokens,omitempty"`
	OutputTokens  *int64 `json:"output_tokens,omitempty"`
}

func identifier(s string, max int) bool {
	if len(s) == 0 || len(s) > max {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("._-:/@", c)) {
			return false
		}
	}
	return true
}
func hexID(s string, n int) bool {
	if len(s) != n || s == strings.Repeat("0", n) {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func optionalID(s string, max int) bool { return s == "" || identifier(s, max) }

// NormalizeInference is a pure projection. It does not authenticate, persist,
// deduplicate, finalize usage, or issue charges. Rejections contain no raw input.
func NormalizeInference(scope Scope, version string, span Span) (Observation, error) {
	if version != Version {
		return Observation{}, ErrUnsupportedVersion
	}
	if !identifier(scope.TenantID, 128) || !hexID(span.TraceID, 32) || !hexID(span.SpanID, 16) || !optionalID(span.RequestID, 128) || !optionalID(span.AttemptID, 128) || len(span.Attributes) > 64 {
		return Observation{}, ErrInvalid
	}
	status := span.Status
	if status == "" {
		status = "unset"
	}
	if status != "unset" && status != "ok" && status != "error" {
		return Observation{}, ErrInvalid
	}
	out := Observation{SchemaVersion: Version, TenantID: scope.TenantID, TraceID: span.TraceID, SpanID: span.SpanID, RequestID: span.RequestID, AttemptID: span.AttemptID, Status: status}
	seen := make(map[string]bool, 6)
	for _, a := range span.Attributes {
		if len(a.Key) == 0 || len(a.Key) > 128 {
			return Observation{}, ErrInvalid
		}
		var dst *string
		var tokens **int64
		switch a.Key {
		case "gen_ai.provider.name":
			dst = &out.Provider
		case "gen_ai.operation.name":
			dst = &out.Operation
		case "gen_ai.request.model":
			dst = &out.RequestModel
		case "gen_ai.response.model":
			dst = &out.ResponseModel
		case "gen_ai.usage.input_tokens":
			tokens = &out.InputTokens
		case "gen_ai.usage.output_tokens":
			tokens = &out.OutputTokens
		default:
			continue
		}
		if seen[a.Key] {
			return Observation{}, ErrInvalid
		}
		seen[a.Key] = true
		if dst != nil {
			v, ok := a.Value.(string)
			if !ok || !identifier(v, 256) {
				return Observation{}, ErrInvalid
			}
			*dst = v
		} else {
			v, ok := a.Value.(int64)
			if !ok || v < 0 {
				return Observation{}, ErrInvalid
			}
			*tokens = &v
		}
	}
	if out.Provider == "" {
		return Observation{}, ErrInvalid
	}
	switch out.Operation {
	case "chat", "generate_content", "realtime_inference", "text_completion", "embeddings":
	default:
		return Observation{}, ErrInvalid
	}
	return out, nil
}
