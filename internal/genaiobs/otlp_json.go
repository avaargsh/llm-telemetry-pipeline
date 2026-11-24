package genaiobs

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"
)

const MaxOTLPJSONBytes = 256 * 1024

var (
	ErrInvalidOTLPJSON = errors.New("invalid GenAI OTLP JSON batch")
	ErrOTLPJSONLimit   = errors.New("GenAI OTLP JSON batch exceeds limit")
)

type otlpJSONSpan struct {
	TraceID string `json:"traceId"`
	SpanID  string `json:"spanId"`
	Status  struct {
		Code int `json:"code"`
	} `json:"status"`
	Attributes []struct {
		Key   string                     `json:"key"`
		Value map[string]json.RawMessage `json:"value"`
	} `json:"attributes"`
}

// DecodeOTLPJSON projects a bounded, prefiltered GenAI OTLP/HTTP JSON batch.
// It is not a general OTLP decoder, HTTP handler, authenticator or ACK boundary.
// No tenant identity is read from the wire. The caller must supply trusted scope
// when admitting the returned spans through SubmitBatch.
func DecodeOTLPJSON(body []byte) ([]Span, error) {
	if len(body) > MaxOTLPJSONBytes {
		return nil, ErrOTLPJSONLimit
	}
	if !utf8.Valid(body) {
		return nil, ErrInvalidOTLPJSON
	}
	var request struct {
		ResourceSpans []struct {
			ScopeSpans []struct {
				Spans []otlpJSONSpan `json:"spans"`
			} `json:"scopeSpans"`
		} `json:"resourceSpans"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, ErrInvalidOTLPJSON
	}
	var spans []Span
	for _, resource := range request.ResourceSpans {
		for _, scope := range resource.ScopeSpans {
			for _, wire := range scope.Spans {
				if len(spans) == MaxBatchSize || len(wire.Attributes) > 64 {
					return nil, ErrOTLPJSONLimit
				}
				span := Span{TraceID: strings.ToLower(wire.TraceID), SpanID: strings.ToLower(wire.SpanID)}
				switch wire.Status.Code {
				case 0:
					span.Status = "unset"
				case 1:
					span.Status = "ok"
				case 2:
					span.Status = "error"
				default:
					return nil, ErrInvalidOTLPJSON
				}
				for _, a := range wire.Attributes {
					if len(a.Key) == 0 || len(a.Key) > 128 {
						return nil, ErrInvalidOTLPJSON
					}
					var value any
					switch a.Key {
					case "gen_ai.provider.name", "gen_ai.operation.name", "gen_ai.request.model", "gen_ai.response.model":
						raw := a.Value["stringValue"]
						var v string
						if len(a.Value) != 1 || len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &v) != nil {
							return nil, ErrInvalidOTLPJSON
						}
						value = v
					case "gen_ai.usage.input_tokens", "gen_ai.usage.output_tokens":
						raw := a.Value["intValue"]
						if len(a.Value) != 1 || len(raw) == 0 {
							return nil, ErrInvalidOTLPJSON
						}
						decimal := string(raw)
						if raw[0] == '"' && json.Unmarshal(raw, &decimal) != nil {
							return nil, ErrInvalidOTLPJSON
						}
						v, err := strconv.ParseInt(decimal, 10, 64)
						if err != nil {
							return nil, ErrInvalidOTLPJSON
						}
						value = v
					default:
						continue
					}
					span.Attributes = append(span.Attributes, Attribute{Key: a.Key, Value: value})
				}
				// Validate the projection using a fixed internal placeholder, not a wire
				// identity. Actual trusted scope is validated again at queue admission.
				if _, err := NormalizeInference(Scope{TenantID: "wire-validation"}, Version, span); err != nil {
					return nil, ErrInvalidOTLPJSON
				}
				spans = append(spans, span)
			}
		}
	}
	if len(spans) == 0 {
		return nil, ErrInvalidOTLPJSON
	}
	return spans, nil
}
