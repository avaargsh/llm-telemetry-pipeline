package genaiobs

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
)

// MaxOTLPJSONDepth bounds nesting even inside ignored metadata.
const MaxOTLPJSONDepth = 64

// Only fields consumed by the projection require exact canonical spelling.
// Unknown metadata remains ignored, but duplicate keys anywhere are rejected.
var otlpJSONFields = map[string]map[string]string{
	"request":   {"resourceSpans": "resource"},
	"resource":  {"scopeSpans": "scope"},
	"scope":     {"spans": "span"},
	"span":      {"traceId": "", "spanId": "", "status": "status", "attributes": "attribute"},
	"status":    {"code": ""},
	"attribute": {"key": "", "value": "value"},
	"value":     {"stringValue": "", "intValue": ""},
}

func validateOTLPJSONStructure(body []byte) error {
	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	if err := scanOTLPJSONValue(d, "request", 0); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return ErrInvalidOTLPJSON
	}
	return nil
}

func scanOTLPJSONValue(d *json.Decoder, kind string, depth int) error {
	tok, err := d.Token()
	if err != nil {
		return ErrInvalidOTLPJSON
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	if depth >= MaxOTLPJSONDepth {
		return ErrOTLPJSONLimit
	}
	var end json.Delim
	switch delim {
	case '{':
		end = '}'
		seen := make(map[string]struct{})
		for d.More() {
			keyToken, err := d.Token()
			key, ok := keyToken.(string)
			if err != nil || !ok {
				return ErrInvalidOTLPJSON
			}
			if _, exists := seen[key]; exists {
				return ErrInvalidOTLPJSON
			}
			seen[key] = struct{}{}
			fields := otlpJSONFields[kind]
			child, known := fields[key]
			if !known {
				for canonical := range fields {
					if strings.EqualFold(canonical, key) {
						return ErrInvalidOTLPJSON
					}
				}
			}
			if err := scanOTLPJSONValue(d, child, depth+1); err != nil {
				return err
			}
		}
	case '[':
		end = ']'
		for d.More() {
			if err := scanOTLPJSONValue(d, kind, depth+1); err != nil {
				return err
			}
		}
	default:
		return ErrInvalidOTLPJSON
	}
	if tok, err := d.Token(); err != nil || tok != end {
		return ErrInvalidOTLPJSON
	}
	return nil
}
