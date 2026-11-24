package genaiobs

import (
	"errors"
	"strings"
	"testing"
)

const strictWire = `{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"0123456789abcdef0123456789abcdef","spanId":"0123456789abcdef","status":{"code":1},"attributes":[{"key":"gen_ai.provider.name","value":{"stringValue":"openai"}},{"key":"gen_ai.operation.name","value":{"stringValue":"chat"}}]}]}]}]}`

func TestOTLPJSONAmbiguousFields(t *testing.T) {
	for _, tc := range []struct{ name, old, replacement string }{
		{"root duplicate", `"resourceSpans":`, `"resourceSpans":[],"resourceSpans":`},
		{"root case", `"resourceSpans":`, `"ResourceSpans":`},
		{"span id duplicate", `"spanId":`, `"spanId":"1111111111111111","spanId":`},
		{"escaped duplicate", `"spanId":`, `"span\u0049d":"1111111111111111","spanId":`},
		{"span case", `"spanId":`, `"SPANID":`},
		{"status duplicate", `"code":1`, `"code":2,"code":1`},
		{"status case", `"code":1`, `"Code":1`},
		{"attribute duplicate", `"key":"gen_ai.provider.name"`, `"key":"tenant.id","key":"gen_ai.provider.name"`},
		{"anyvalue duplicate", `"stringValue":"openai"`, `"stringValue":"other","stringValue":"openai"`},
		{"ignored object duplicate", `"status":`, `"extra":{"x":1,"x":2},"status":`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.Replace(strictWire, tc.old, tc.replacement, 1)
			if got, err := DecodeOTLPJSON([]byte(body)); got != nil || !errors.Is(err, ErrInvalidOTLPJSON) {
				t.Fatalf("ambiguous input accepted: %v", err)
			}
		})
	}
}

func TestOTLPJSONDepthAndUnknownFields(t *testing.T) {
	body := strings.Replace(strictWire, `"status":`, `"extra":{"Code":1,"code":2,"n":1e9999},"status":`, 1)
	if got, err := DecodeOTLPJSON([]byte(body)); err != nil || len(got) != 1 {
		t.Fatalf("unknown metadata rejected: %v", err)
	}
	body = strings.Replace(strictWire, `"status":`, `"extra":`+strings.Repeat("[", 70)+"0"+strings.Repeat("]", 70)+`,"status":`, 1)
	if got, err := DecodeOTLPJSON([]byte(body)); got != nil || !errors.Is(err, ErrOTLPJSONLimit) {
		t.Fatalf("deep input accepted: %v", err)
	}
}
