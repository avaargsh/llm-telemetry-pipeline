package genaiobs

import (
	"errors"
	"strings"
	"testing"
)

const wireSpan = `{"traceId":"0123456789ABCDEF0123456789ABCDEF","spanId":"0123456789ABCDEF","status":{"code":1},"attributes":[{"key":"gen_ai.provider.name","value":{"stringValue":"openai"}},{"key":"gen_ai.operation.name","value":{"stringValue":"chat"}},{"key":"gen_ai.usage.input_tokens","value":{"intValue":"9007199254740993"}},{"key":"gen_ai.usage.output_tokens","value":{"intValue":0}},{"key":"tenant.id","value":{"stringValue":"spoof"}},{"key":"gen_ai.input.messages","value":{"arrayValue":{"values":[{"stringValue":"secret"}]}}}]}`

func wireBody(spans string) []byte {
	return []byte(`{"resourceSpans":[{"resource":{"attributes":[]},"scopeSpans":[{"scope":{"name":"test"},"spans":[` + spans + `]}]}]}`)
}

func TestOTLPJSONProjection(t *testing.T) {
	spans, err := DecodeOTLPJSON(wireBody(wireSpan + "," + wireSpan))
	if err != nil || len(spans) != 2 {
		t.Fatalf("decode: %v", err)
	}
	q, _ := NewQueue(2)
	if err := q.SubmitBatch(Scope{TenantID: "trusted"}, Version, spans); err != nil {
		t.Fatal(err)
	}
	o, ok := q.Pop()
	if !ok || o.TenantID != "trusted" || o.TraceID != "0123456789abcdef0123456789abcdef" || o.SpanID != "0123456789abcdef" || o.Status != "ok" || o.InputTokens == nil || *o.InputTokens != 9007199254740993 || o.OutputTokens == nil || *o.OutputTokens != 0 || len(spans[0].Attributes) != 4 {
		t.Fatalf("unexpected projection: %+v", o)
	}
}

func TestOTLPJSONRejectsWholeMalformedBatch(t *testing.T) {
	for _, bad := range []string{
		strings.Replace(wireSpan, `"code":1`, `"code":"STATUS_CODE_OK"`, 1),
		strings.Replace(wireSpan, `"code":1`, `"code":99`, 1),
		strings.Replace(wireSpan, `"intValue":"9007199254740993"`, `"intValue":"9223372036854775808"`, 1),
		strings.Replace(wireSpan, `"intValue":"9007199254740993"`, `"stringValue":"1"`, 1),
		strings.Replace(wireSpan, `"stringValue":"openai"`, `"stringValue":"openai","intValue":"2"`, 1),
		strings.Replace(wireSpan, `"0123456789ABCDEF"`, `"0000000000000000"`, 1),
		strings.Replace(wireSpan, `"chat"`, `"not-inference"`, 1),
	} {
		if got, err := DecodeOTLPJSON(wireBody(wireSpan + "," + bad)); !errors.Is(err, ErrInvalidOTLPJSON) || got != nil {
			t.Fatalf("accepted malformed batch: %v", err)
		}
	}
	for _, body := range [][]byte{[]byte(`{}`), []byte(`null`), []byte(`{`), append(wireBody(wireSpan), []byte(`{}`)...)} {
		if got, err := DecodeOTLPJSON(body); !errors.Is(err, ErrInvalidOTLPJSON) || got != nil {
			t.Fatalf("accepted invalid envelope: %v", err)
		}
	}
}

func TestOTLPJSONLimits(t *testing.T) {
	for _, body := range [][]byte{make([]byte, MaxOTLPJSONBytes+1), wireBody(strings.Repeat(wireSpan+",", MaxBatchSize) + wireSpan)} {
		if got, err := DecodeOTLPJSON(body); !errors.Is(err, ErrOTLPJSONLimit) || got != nil {
			t.Fatalf("limit: %v", err)
		}
	}
}

func TestOTLPJSONRejectsDuplicateObjectMembers(t *testing.T) {
    cases := map[string]string{
        "duplicate traceId": strings.Replace(wireSpan, `"traceId":"0123456789ABCDEF0123456789ABCDEF"`, `"traceId":"0123456789ABCDEF0123456789ABCDEF","traceId":"1123456789ABCDEF0123456789ABCDEF"`, 1),
        "duplicate status code": strings.Replace(wireSpan, `"code":1`, `"code":1,"code":2`, 1),
        "duplicate attribute value": strings.Replace(wireSpan, `"stringValue":"openai"`, `"stringValue":"openai","stringValue":"other"`, 1),
        "duplicate envelope member": strings.Replace(string(wireBody(wireSpan)), `"resourceSpans":[`, `"resourceSpans":[],"resourceSpans":[`, 1),
        "case-folded known field collision": strings.Replace(wireSpan, `"traceId":"0123456789ABCDEF0123456789ABCDEF"`, `"traceId":"0123456789ABCDEF0123456789ABCDEF","TraceId":"1123456789ABCDEF0123456789ABCDEF"`, 1),
    }
    for name, body := range cases {
        t.Run(name, func(t *testing.T) {
            if name == "duplicate envelope member" {
                if got, err := DecodeOTLPJSON([]byte(body)); !errors.Is(err, ErrInvalidOTLPJSON) || got != nil {
                    t.Fatalf("accepted ambiguous envelope: %v", err)
                }
                return
            }
            if got, err := DecodeOTLPJSON(wireBody(body)); !errors.Is(err, ErrInvalidOTLPJSON) || got != nil {
                t.Fatalf("accepted ambiguous span: %v", err)
            }
        })
    }
}
