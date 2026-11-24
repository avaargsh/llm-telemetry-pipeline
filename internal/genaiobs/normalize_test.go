package genaiobs

import (
	"encoding/json"
	"strings"
	"testing"
)

func fixture() Span {
	return Span{TraceID: "0123456789abcdef0123456789abcdef", SpanID: "0123456789abcdef", RequestID: "req-1", AttemptID: "attempt-1", Status: "ok", Attributes: []Attribute{{"gen_ai.provider.name", "openai"}, {"gen_ai.operation.name", "chat"}, {"gen_ai.request.model", "gpt-4"}, {"gen_ai.usage.input_tokens", int64(0)}}}
}

func TestNormalize(t *testing.T) {
	s := fixture()
	s.Attributes = append(s.Attributes, Attribute{"tenant.id", "evil"}, Attribute{"gen_ai.input.messages", "SECRET"}, Attribute{"authorization", "SECRET"})
	o, e := NormalizeInference(Scope{TenantID: "tenant-a"}, Version, s)
	if e != nil || o.TenantID != "tenant-a" || o.TraceID != s.TraceID || o.SpanID != s.SpanID || o.RequestID != "req-1" || o.AttemptID != "attempt-1" || o.Provider != "openai" || o.Operation != "chat" || o.RequestModel != "gpt-4" || o.InputTokens == nil || *o.InputTokens != 0 || o.OutputTokens != nil {
		t.Fatalf("bad projection: %+v %v", o, e)
	}
	raw, _ := json.Marshal(o)
	if strings.Contains(string(raw), "SECRET") || strings.Contains(string(raw), "evil") || !strings.Contains(string(raw), `"input_tokens":0`) || strings.Contains(string(raw), `"output_tokens"`) {
		t.Fatal(string(raw))
	}
	b, e := NormalizeInference(Scope{TenantID: "tenant-b"}, Version, s)
	if e != nil || b.TenantID != "tenant-b" {
		t.Fatal(b, e)
	}
	*o.InputTokens = 99
	if *b.InputTokens != 0 {
		t.Fatal("shared token state")
	}
}

func TestReject(t *testing.T) {
	cases := map[string]func(*Span){
		"zero trace": func(s *Span) { s.TraceID = strings.Repeat("0", 32) }, "short span": func(s *Span) { s.SpanID = "a" }, "uppercase": func(s *Span) { s.SpanID = "ABCDEF0123456789" },
		"duplicate": func(s *Span) { s.Attributes = append(s.Attributes, Attribute{"gen_ai.provider.name", "other"}) }, "negative": func(s *Span) { s.Attributes[3].Value = int64(-1) }, "overflow": func(s *Span) { s.Attributes[3].Value = uint64(1 << 63) }, "float": func(s *Span) { s.Attributes[3].Value = 1.5 },
		"missing provider": func(s *Span) { s.Attributes = s.Attributes[1:] }, "invalid operation": func(s *Span) { s.Attributes[1].Value = "execute_tool" }, "oversize": func(s *Span) { s.Attributes = make([]Attribute, 65) }, "secret model": func(s *Span) { s.Attributes[2].Value = "secret prompt with spaces" }, "status": func(s *Span) { s.Status = "secret" }, "invalid attempt": func(s *Span) { s.AttemptID = "secret prompt" },
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			s := fixture()
			edit(&s)
			o, e := NormalizeInference(Scope{TenantID: "tenant-a"}, Version, s)
			if e != ErrInvalid || o.TenantID != "" {
				t.Fatalf("accepted or leaked invalid input: %+v %v", o, e)
			}
		})
	}
	if _, e := NormalizeInference(Scope{}, Version, fixture()); e != ErrInvalid {
		t.Fatal(e)
	}
	if _, e := NormalizeInference(Scope{TenantID: "tenant-a"}, "latest", fixture()); e != ErrUnsupportedVersion {
		t.Fatal(e)
	}
}
