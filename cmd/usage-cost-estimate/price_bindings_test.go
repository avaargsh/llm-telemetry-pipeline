package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPriceVersionBindingsOffline(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0600); err != nil { t.Fatal(err) }
		return p
	}
	usage := write("usage.json", `[]`)
	prices := write("prices.json", `{"version":"example-v1","currency":"USD","rates":[{"model":"Qwen3-32B","input_per_million":"2.500000","output_per_million":"10.0"}]}`)
	const fp = "sha256:433d2e0b10d8481699f2a2353f26bef71f057dd2cb08dc7069c026c0d7cc3cd5"
	const other = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	for _, tc := range []struct{name, bindings, want string}{
		{"matches", `[{"version":"example-v1","price_fingerprint":"`+fp+`"}]`, ""},
		{"mismatch", `[{"version":"example-v1","price_fingerprint":"`+other+`"}]`, "price version binding mismatch"},
		{"missing", `[{"version":"another","price_fingerprint":"`+fp+`"}]`, "missing price version binding"},
		{"duplicate", `[{"version":"example-v1","price_fingerprint":"`+fp+`"},{"version":"example-v1","price_fingerprint":"`+fp+`"}]`, "invalid price bindings"},
		{"null", `null`, "invalid price bindings"},
		{"empty", `[]`, "missing price version binding"},
		{"malformed digest", `[{"version":"example-v1","price_fingerprint":"sha256:ABC"}]`, "invalid price bindings"},
		{"invalid unused entry", `[{"version":"example-v1","price_fingerprint":"`+fp+`"},{"version":"unused","price_fingerprint":"bad"}]`, "invalid price bindings"},
		{"unknown field", `[{"version":"example-v1","price_fingerprint":"`+fp+`","other":true}]`, "invalid price bindings"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			err := run([]string{"--usage", usage, "--prices", prices, "--price-bindings", write("bindings.json",tc.bindings)}, &out)
			if tc.want == "" {
				if err != nil || !strings.Contains(out.String(),fp) { t.Fatalf("valid binding: err=%v out=%s",err,out.String()) }
			} else if err == nil || err.Error()!=tc.want || out.Len()!=0 {
				t.Fatalf("expected %q and no output: err=%v out=%s",tc.want,err,out.String())
			}
		})
	}
}

func TestHistoricalPriceBindingsCheckAllPeriodsBeforeDatabase(t *testing.T) {
	t.Setenv("USAGE_QUERY_DATABASE_URL","")
	dir := t.TempDir()
	schedule := filepath.Join(dir,"schedule.json")
	bindings := filepath.Join(dir,"bindings.json")
	const fp = "sha256:433d2e0b10d8481699f2a2353f26bef71f057dd2cb08dc7069c026c0d7cc3cd5"
	const table = `{"currency":"USD","rates":[{"model":"Qwen3-32B","input_per_million":"2.5","output_per_million":"10"}],`
	data := `[{"from":"2026-11-21T00:00:00Z","to":"2026-11-21T01:00:00Z","table":`+table+`"version":"v1"}},{"from":"2026-11-21T01:00:00Z","to":"2026-11-21T02:00:00Z","table":`+table+`"version":"v2"}}]`
	if err := os.WriteFile(schedule,[]byte(data),0600); err != nil { t.Fatal(err) }
	for _,tc := range []struct{data,want string}{
		{`[{"version":"v1","price_fingerprint":"`+fp+`"}]`,"missing price version binding"},
		{`[{"version":"v1","price_fingerprint":"`+fp+`"},{"version":"v2","price_fingerprint":"sha256:0000000000000000000000000000000000000000000000000000000000000000"}]`,"price version binding mismatch"},
		{`[{"version":"v1","price_fingerprint":"`+fp+`"},{"version":"v2","price_fingerprint":"`+fp+`"}]`,"USAGE_QUERY_DATABASE_URL is required"},
	} {
		if err := os.WriteFile(bindings,[]byte(tc.data),0600);err!=nil{t.Fatal(err)}
		var out bytes.Buffer
		err := run([]string{"--schedule",schedule,"--from","2026-11-21T00:00:00Z","--to","2026-11-21T01:00:00Z","--price-bindings",bindings},&out)
		if err==nil || err.Error()!=tc.want || out.Len()!=0{t.Fatalf("want %q got %v out=%s",tc.want,err,out.String())}
	}
}
