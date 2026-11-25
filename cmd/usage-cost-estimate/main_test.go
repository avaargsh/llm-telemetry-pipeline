package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/finops"
)

func TestRunEstimateFromFiles(t *testing.T) {
	dir := t.TempDir()
	usage := filepath.Join(dir,"usage.json")
	prices := filepath.Join(dir,"prices.json")
	if err := os.WriteFile(usage,[]byte(`[{"model":"Qwen3-32B","request_count":2,"prompt_tokens":"408","completion_tokens":"58","total_tokens":"466"}]`),0600); err != nil { t.Fatal(err) }
	if err := os.WriteFile(prices,[]byte(`{"version":"example-v1","currency":"USD","rates":[{"model":"Qwen3-32B","input_per_million":"2.5","output_per_million":"10"}]}`),0600); err != nil { t.Fatal(err) }
	var out bytes.Buffer
	if err := run([]string{"--usage",usage,"--prices",prices},&out); err != nil { t.Fatal(err) }
	var got finops.CostEstimate
	if err := json.Unmarshal(out.Bytes(),&got); err != nil { t.Fatal(err) }
	if got.PriceVersion != "example-v1" || got.TotalCost != "0.001600000000" || len(got.Models) != 1 { t.Fatalf("unexpected output: %+v",got) }
}

func TestRunRejectsInvalidUsageWithoutOutput(t *testing.T) {
	dir := t.TempDir()
	usage := filepath.Join(dir,"usage.json")
	prices := filepath.Join(dir,"prices.json")
	if err := os.WriteFile(prices,[]byte(`{"version":"v1","currency":"USD","rates":[]}`),0600); err != nil { t.Fatal(err) }
	for _,data := range []string{`null`,`[] []`,`[{"model":"x","unknown":true}]`,`[{"model":"x","model":"y","request_count":1,"prompt_tokens":"1","completion_tokens":"0","total_tokens":"1"}]`,`[{"model":"x","request_count":1,"prompt_tokens":"1","completion_tokens":"0","total_tokens":"1"}]`} {
		if err := os.WriteFile(usage,[]byte(data),0600); err != nil { t.Fatal(err) }
		var out bytes.Buffer
		if err := run([]string{"--usage",usage,"--prices",prices},&out); err == nil || out.Len() != 0 { t.Fatalf("expected no estimate: %q err=%v out=%s",data,err,out.String()) }
	}
}

func TestRunRejectsDuplicatePriceKeys(t *testing.T) {
	dir := t.TempDir()
	usage := filepath.Join(dir,"usage.json")
	prices := filepath.Join(dir,"prices.json")
	if err := os.WriteFile(usage,[]byte(`[]`),0600); err != nil { t.Fatal(err) }
	if err := os.WriteFile(prices,[]byte(`{"version":"v1","currency":"USD","rates":[{"model":"a","input_per_million":"1","input_per_million":"0","output_per_million":"0"}]}`),0600); err != nil { t.Fatal(err) }
	var out bytes.Buffer
	if err := run([]string{"--usage",usage,"--prices",prices},&out); err == nil || out.Len() != 0 { t.Fatal("duplicate rate key was accepted") }
}
