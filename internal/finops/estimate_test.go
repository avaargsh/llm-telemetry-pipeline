package finops

import (
	"errors"
	"testing"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
)

func usage(model, prompt, completion, total string) ledger.ModelUsage {
	return ledger.ModelUsage{Model: model, UsageSummary: ledger.UsageSummary{
		RequestCount: 1, PromptTokens: prompt, CompletionTokens: completion, TotalTokens: total,
	}}
}

func TestEstimateExactCosts(t *testing.T) {
	table := PriceTable{Version: "example-v1", Currency: "USD", Rates: []ModelRate{
		{Model: "a", InputPerMillion: "2.5", OutputPerMillion: "10"},
		{Model: "b", InputPerMillion: "0.000001", OutputPerMillion: "0"},
	}}
	got, err := Estimate([]ledger.ModelUsage{usage("a","408","58","466"),usage("b","1","0","1")}, table)
	if err != nil { t.Fatal(err) }
	if got.Currency != "USD" || got.PriceVersion != "example-v1" || got.TotalCost != "0.001600000001" ||
		len(got.Models) != 2 || got.Models[0].InputCost != "0.001020000000" ||
		got.Models[0].OutputCost != "0.000580000000" || got.Models[1].TotalCost != "0.000000000001" {
		t.Fatalf("unexpected estimate: %+v", got)
	}
}

func TestEstimateBeyondUint64(t *testing.T) {
	table := PriceTable{Version:"v1",Currency:"USD",Rates:[]ModelRate{{Model:"a",InputPerMillion:"1",OutputPerMillion:"0"}}}
	got,err := Estimate([]ledger.ModelUsage{usage("a","18446744073709551616","0","18446744073709551616")},table)
	if err != nil || got.TotalCost != "18446744073709.551616000000" { t.Fatalf("estimate=%+v err=%v",got,err) }
}

func TestEstimateRejectsIncompleteOrAmbiguousInputs(t *testing.T) {
	valid := ModelRate{Model:"a",InputPerMillion:"1",OutputPerMillion:"0"}
	for _,tc := range []struct{
		name string
		models []ledger.ModelUsage
		rates []ModelRate
		currency string
		want error
	}{
		{"missing price",[]ledger.ModelUsage{usage("b","1","0","1")},[]ModelRate{valid},"USD",ErrMissingRate},
		{"duplicate usage",[]ledger.ModelUsage{usage("a","1","0","1"),usage("a","1","0","1")},[]ModelRate{valid},"USD",ErrInvalidEstimate},
		{"duplicate rate",nil,[]ModelRate{valid,valid},"USD",ErrInvalidEstimate},
		{"mixed total",[]ledger.ModelUsage{usage("a","1","0","2")},[]ModelRate{valid},"USD",ErrInvalidEstimate},
		{"negative tokens",[]ledger.ModelUsage{usage("a","-1","2","1")},[]ModelRate{valid},"USD",ErrInvalidEstimate},
		{"missing output price",nil,[]ModelRate{{Model:"a",InputPerMillion:"1"}},"USD",ErrInvalidEstimate},
		{"excess rate precision",nil,[]ModelRate{{Model:"a",InputPerMillion:"0.0000001",OutputPerMillion:"0"}},"USD",ErrInvalidEstimate},
		{"exponent",nil,[]ModelRate{{Model:"a",InputPerMillion:"1e2",OutputPerMillion:"0"}},"USD",ErrInvalidEstimate},
		{"unsupported currency",nil,[]ModelRate{valid},"CNY",ErrInvalidEstimate},
	} {
		t.Run(tc.name,func(t *testing.T){
			got,err := Estimate(tc.models,PriceTable{Version:"v1",Currency:tc.currency,Rates:tc.rates})
			if !errors.Is(err,tc.want) || got.TotalCost != "" || got.Models != nil { t.Fatalf("estimate=%+v err=%v",got,err) }
		})
	}
}

func TestEstimateEmptyAndExplicitZeroPrice(t *testing.T) {
	table := PriceTable{Version:"v1",Currency:"USD",Rates:[]ModelRate{{Model:"a",InputPerMillion:"0",OutputPerMillion:"0"}}}
	got,err := Estimate(nil,table)
	if err != nil || got.TotalCost != "0.000000000000" || got.Models == nil || len(got.Models) != 0 { t.Fatalf("empty: %+v %v",got,err) }
	got,err = Estimate([]ledger.ModelUsage{usage("a","1","2","3")},table)
	if err != nil || got.TotalCost != "0.000000000000" || len(got.Models) != 1 { t.Fatalf("explicit zero: %+v %v",got,err) }
}
