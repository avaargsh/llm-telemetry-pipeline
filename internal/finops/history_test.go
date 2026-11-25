package finops

import (
	"errors"
	"testing"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
)

func historicalFixture() (time.Time,time.Time,time.Time,[]PricePeriod) {
	start := time.Date(2026,11,21,0,0,0,0,time.UTC)
	mid := start.Add(time.Hour)
	end := mid.Add(time.Hour)
	table := func(v,rate string) PriceTable {
		return PriceTable{Version:v,Currency:"USD",Rates:[]ModelRate{{Model:"a",InputPerMillion:rate,OutputPerMillion:"0"}}}
	}
	return start,mid,end,[]PricePeriod{
		{From:start.Add(-time.Hour),To:mid,Table:table("v1","1")},
		{From:mid,To:end.Add(time.Hour),Table:table("v2","2")},
	}
}

func TestHistoricalPricesClipWindowsAtRateChange(t *testing.T) {
	start,mid,end,periods := historicalFixture()
	windows,err := PlanPriceWindows(start,end,periods)
	if err != nil || len(windows) != 2 { t.Fatalf("windows=%+v err=%v",windows,err) }
	if !windows[0].From.Equal(start) || !windows[0].To.Equal(mid) || windows[0].PriceVersion != "v1" ||
		!windows[1].From.Equal(mid) || !windows[1].To.Equal(end) || windows[1].PriceVersion != "v2" {
		t.Fatalf("incorrect half-open partition: %+v",windows)
	}
	windows,err = PlanPriceWindows(mid,end,periods)
	if err != nil || len(windows) != 1 || windows[0].PriceVersion != "v2" { t.Fatalf("boundary: %+v %v",windows,err) }
}

func TestHistoricalEstimateUsesEachWindowRateExactly(t *testing.T) {
	start,mid,end,periods := historicalFixture()
	captures := []UsageWindow{
		{From:start,To:mid,Models:[]ledger.ModelUsage{usage("a","100","0","100")}},
		{From:mid,To:end,Models:[]ledger.ModelUsage{usage("a","200","0","200")}},
	}
	got,err := EstimateHistorical(start,end,periods,captures)
	if err != nil || got.TotalCost != "0.000500000000" || got.Currency != "USD" || len(got.Windows) != 2 {
		t.Fatalf("estimate=%+v err=%v",got,err)
	}
	if got.Windows[0].Estimate.TotalCost != "0.000100000000" || got.Windows[1].Estimate.TotalCost != "0.000400000000" {
		t.Fatalf("wrong per-window estimate: %+v",got.Windows)
	}
	captures[1].Models = []ledger.ModelUsage{}
	got,err = EstimateHistorical(start,end,periods,captures)
	if err != nil || got.TotalCost != "0.000100000000" { t.Fatalf("explicit empty: %+v %v",got,err) }
}

func TestHistoricalRejectsInvalidSchedule(t *testing.T) {
	start,mid,end,_ := historicalFixture()
	for _,tc := range []struct{name string; change func([]PricePeriod); want error}{
		{"overlap",func(p []PricePeriod){p[1].From=mid.Add(-time.Minute)},ErrInvalidEstimate},
		{"gap",func(p []PricePeriod){p[1].From=mid.Add(time.Minute)},ErrPriceCoverage},
		{"duplicate version",func(p []PricePeriod){p[1].Table.Version=p[0].Table.Version},ErrInvalidEstimate},
		{"unordered",func(p []PricePeriod){p[0],p[1]=p[1],p[0]},ErrInvalidEstimate},
		{"invalid price outside query",func(p []PricePeriod){p[1].Table.Rates[0].InputPerMillion="-1"},ErrInvalidEstimate},
		{"submicrosecond",func(p []PricePeriod){p[0].From=p[0].From.Add(time.Nanosecond)},ErrInvalidEstimate},
	} {
		t.Run(tc.name,func(t *testing.T){
			_,_,_,periods := historicalFixture()
			tc.change(periods)
			got,err := PlanPriceWindows(start,end,periods)
			if !errors.Is(err,tc.want) || got != nil { t.Fatalf("windows=%+v err=%v",got,err) }
		})
	}
	_,_,_,periods := historicalFixture()
	if _,err := PlanPriceWindows(start.Add(-2*time.Hour),end,periods);!errors.Is(err,ErrPriceCoverage){t.Fatalf("uncovered start: %v",err)}
	if _,err := PlanPriceWindows(start,end.Add(32*24*time.Hour),periods);!errors.Is(err,ErrInvalidEstimate){t.Fatalf("oversized: %v",err)}
}

func TestHistoricalRejectsMissingOrMismatchedCaptures(t *testing.T) {
	start,mid,end,periods := historicalFixture()
	valid := UsageWindow{From:start,To:mid,Models:[]ledger.ModelUsage{}}
	second := UsageWindow{From:mid,To:end,Models:[]ledger.ModelUsage{}}
	for _,tc := range []struct{name string; captures []UsageWindow; want error}{
		{"absent",nil,ErrUsageWindowMismatch},
		{"missing second",[]UsageWindow{valid},ErrUsageWindowMismatch},
		{"single aggregate across change",[]UsageWindow{{From:start,To:end,Models:[]ledger.ModelUsage{usage("a","1","0","1")}}},ErrUsageWindowMismatch},
		{"reordered",[]UsageWindow{second,valid},ErrUsageWindowMismatch},
		{"null models",[]UsageWindow{valid,{From:mid,To:end}},ErrUsageWindowMismatch},
		{"wrong boundary",[]UsageWindow{valid,{From:mid.Add(time.Second),To:end,Models:[]ledger.ModelUsage{}}},ErrUsageWindowMismatch},
		{"missing rate",[]UsageWindow{valid,{From:mid,To:end,Models:[]ledger.ModelUsage{usage("unpriced","1","0","1")}}},ErrMissingRate},
	} {
		t.Run(tc.name,func(t *testing.T){
			got,err := EstimateHistorical(start,end,periods,tc.captures)
			if !errors.Is(err,tc.want) || got.TotalCost != "" || got.Windows != nil { t.Fatalf("estimate=%+v err=%v",got,err) }
		})
	}
}
