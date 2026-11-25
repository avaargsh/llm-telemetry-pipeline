package finops

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
)

type snapshotReaderFunc func(context.Context,string,string,[]ledger.ModelQueryWindow)([]ledger.ModelWindowCapture,error)

func (f snapshotReaderFunc) UsageByModelWindows(ctx context.Context,tenant,project string,windows []ledger.ModelQueryWindow)([]ledger.ModelWindowCapture,error) {
	return f(ctx,tenant,project,windows)
}

func TestEstimateScopedHistoryUsesSingleCapture(t *testing.T) {
	start,mid,end,periods := historicalFixture()
	calls := 0
	reader := snapshotReaderFunc(func(ctx context.Context,tenant,project string,windows []ledger.ModelQueryWindow)([]ledger.ModelWindowCapture,error){
		calls++
		if tenant!="tenant" || project!="project" || len(windows)!=2 ||
			!windows[0].From.Equal(start) || !windows[0].To.Equal(mid) ||
			!windows[1].From.Equal(mid) || !windows[1].To.Equal(end) { t.Fatalf("wrong capture request: %s %s %+v",tenant,project,windows) }
		return []ledger.ModelWindowCapture{
			{ModelQueryWindow:windows[0],Models:[]ledger.ModelUsage{usage("a","100","0","100")}},
			{ModelQueryWindow:windows[1],Models:[]ledger.ModelUsage{usage("a","200","0","200")}},
		},nil
	})
	got,err := EstimateScopedHistory(context.Background(),reader,"tenant","project",start,end,periods)
	if err!=nil || calls!=1 || got.TotalCost!="0.000500000000" { t.Fatalf("estimate=%+v calls=%d err=%v",got,calls,err) }
}

func TestEstimateScopedHistoryFailsClosed(t *testing.T) {
	start,mid,end,periods := historicalFixture()
	calls := 0
	reader := snapshotReaderFunc(func(context.Context,string,string,[]ledger.ModelQueryWindow)([]ledger.ModelWindowCapture,error){
		calls++
		return nil,ledger.ErrUsageQueryScope
	})
	got,err := EstimateScopedHistory(context.Background(),reader,"tenant","project",start,end,periods)
	if !errors.Is(err,ledger.ErrUsageQueryScope) || got.TotalCost!="" || calls!=1 { t.Fatalf("estimate=%+v calls=%d err=%v",got,calls,err) }
	periods[1].From=mid.Add(time.Minute)
	got,err=EstimateScopedHistory(context.Background(),reader,"tenant","project",start,end,periods)
	if !errors.Is(err,ErrPriceCoverage)||got.TotalCost!=""||calls!=1 { t.Fatalf("invalid schedule queried: %+v calls=%d err=%v",got,calls,err) }
}
