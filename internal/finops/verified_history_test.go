package finops

import (
	"context"
	"errors"
	"testing"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
)

type verifiedReaderFunc func(context.Context,string,string,[]ledger.ModelQueryWindow,[]ledger.PriceVersionExpectation)([]ledger.ModelWindowCapture,error)

func (f verifiedReaderFunc) VerifiedUsageByModelWindows(ctx context.Context,tenant,project string,windows []ledger.ModelQueryWindow,expectations []ledger.PriceVersionExpectation)([]ledger.ModelWindowCapture,error) {
	return f(ctx,tenant,project,windows,expectations)
}

func TestEstimateVerifiedHistoryChecksEntireSchedule(t *testing.T) {
	start,mid,_,periods := historicalFixture()
	calls := 0
	reader := verifiedReaderFunc(func(_ context.Context,tenant,project string,windows []ledger.ModelQueryWindow,expectations []ledger.PriceVersionExpectation)([]ledger.ModelWindowCapture,error){
		calls++
		if tenant!="tenant" || project!="project" || len(windows)!=1 || !windows[0].From.Equal(start) || !windows[0].To.Equal(mid) || len(expectations)!=2 {
			t.Fatalf("wrong request: %s %s %+v %+v",tenant,project,windows,expectations)
		}
		for i,p := range periods {
			want,err:=Estimate(nil,p.Table)
			if err!=nil || expectations[i].Version!=want.PriceVersion || expectations[i].Fingerprint!=want.PriceFingerprint {
				t.Fatalf("expectation %d: %+v %v",i,expectations[i],err)
			}
		}
		return []ledger.ModelWindowCapture{{ModelQueryWindow:windows[0],Models:[]ledger.ModelUsage{usage("a","100","0","100")}}},nil
	})
	got,err:=EstimateVerifiedHistory(context.Background(),reader,"tenant","project",start,mid,periods)
	if err!=nil || calls!=1 || got.TotalCost!="0.000100000000" { t.Fatalf("got %+v calls=%d err=%v",got,calls,err) }
}

func TestEstimateVerifiedHistoryRejectsRegistryFailure(t *testing.T) {
	start,_,end,periods:=historicalFixture()
	for _,want:=range []error{ledger.ErrPriceVersionNotFound,ledger.ErrPriceVersionMismatch,ledger.ErrUsageQueryScope} {
		calls:=0
		reader:=verifiedReaderFunc(func(context.Context,string,string,[]ledger.ModelQueryWindow,[]ledger.PriceVersionExpectation)([]ledger.ModelWindowCapture,error){
			calls++
			return nil,want
		})
		got,err:=EstimateVerifiedHistory(context.Background(),reader,"tenant","project",start,end,periods)
		if !errors.Is(err,want)||got.TotalCost!=""||calls!=1 { t.Fatalf("got %+v calls=%d err=%v want=%v",got,calls,err,want) }
	}
}
