package finops

import (
	"context"
	"strings"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
)

// ModelWindowReader captures all requested windows in a single scoped
// database snapshot. The production implementation is ScopedUsageReader.
type ModelWindowReader interface {
	UsageByModelWindows(context.Context,string,string,[]ledger.ModelQueryWindow)([]ledger.ModelWindowCapture,error)
}

var _ ModelWindowReader = (*ledger.ScopedUsageReader)(nil)

// EstimateScopedHistory validates the schedule before database I/O, then
// obtains every price interval through one scoped capture call. The
// resulting cost remains an estimate using caller-supplied price evidence.
func EstimateScopedHistory(ctx context.Context,reader ModelWindowReader,tenant,project string,from,to time.Time,periods []PricePeriod)(HistoricalEstimate,error) {
	if reader==nil || strings.TrimSpace(tenant)=="" || strings.TrimSpace(project)=="" {
		return HistoricalEstimate{},ErrInvalidEstimate
	}
	plan,err := PlanPriceWindows(from,to,periods)
	if err != nil { return HistoricalEstimate{},err }
	windows := make([]ledger.ModelQueryWindow,0,len(plan))
	for _,w := range plan { windows = append(windows,ledger.ModelQueryWindow{From:w.From,To:w.To}) }
	captures,err := reader.UsageByModelWindows(ctx,tenant,project,windows)
	if err != nil { return HistoricalEstimate{},err }
	if len(captures)!=len(windows) { return HistoricalEstimate{},ErrUsageWindowMismatch }
	usage := make([]UsageWindow,0,len(captures))
	for _,c := range captures { usage = append(usage,UsageWindow{From:c.From,To:c.To,Models:c.Models}) }
	return EstimateHistorical(from,to,periods,usage)
}
