package finops

import (
	"context"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
)

type VerifiedModelWindowReader interface {
	VerifiedUsageByModelWindows(context.Context, string, string, []ledger.ModelQueryWindow, []ledger.PriceVersionExpectation) ([]ledger.ModelWindowCapture, error)
}

var _ VerifiedModelWindowReader = (*ledger.ScopedUsageReader)(nil)

type verifiedHistoryReader struct {
	reader VerifiedModelWindowReader
	expectations []ledger.PriceVersionExpectation
}

func (v verifiedHistoryReader) UsageByModelWindows(ctx context.Context, tenant, project string, windows []ledger.ModelQueryWindow) ([]ledger.ModelWindowCapture, error) {
	return v.reader.VerifiedUsageByModelWindows(ctx, tenant, project, windows, v.expectations)
}

// EstimateVerifiedHistory checks all supplied price tables, even outside
// the requested window, against the registry in the usage read snapshot.
// It verifies content bindings, not the authenticity of effective dates.
func EstimateVerifiedHistory(ctx context.Context, reader VerifiedModelWindowReader, tenant, project string, from, to time.Time, periods []PricePeriod) (HistoricalEstimate, error) {
	if reader == nil {
		return HistoricalEstimate{}, ErrInvalidEstimate
	}
	if _, err := PlanPriceWindows(from, to, periods); err != nil {
		return HistoricalEstimate{}, err
	}
	expectations := make([]ledger.PriceVersionExpectation, 0, len(periods))
	for _, period := range periods {
		estimate, err := Estimate(nil, period.Table)
		if err != nil {
			return HistoricalEstimate{}, err
		}
		expectations = append(expectations, ledger.PriceVersionExpectation{Version: estimate.PriceVersion, Fingerprint: estimate.PriceFingerprint})
	}
	return EstimateScopedHistory(ctx, verifiedHistoryReader{reader: reader, expectations: expectations}, tenant, project, from, to, periods)
}
