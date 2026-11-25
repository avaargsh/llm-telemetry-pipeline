package finops

import (
	"errors"
	"math/big"
	"strings"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
)

var (
	ErrPriceCoverage = errors.New("price periods do not cover the requested window")
	ErrUsageWindowMismatch = errors.New("usage captures do not match planned price windows")
)

const MaxPricePeriods = 128

// PricePeriod is one explicitly bounded, immutable price snapshot interval.
// Both the price schedule and the usage window are half-open [from,to).
type PricePeriod struct {
	From time.Time `json:"from"`
	To time.Time `json:"to"`
	Table PriceTable `json:"table"`
}

type PriceWindow struct {
	From time.Time `json:"from"`
	To time.Time `json:"to"`
	PriceVersion string `json:"price_version"`
}

// UsageWindow must contain an independently captured model breakdown for
// exactly one planned window. An explicit empty array is valid; nil is
// missing evidence, not zero usage.
type UsageWindow struct {
	From time.Time `json:"from"`
	To time.Time `json:"to"`
	Models []ledger.ModelUsage `json:"models"`
}

type HistoricalWindowCost struct {
	From time.Time `json:"from"`
	To time.Time `json:"to"`
	Estimate CostEstimate `json:"estimate"`
}

type HistoricalEstimate struct {
	From time.Time `json:"from"`
	To time.Time `json:"to"`
	Currency string `json:"currency"`
	Windows []HistoricalWindowCost `json:"windows"`
	TotalCost string `json:"total_cost"`
}

type plannedPriceWindow struct {
	PriceWindow
	table PriceTable
}

// PlanPriceWindows partitions a requested ledger query into exact
// price-version intervals. No token counts are apportioned or interpolated.
func PlanPriceWindows(from, to time.Time, periods []PricePeriod) ([]PriceWindow,error) {
	planned,err := planPriceWindows(from,to,periods)
	if err != nil { return nil,err }
	windows := make([]PriceWindow,0,len(planned))
	for _,w := range planned { windows = append(windows,w.PriceWindow) }
	return windows,nil
}

func validPriceInstant(t time.Time) bool {
	return !t.IsZero() && t.Year() >= 1 && t.Year() <= 9999 && t.Nanosecond()%1000 == 0
}

func planPriceWindows(from,to time.Time,periods []PricePeriod)([]plannedPriceWindow,error) {
	if !validPriceInstant(from) || !validPriceInstant(to) || !from.Before(to) ||
		to.Sub(from) > ledger.MaxUsageWindow || len(periods)>MaxPricePeriods {
		return nil,ErrInvalidEstimate
	}
	// Validate the entire supplied schedule, even periods outside the query.
	seen := make(map[string]bool,len(periods))
	for i,p := range periods {
		if !validPriceInstant(p.From) || !validPriceInstant(p.To) || !p.From.Before(p.To) ||
			(i>0 && p.From.Before(periods[i-1].To)) || seen[p.Table.Version] || p.Table.Rates == nil {
			return nil,ErrInvalidEstimate
		}
		if _,err := Estimate(nil,p.Table); err != nil { return nil,err }
		seen[p.Table.Version] = true
	}
	cursor := from
	result := make([]plannedPriceWindow,0,len(periods))
	for _,p := range periods {
		if !p.To.After(cursor) { continue }
		if !p.From.Before(to) { break }
		if p.From.After(cursor) { return nil,ErrPriceCoverage }
		end := p.To
		if end.After(to) { end = to }
		result = append(result,plannedPriceWindow{
			PriceWindow:PriceWindow{From:cursor.UTC(),To:end.UTC(),PriceVersion:p.Table.Version},
			table:p.Table,
		})
		cursor = end
		if cursor.Equal(to) { return result,nil }
	}
	return nil,ErrPriceCoverage
}

// EstimateHistorical combines independently captured, exact price-window
// usage breakdowns. It refuses missing, overlapping or unmatched captures
// instead of splitting aggregated token counts by time or assuming zero.
func EstimateHistorical(from,to time.Time,periods []PricePeriod,captures []UsageWindow)(HistoricalEstimate,error) {
	windows,err := planPriceWindows(from,to,periods)
	if err != nil { return HistoricalEstimate{},err }
	if len(captures)!=len(windows) { return HistoricalEstimate{},ErrUsageWindowMismatch }
	result := HistoricalEstimate{From:from.UTC(),To:to.UTC(),Currency:"USD",Windows:make([]HistoricalWindowCost,0,len(windows))}
	total := new(big.Int)
	for i,w := range windows {
		c := captures[i]
		if !c.From.Equal(w.From) || !c.To.Equal(w.To) || c.Models == nil {
			return HistoricalEstimate{},ErrUsageWindowMismatch
		}
		e,err := Estimate(c.Models,w.table)
		if err != nil { return HistoricalEstimate{},err }
		// Estimate always emits canonical nonnegative dollars with 12
		// fractional digits. Remove the separator to sum integer picodollars.
		n,ok := new(big.Int).SetString(strings.Replace(e.TotalCost,".","",1),10)
		if !ok { return HistoricalEstimate{},ErrInvalidEstimate }
		total.Add(total,n)
		result.Windows = append(result.Windows,HistoricalWindowCost{From:w.From,To:w.To,Estimate:e})
	}
	result.TotalCost = formatCost(total)
	return result,nil
}
