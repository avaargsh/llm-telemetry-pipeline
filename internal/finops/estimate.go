// Package finops estimates token costs using an explicitly supplied price
// snapshot. It neither bills customers nor modifies authoritative usage.
package finops

import (
	"errors"
	"math/big"
	"strings"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
)

var (
	ErrInvalidEstimate = errors.New("invalid cost estimate input")
	ErrMissingRate = errors.New("missing model rate")
)

// PriceTable is a caller-selected, single-currency, uniform rate snapshot.
// No implicit/default rate or time-dependent tariff is applied.
type PriceTable struct {
	Version string `json:"version"`
	Currency string `json:"currency"`
	Rates []ModelRate `json:"rates"`
}

type ModelRate struct {
	Model string `json:"model"`
	InputPerMillion string `json:"input_per_million"`
	OutputPerMillion string `json:"output_per_million"`
}

type ModelCost struct {
	Model string `json:"model"`
	InputCost string `json:"input_cost"`
	OutputCost string `json:"output_cost"`
	TotalCost string `json:"total_cost"`
}

type CostEstimate struct {
	Currency string `json:"currency"`
	PriceVersion string `json:"price_version"`
	Models []ModelCost `json:"models"`
	TotalCost string `json:"total_cost"`
}

type rate struct { input, output *big.Int }

// Estimate prices the exact model aggregates using nonnegative USD decimal
// rates per million tokens, with at most six fractional digits. Results
// have twelve fractional digits and require no rounding. Missing rates,
// ambiguous or invalid data fail the entire estimate without partial costs.
// A price snapshot is not historical tariff evidence or an invoice.
func Estimate(models []ledger.ModelUsage, table PriceTable) (CostEstimate, error) {
	if table.Currency != "USD" || strings.TrimSpace(table.Version) == "" ||
		len(table.Version) > 128 || len(table.Rates) > 1000 ||
		len(models) > ledger.MaxUsageModels {
		return CostEstimate{}, ErrInvalidEstimate
	}
	rates := make(map[string]rate, len(table.Rates))
	for _, r := range table.Rates {
		if !validModel(r.Model) { return CostEstimate{}, ErrInvalidEstimate }
		if _, ok := rates[r.Model]; ok { return CostEstimate{}, ErrInvalidEstimate }
		in, ok := rateMicros(r.InputPerMillion)
		if !ok { return CostEstimate{}, ErrInvalidEstimate }
		out, ok := rateMicros(r.OutputPerMillion)
		if !ok { return CostEstimate{}, ErrInvalidEstimate }
		rates[r.Model] = rate{in,out}
	}
	result := CostEstimate{Currency:table.Currency, PriceVersion:table.Version, Models:make([]ModelCost,0,len(models))}
	seen := make(map[string]bool,len(models))
	total := new(big.Int)
	for _, m := range models {
		if !validModel(m.Model) || seen[m.Model] || m.RequestCount <= 0 {
			return CostEstimate{}, ErrInvalidEstimate
		}
		seen[m.Model] = true
		in, ok := unsigned(m.PromptTokens,40)
		if !ok { return CostEstimate{}, ErrInvalidEstimate }
		out, ok := unsigned(m.CompletionTokens,40)
		if !ok { return CostEstimate{}, ErrInvalidEstimate }
		all, ok := unsigned(m.TotalTokens,40)
		if !ok || new(big.Int).Add(in,out).Cmp(all) != 0 { return CostEstimate{}, ErrInvalidEstimate }
		r, ok := rates[m.Model]
		if !ok { return CostEstimate{}, ErrMissingRate }
		inputCost := new(big.Int).Mul(in,r.input)
		outputCost := new(big.Int).Mul(out,r.output)
		modelTotal := new(big.Int).Add(inputCost,outputCost)
		total.Add(total,modelTotal)
		result.Models = append(result.Models,ModelCost{
			Model:m.Model, InputCost:formatCost(inputCost), OutputCost:formatCost(outputCost), TotalCost:formatCost(modelTotal),
		})
	}
	result.TotalCost = formatCost(total)
	return result,nil
}

func validModel(s string) bool {
	return s != "" && len(s) <= 512 && s == strings.TrimSpace(s)
}

func unsigned(s string, limit int) (*big.Int,bool) {
	if len(s) == 0 || len(s) > limit || (len(s) > 1 && s[0] == '0') { return nil,false }
	for _, c := range s { if c < '0' || c > '9' { return nil,false } }
	return new(big.Int).SetString(s,10)
}

func rateMicros(s string) (*big.Int,bool) {
	whole, frac, found := strings.Cut(s,".")
	n, ok := unsigned(whole,18)
	if !ok || (found && (len(frac) == 0 || len(frac) > 6)) { return nil,false }
	for _, c := range frac { if c < '0' || c > '9' { return nil,false } }
	n.Mul(n,big.NewInt(1_000_000))
	if found {
		f, ok := new(big.Int).SetString(frac+strings.Repeat("0",6-len(frac)),10)
		if !ok { return nil,false }
		n.Add(n,f)
	}
	return n,true
}

// One integer unit is one trillionth of a dollar (microdollars / million).
func formatCost(n *big.Int) string {
	q,r := new(big.Int),new(big.Int)
	q.QuoRem(n,big.NewInt(1_000_000_000_000),r)
	frac := r.String()
	return q.String()+"."+strings.Repeat("0",12-len(frac))+frac
}
