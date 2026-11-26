package finops

import (
	"math/big"
	"sort"
	"strings"
	"unicode/utf8"
)

// TariffPayload preserves the exact version label and normalized complete
// price content. Fingerprint is the existing canonical v1 content digest,
// not an assertion about source authenticity or effective dates.
type TariffPayload struct {
	Table       PriceTable `json:"table"`
	Fingerprint string     `json:"fingerprint"`
}

// CanonicalTariffPayload prepares a detached, deterministic payload for
// future catalog storage. It rejects identities PostgreSQL/JSON cannot
// faithfully represent and requires an explicit rates array.
func CanonicalTariffPayload(table PriceTable) (TariffPayload, error) {
	if table.Rates == nil || !validStorageIdentity(table.Version) {
		return TariffPayload{}, ErrInvalidEstimate
	}
	for _, r := range table.Rates {
		if !validStorageIdentity(r.Model) {
			return TariffPayload{}, ErrInvalidEstimate
		}
	}

	estimate, err := Estimate(nil, table)
	if err != nil {
		return TariffPayload{}, err
	}

	normalized := PriceTable{
		Version:  table.Version,
		Currency: table.Currency,
		Rates:    make([]ModelRate, 0, len(table.Rates)),
	}
	for _, r := range table.Rates {
		input, ok := rateMicros(r.InputPerMillion)
		if !ok {
			return TariffPayload{}, ErrInvalidEstimate
		}
		output, ok := rateMicros(r.OutputPerMillion)
		if !ok {
			return TariffPayload{}, ErrInvalidEstimate
		}
		normalized.Rates = append(normalized.Rates, ModelRate{
			Model:            r.Model,
			InputPerMillion:  formatTariffRate(input),
			OutputPerMillion: formatTariffRate(output),
		})
	}
	sort.Slice(normalized.Rates, func(i, j int) bool {
		return normalized.Rates[i].Model < normalized.Rates[j].Model
	})

	return TariffPayload{
		Table:       normalized,
		Fingerprint: estimate.PriceFingerprint,
	}, nil
}

func validStorageIdentity(s string) bool {
	return utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}

func formatTariffRate(n *big.Int) string {
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(n, big.NewInt(1_000_000), remainder)
	fraction := remainder.String()
	return quotient.String() + "." + strings.Repeat("0", 6-len(fraction)) + fraction
}
