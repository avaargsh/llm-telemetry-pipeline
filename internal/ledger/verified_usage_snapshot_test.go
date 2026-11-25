package ledger

import (
	"errors"
	"strings"
	"testing"
)

func TestValidatePriceExpectations(t *testing.T) {
	fp := "sha256:" + strings.Repeat("a", 64)
	other := "sha256:" + strings.Repeat("b", 64)
	for _, tc := range []struct {
		name string
		input []PriceVersionExpectation
		want error
	}{
		{"valid", []PriceVersionExpectation{{"v1", fp}, {"V1", other}}, nil},
		{"empty", nil, ErrInvalidPriceExpectation},
		{"invalid version", []PriceVersionExpectation{{"", fp}}, ErrInvalidPriceExpectation},
		{"invalid fingerprint", []PriceVersionExpectation{{"v1", "sha256:bad"}}, ErrInvalidPriceExpectation},
		{"uppercase digest", []PriceVersionExpectation{{"v1", "sha256:" + strings.Repeat("A", 64)}}, ErrInvalidPriceExpectation},
		{"duplicate", []PriceVersionExpectation{{"v1", fp}, {"v1", fp}}, ErrInvalidPriceExpectation},
		{"conflict", []PriceVersionExpectation{{"v1", fp}, {"v1", other}}, ErrInvalidPriceExpectation},
		{"too many", make([]PriceVersionExpectation, MaxModelQueryWindows+1), ErrInvalidPriceExpectation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validatePriceExpectations(tc.input); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}
