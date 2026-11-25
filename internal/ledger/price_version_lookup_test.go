package ledger

import (
	"strings"
	"testing"
)

func TestPriceVersionLookupIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		valid       bool
	}{
		{"exact ASCII", "v1", true},
		{"case preserved", "V1", true},
		{"Unicode", "版本-1", true},
		{"byte limit", strings.Repeat("界", 42) + "ab", true},
		{"empty", "", false},
		{"whitespace only", "\u3000\t", false},
		{"too many bytes", strings.Repeat("界", 43), false},
		{"NUL", "v\x001", false},
		{"invalid UTF-8", "v\xff", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := validPriceVersionLookup(tc.value); got != tc.valid {
				t.Fatalf("valid=%v, want %v", got, tc.valid)
			}
		})
	}
}
