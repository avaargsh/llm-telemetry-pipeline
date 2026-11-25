package ledger

import (
	"strings"
	"unicode/utf8"
)

func validPriceVersionLookup(version string) bool {
	return len(version) <= 128 && utf8.ValidString(version) && !strings.ContainsRune(version, 0) && strings.TrimSpace(version) != ""
}
