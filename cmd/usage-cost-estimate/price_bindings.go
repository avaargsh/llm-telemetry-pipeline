package main

import (
	"errors"
	"strings"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/finops"
)

const maxPriceBindings = 1000

// nil is the explicitly unbound legacy mode; a supplied empty manifest
// is non-nil and rejects every attempted price version.
type priceBindings map[string]string

type priceBinding struct {
	Version     string `json:"version"`
	Fingerprint string `json:"price_fingerprint"`
}

func readPriceBindings(path string) (priceBindings, error) {
	var entries []priceBinding
	if err := readJSON(path, &entries); err != nil || entries == nil || len(entries) > maxPriceBindings {
		return nil, errors.New("invalid price bindings")
	}
	bindings := make(priceBindings, len(entries))
	for _, entry := range entries {
		if strings.TrimSpace(entry.Version) == "" || len(entry.Version) > 128 || !validPriceFingerprint(entry.Fingerprint) {
			return nil, errors.New("invalid price bindings")
		}
		if _, exists := bindings[entry.Version]; exists {
			return nil, errors.New("invalid price bindings")
		}
		bindings[entry.Version] = entry.Fingerprint
	}
	return bindings, nil
}

func validPriceFingerprint(s string) bool {
	if len(s) != 71 || !strings.HasPrefix(s, "sha256:") {
		return false
	}
	for _, c := range s[7:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func (b priceBindings) verify(estimate finops.CostEstimate) error {
	if b == nil {
		return nil
	}
	expected, ok := b[estimate.PriceVersion]
	if !ok {
		return errors.New("missing price version binding")
	}
	if expected != estimate.PriceFingerprint {
		return errors.New("price version binding mismatch")
	}
	return nil
}
