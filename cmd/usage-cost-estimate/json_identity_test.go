package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestReadJSONRejectsAmbiguousPriceIdentity(t *testing.T) {
	// encoding/json case-folds long s and Kelvin sign when matching struct
	// fields; strings.ToLower alone does not identify every such alias.
	type price struct {
		Version string `json:"version"`
		Rates   []string `json:"rates"`
	}
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"long-s duplicate", []byte(`{"version":"v1","rates":["paid"],"rate\u017f":["free"]}`)},
		{"long-s alias", []byte(`{"ver\u017fion":"v1","rates":[]}`)},
		{"invalid UTF-8 value", []byte("{\"version\":\"v\xff\",\"rates\":[]}")},
		{"invalid UTF-8 key", []byte("{\"ver\xffsion\":\"v1\",\"rates\":[]}")},
		{"ASCII duplicate", []byte(`{"version":"v1","VERSION":"v2","rates":[]}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "prices.json")
			if err := os.WriteFile(path, tc.data, 0600); err != nil {
				t.Fatal(err)
			}
			var got price
			if err := readJSON(path, &got); err == nil {
				t.Fatalf("ambiguous JSON accepted: %+v", got)
			}
		})
	}
}

func TestReadJSONPreservesUnicodeValuesAndASCIIFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prices.json")
	if err := os.WriteFile(path, []byte(`{"VERSION":"价格-v1","rates":["模型"]}`), 0600); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Version string `json:"version"`
		Rates []string `json:"rates"`
	}
	if err := readJSON(path, &got); err != nil || got.Version != "价格-v1" || len(got.Rates) != 1 || got.Rates[0] != "模型" {
		t.Fatalf("valid Unicode values/ASCII case compatibility lost: %+v err=%v", got, err)
	}
}

func TestReadJSONUnicodeScalarIdentity(t *testing.T) {
	for _, tc := range []struct {
		input, want string
		valid bool
	}{
		{"\"\\ud800\"", "", false},
		{"\"\\udfff\"", "", false},
		{"\"\\ud800\\u0041\"", "", false},
		{"\"\\ud800\\ud800\"", "", false},
		{"\"\\ud83d\\ude80\"", "🚀", true},
		{"\"\\uFFFD\"", "�", true},
		{"\"\\\\ud800\"", "\\ud800", true},
		{"\"模型\"", "模型", true},
	} {
		t.Run(tc.input, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "prices.json")
			if err := os.WriteFile(path, []byte(tc.input), 0600); err != nil {
				t.Fatal(err)
			}
			var got string
			err := readJSON(path, &got)
			if (err == nil) != tc.valid || (tc.valid && got != tc.want) {
				t.Fatalf("got %q err=%v", got, err)
			}
		})
	}
}


// Verify the public CLI boundary: malformed model/version identities must
// not produce a price receipt, even when the usage array is empty.
func TestCostEstimateRejectsLossyUnicodeIdentity(t *testing.T) {
	dir := t.TempDir()
	usage := filepath.Join(dir, "usage.json")
	prices := filepath.Join(dir, "prices.json")
	if err := os.WriteFile(usage, []byte("[]"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, table string
		valid bool
	}{
		{"unpaired model", "{\"version\":\"v1\",\"currency\":\"USD\",\"rates\":[{\"model\":\"\\ud800\",\"input_per_million\":\"1\",\"output_per_million\":\"2\"}]}", false},
		{"unpaired version", "{\"version\":\"\\ud800\",\"currency\":\"USD\",\"rates\":[]}", false},
		{"explicit replacement", "{\"version\":\"v1\",\"currency\":\"USD\",\"rates\":[{\"model\":\"\\ufffd\",\"input_per_million\":\"1\",\"output_per_million\":\"2\"}]}", true},
		{"valid pair", "{\"version\":\"v1\",\"currency\":\"USD\",\"rates\":[{\"model\":\"\\ud83d\\ude80\",\"input_per_million\":\"1\",\"output_per_million\":\"2\"}]}", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(prices, []byte(tc.table), 0600); err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			err := run([]string{"--usage", usage, "--prices", prices}, &out)
			if tc.valid {
				if err != nil || out.Len() == 0 {
					t.Fatalf("valid scalar rejected: %v", err)
				}
			} else if err == nil || out.Len() != 0 {
				t.Fatalf("lossy identity accepted: err=%v output=%s", err, out.String())
			}
		})
	}
}
