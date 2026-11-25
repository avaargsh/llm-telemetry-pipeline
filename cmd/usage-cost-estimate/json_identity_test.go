package main

import (
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
