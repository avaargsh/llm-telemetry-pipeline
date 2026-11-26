package finops

import (
	"errors"
	"reflect"
	"testing"
)

func TestCanonicalTariffPayload(t *testing.T) {
	a := PriceTable{
		Version:  "v1",
		Currency: "USD",
		Rates: []ModelRate{
			{Model: "模型", InputPerMillion: "1", OutputPerMillion: "0.0"},
			{Model: "a", InputPerMillion: "2.50", OutputPerMillion: "3"},
		},
	}
	b := PriceTable{
		Version:  "v1",
		Currency: "USD",
		Rates: []ModelRate{
			{Model: "a", InputPerMillion: "2.500000", OutputPerMillion: "3.000000"},
			{Model: "模型", InputPerMillion: "1.000000", OutputPerMillion: "0"},
		},
	}

	first, err := CanonicalTariffPayload(a)
	if err != nil {
		t.Fatal(err)
	}
	second, err := CanonicalTariffPayload(b)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("payloads differ: %+v %+v %v", first, second, err)
	}
	if first.Table.Rates[0].Model != "a" ||
		first.Table.Rates[0].InputPerMillion != "2.500000" ||
		first.Table.Rates[1].OutputPerMillion != "0.000000" {
		t.Fatalf("not canonical: %+v", first)
	}
	want, err := Estimate(nil, a)
	if err != nil || first.Fingerprint != want.PriceFingerprint {
		t.Fatalf("fingerprint: %+v %v", first, err)
	}

	a.Rates[0].Model = "changed"
	if first.Table.Rates[1].Model != "模型" {
		t.Fatal("payload aliases caller input")
	}
	b.Version = "V1"
	third, err := CanonicalTariffPayload(b)
	if err != nil || third.Table.Version != "V1" ||
		third.Fingerprint != first.Fingerprint {
		t.Fatalf("version identity: %+v %v", third, err)
	}
}

func TestCanonicalTariffPayloadRejectsInvalidStorageIdentity(t *testing.T) {
	cases := []struct {
		name   string
		change func(*PriceTable)
	}{
		{"null rates", func(p *PriceTable) { p.Rates = nil }},
		{"NUL version", func(p *PriceTable) { p.Version = "v\x001" }},
		{"invalid UTF8 version", func(p *PriceTable) { p.Version = "v\xff" }},
		{"NUL model", func(p *PriceTable) { p.Rates[0].Model = "a\x00b" }},
		{"invalid UTF8 model", func(p *PriceTable) { p.Rates[0].Model = "a\xff" }},
		{"invalid rate", func(p *PriceTable) { p.Rates[0].InputPerMillion = "-1" }},
		{"duplicate model", func(p *PriceTable) { p.Rates = append(p.Rates, p.Rates[0]) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := PriceTable{
				Version:  "v1",
				Currency: "USD",
				Rates: []ModelRate{
					{Model: "a", InputPerMillion: "1", OutputPerMillion: "0"},
				},
			}
			tc.change(&p)
			got, err := CanonicalTariffPayload(p)
			if !errors.Is(err, ErrInvalidEstimate) || got.Fingerprint != "" {
				t.Fatalf("got %+v %v", got, err)
			}
		})
	}

	got, err := CanonicalTariffPayload(PriceTable{
		Version: "v1", Currency: "USD", Rates: []ModelRate{},
	})
	if err != nil || got.Table.Rates == nil || len(got.Table.Rates) != 0 {
		t.Fatalf("explicit empty: %+v %v", got, err)
	}
}
