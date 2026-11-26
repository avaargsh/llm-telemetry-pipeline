package finops

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
)

type tariffPayloadReaderFunc func(context.Context, string, string, string) (ledger.PriceTariffPayloadRecord, error)

func (f tariffPayloadReaderFunc) PriceTariffPayload(ctx context.Context, tenant, project, version string) (ledger.PriceTariffPayloadRecord, error) {
	return f(ctx, tenant, project, version)
}

func TestLoadRegisteredTariffRecomputesFingerprint(t *testing.T) {
	table := PriceTable{
		Version: "v1",
		Currency: "USD",
		Rates: []ModelRate{{Model: "a", InputPerMillion: "1", OutputPerMillion: "2"}},
	}
	payload, err := CanonicalTariffPayload(table)
	if err != nil {
		t.Fatal(err)
	}
	rates, err := json.Marshal(payload.Table.Rates)
	if err != nil {
		t.Fatal(err)
	}
	reader := tariffPayloadReaderFunc(func(_ context.Context, tenant, project, version string) (ledger.PriceTariffPayloadRecord, error) {
		if tenant != "tenant" || project != "project" || version != "v1" {
			t.Fatalf("unexpected scope: %s/%s/%s", tenant, project, version)
		}
		return ledger.PriceTariffPayloadRecord{
			Version: "v1", Fingerprint: payload.Fingerprint,
			SchemaVersion: 1, Currency: "USD", RatesJSON: rates,
		}, nil
	})
	got, err := LoadRegisteredTariff(context.Background(), reader, "tenant", "project", "v1")
	if err != nil || got.Fingerprint != payload.Fingerprint || len(got.Table.Rates) != 1 {
		t.Fatalf("got %+v err=%v", got, err)
	}
}

func TestLoadRegisteredTariffRejectsTamperedPayload(t *testing.T) {
	for _, tc := range []struct {
		name string
		record ledger.PriceTariffPayloadRecord
		want error
	}{
		{
			name: "fingerprint mismatch",
			record: ledger.PriceTariffPayloadRecord{
				Version: "v1", Fingerprint: "sha256:" + string(make([]byte, 64)),
				SchemaVersion: 1, Currency: "USD",
				RatesJSON: []byte(`[{"model":"a","input_per_million":"1.000000","output_per_million":"2.000000"}]`),
			},
			want: ledger.ErrPriceVersionMismatch,
		},
		{
			name: "malformed rates",
			record: ledger.PriceTariffPayloadRecord{
				Version: "v1", Fingerprint: "sha256:" + string(make([]byte, 64)),
				SchemaVersion: 1, Currency: "USD", RatesJSON: []byte("{"),
			},
			want: ErrInvalidEstimate,
		},
		{
			name: "null rates",
			record: ledger.PriceTariffPayloadRecord{
				Version: "v1", Fingerprint: "sha256:" + string(make([]byte, 64)),
				SchemaVersion: 1, Currency: "USD", RatesJSON: []byte("null"),
			},
			want: ErrInvalidEstimate,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := tariffPayloadReaderFunc(func(context.Context, string, string, string) (ledger.PriceTariffPayloadRecord, error) {
				return tc.record, nil
			})
			got, err := LoadRegisteredTariff(context.Background(), reader, "tenant", "project", "v1")
			if !errors.Is(err, tc.want) || got.Fingerprint != "" {
				t.Fatalf("got %+v err=%v want=%v", got, err, tc.want)
			}
		})
	}
}

func TestLoadRegisteredTariffRejectsNilReader(t *testing.T) {
	if _, err := LoadRegisteredTariff(context.Background(), nil, "tenant", "project", "v1"); !errors.Is(err, ledger.ErrInvalidPriceVersion) {
		t.Fatalf("got %v", err)
	}
}
