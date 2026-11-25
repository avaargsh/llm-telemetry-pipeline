package finops

import (
    "encoding/json"
    "testing"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
)

// Check the external JSON contract without requiring a new Go field to
// compile on the baseline. This makes the first commit a runnable RED.
func estimateFingerprint(t *testing.T, table PriceTable) string {
    t.Helper()
    got, err := Estimate(nil, table)
    if err != nil { t.Fatal(err) }
    raw, err := json.Marshal(got)
    if err != nil { t.Fatal(err) }
    var result struct { Fingerprint string `json:"price_fingerprint"` }
    if err := json.Unmarshal(raw, &result); err != nil { t.Fatal(err) }
    if len(result.Fingerprint) != 71 || result.Fingerprint[:7] != "sha256:" {
        t.Fatalf("missing canonical price fingerprint: %q", result.Fingerprint)
    }
    return result.Fingerprint
}

func TestPriceFingerprintGolden(t *testing.T) {
    table := PriceTable{Version:"example-v1", Currency:"USD", Rates:[]ModelRate{
        {Model:"Qwen3-32B", InputPerMillion:"2.5", OutputPerMillion:"10"},
    }}
    const want = "sha256:433d2e0b10d8481699f2a2353f26bef71f057dd2cb08dc7069c026c0d7cc3cd5"
    if got := estimateFingerprint(t, table); got != want { t.Fatalf("got %s want %s", got, want) }
}

func TestPriceFingerprintCanonicalContent(t *testing.T) {
    a := PriceTable{Version:"v1", Currency:"USD", Rates:[]ModelRate{
        {Model:"模型-a", InputPerMillion:"1.0", OutputPerMillion:"0"},
        {Model:"b", InputPerMillion:"2.50", OutputPerMillion:"3"},
    }}
    b := PriceTable{Version:"v2", Currency:"USD", Rates:[]ModelRate{
        {Model:"b", InputPerMillion:"2.500000", OutputPerMillion:"3.000000"},
        {Model:"模型-a", InputPerMillion:"1", OutputPerMillion:"0.0"},
    }}
    expected := estimateFingerprint(t, a)
    if got := estimateFingerprint(t, b); got != expected { t.Fatal("rate order, equivalent decimals or version label changed content identity") }
    b.Rates[0].OutputPerMillion = "3.000001"
    if got := estimateFingerprint(t, b); got == expected { t.Fatal("changed output rate retained fingerprint") }
    b.Rates[0].OutputPerMillion = "3"
    b.Rates[0].Model = "B"
    if got := estimateFingerprint(t, b); got == expected { t.Fatal("changed model identity retained fingerprint") }
    b.Rates = b.Rates[1:]
    if got := estimateFingerprint(t, b); got == expected { t.Fatal("omitted rate retained fingerprint") }
}

func TestPriceFingerprintIndependentOfUsage(t *testing.T) {
    table := PriceTable{Version:"v1", Currency:"USD", Rates:[]ModelRate{
        {Model:"a", InputPerMillion:"1", OutputPerMillion:"2"},
    }}
    expected := estimateFingerprint(t, table)
    got, err := Estimate([]ledger.ModelUsage{usage("a","1","2","3")}, table)
    if err != nil { t.Fatal(err) }
    raw, err := json.Marshal(got)
    if err != nil { t.Fatal(err) }
    var result struct { Fingerprint string `json:"price_fingerprint"` }
    if err := json.Unmarshal(raw, &result); err != nil { t.Fatal(err) }
    if result.Fingerprint != expected { t.Fatal("usage changed price identity") }
}

func TestHistoricalOutputBindsEachPriceFingerprint(t *testing.T) {
    start, mid, end, periods := historicalFixture()
    captures := []UsageWindow{
        {From:start, To:mid, Models:[]ledger.ModelUsage{}},
        {From:mid, To:end, Models:[]ledger.ModelUsage{}},
    }
    got, err := EstimateHistorical(start, end, periods, captures)
    if err != nil { t.Fatal(err) }
    raw, err := json.Marshal(got)
    if err != nil { t.Fatal(err) }
    var result struct { Windows []struct { Estimate struct {
        Fingerprint string `json:"price_fingerprint"`
    } `json:"estimate"` } `json:"windows"` }
    if err := json.Unmarshal(raw, &result); err != nil { t.Fatal(err) }
    if len(result.Windows)!=2 { t.Fatal("missing historical windows") }
    for i,w := range result.Windows {
        if w.Estimate.Fingerprint != estimateFingerprint(t, periods[i].Table) {
            t.Fatalf("window %d lost exact price content identity", i)
        }
    }
}
