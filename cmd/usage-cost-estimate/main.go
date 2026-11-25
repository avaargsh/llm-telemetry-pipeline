// usage-cost-estimate reads a local model usage breakdown and an explicit
// price snapshot. It never calls a provider or changes the ledger.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/finops"
	"github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
)

const maxInputBytes = 1 << 20

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("usage-cost-estimate",flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	usagePath := flags.String("usage","","JSON model usage breakdown")
	pricePath := flags.String("prices","","JSON price snapshot")
	bindingsPath := flags.String("price-bindings","","JSON expected version-to-fingerprint bindings")
	schedule := flags.String("schedule","","JSON historical price period array")
	from := flags.String("from","","historical window start RFC3339")
	to := flags.String("to","","historical window end RFC3339")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return errors.New("usage: usage-cost-estimate --usage usage.json --prices prices.json [--price-bindings bindings.json] OR --schedule schedule.json --from RFC3339 --to RFC3339 [--price-bindings bindings.json]")
	}
	var bindings priceBindings
	var bindingErr error
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "price-bindings" {
			bindings, bindingErr = readPriceBindings(*bindingsPath)
		}
	})
	if bindingErr != nil { return bindingErr }
	if *schedule!="" || *from!="" || *to!="" {
		if *usagePath!="" || *pricePath!="" { return errors.New("choose either offline or historical mode") }
		if *schedule=="" || *from=="" || *to=="" { return errors.New("historical mode requires --schedule, --from and --to") }
		return runHistorical(*schedule,*from,*to,bindings,out)
	}
	if *usagePath=="" || *pricePath=="" {
		return errors.New("usage: usage-cost-estimate --usage usage.json --prices prices.json [--price-bindings bindings.json]")
	}
	var models []ledger.ModelUsage
	if err := readJSON(*usagePath,&models); err != nil || models == nil {
		return errors.New("invalid usage file: expected a non-null model array")
	}
	var table finops.PriceTable
	if err := readJSON(*pricePath,&table); err != nil || table.Rates == nil {
		return errors.New("invalid prices file: expected an explicit rate array")
	}
	result,err := finops.Estimate(models,table)
	if err != nil { return err }
	if err := bindings.verify(result); err != nil { return err }
	enc := json.NewEncoder(out)
	enc.SetIndent("","  ")
	return enc.Encode(result)
}

func readJSON(path string, target any) error {
	f,err := os.Open(path)
	if err != nil { return err }
	defer f.Close()
	info,err := f.Stat()
	if err != nil || !info.Mode().IsRegular() { return errors.New("expected regular input file") }
	data,err := io.ReadAll(io.LimitReader(f,maxInputBytes+1))
	if err != nil || len(data) > maxInputBytes { return errors.New("input unreadable or too large") }
	if !utf8.Valid(data) { return errors.New("invalid UTF-8 input") }
	if !validUnicodeEscapes(data) { return errors.New("invalid Unicode escape") }
	// encoding/json normally accepts repeated object keys. Reject them
	// before typed decoding, including case variants of struct field names.
	check := json.NewDecoder(bytes.NewReader(data))
	check.UseNumber()
	if err := scanValue(check,0); err != nil { return err }
	if _,err := check.Token(); err != io.EOF { return errors.New("trailing JSON") }
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(target)
}

func scanValue(dec *json.Decoder, depth int) error {
	if depth > 16 { return errors.New("JSON nesting limit exceeded") }
	tok,err := dec.Token()
	if err != nil { return err }
	d,ok := tok.(json.Delim)
	if !ok { return nil }
	switch d {
	case '{':
		seen := make(map[string]bool)
		for dec.More() {
			key,err := dec.Token()
			if err != nil { return err }
			name,ok := key.(string)
			if !ok { return errors.New("invalid JSON key") }
			// All accepted schema field names are ASCII. Reject Unicode aliases
			// (such as long s) before encoding/json can fold them onto a field.
			for _, r := range name {
				if r > 127 { return errors.New("non-ASCII JSON key") }
			}
			name = strings.ToLower(name)
			if seen[name] { return errors.New("duplicate JSON key") }
			seen[name] = true
			if err := scanValue(dec,depth+1); err != nil { return err }
		}
		end,err := dec.Token()
		if err != nil || end != json.Delim('}') { return errors.New("invalid JSON object") }
	case '[':
		for dec.More() {
			if err := scanValue(dec,depth+1); err != nil { return err }
		}
		end,err := dec.Token()
		if err != nil || end != json.Delim(']') { return errors.New("invalid JSON array") }
	default:
		return errors.New("invalid JSON delimiter")
	}
	return nil
}
