package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/event"
)

func main() {
	count := flag.Int("count", 10, "number of synthetic events")
	seed := flag.String("seed", "demo-run-001", "logical seed for deterministic replay")
	start := flag.String("start", "", "start timestamp RFC3339; defaults to midnight UTC today")
	flag.Parse()
	if *count < 1 {
		fmt.Fprintln(os.Stderr, "--count must be positive")
		os.Exit(2)
	}
	t := time.Now().UTC().Truncate(24 * time.Hour)
	if *start != "" {
		parsed, err := time.Parse(time.RFC3339, *start)
		if err != nil {
			fmt.Fprintln(os.Stderr, "invalid --start:", err)
			os.Exit(2)
		}
		t = parsed.UTC()
	}
	out := bufio.NewWriterSize(os.Stdout, 1024*1024)
	enc := json.NewEncoder(out)
	for i := 0; i < *count; i++ {
		e, err := event.SyntheticUsageEvent(*seed, i, t)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := enc.Encode(e); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if err := out.Flush(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
