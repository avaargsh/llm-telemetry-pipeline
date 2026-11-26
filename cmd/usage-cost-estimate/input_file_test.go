package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunRejectsFIFOWithoutBlocking(t *testing.T) {
	mkfifo, err := exec.LookPath("mkfifo")
	if err != nil {
		t.Skip("requires mkfifo")
	}
	dir := t.TempDir()
	usage := filepath.Join(dir, "usage.json")
	prices := filepath.Join(dir, "prices.json")
	if out, err := exec.Command(mkfifo, usage).CombinedOutput(); err != nil {
		t.Fatalf("mkfifo: %v: %s", err, out)
	}
	if err := os.WriteFile(prices, []byte(`{"version":"v1","currency":"USD","rates":[]}`), 0600); err != nil {
		t.Fatal(err)
	}

	// Use the test binary as a subprocess, giving a blocked CLI a hard deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestUsageCostCLIChild$", "--", "--usage", usage, "--prices", prices)
	cmd.Env = append(os.Environ(), "TEST_USAGE_COST_FIFO_CHILD=1")
	out, err := cmd.Output()
	var exit *exec.ExitError
	if ctx.Err() != nil || !errors.As(err, &exit) || exit.ExitCode() != 1 ||
		len(out) != 0 || !strings.Contains(string(exit.Stderr), "invalid usage file:") {
		t.Fatalf("expected prompt invalid-input exit; deadline=%v err=%v output=%q", ctx.Err(), err, out)
	}
}

func TestUsageCostCLIChild(t *testing.T) {
	if os.Getenv("TEST_USAGE_COST_FIFO_CHILD") != "1" {
		return
	}
	os.Args = append(os.Args[:1], os.Args[3:]...)
	main()
	t.Fatal("CLI unexpectedly succeeded")
}
