package main

import (
    "testing"
    "time"
)

func TestValidateRelayTuningAcceptsBoundedOptions(t *testing.T) {
    for _, tc := range []struct {limit int; batchTimeout time.Duration}{
        {5, 100*time.Millisecond}, {25, 5*time.Millisecond},
        {1, time.Millisecond}, {500, time.Second},
    } {
        if err := validateRelayTuning(tc.limit,tc.batchTimeout); err!=nil {
            t.Fatalf("limit=%d timeout=%s: unexpected error: %v",tc.limit,tc.batchTimeout,err)
        }
    }
}

func TestValidateRelayTuningRejectsUnsafeOptions(t *testing.T) {
    for _,tc:=range []struct {limit int; batchTimeout time.Duration}{
        {0,time.Millisecond}, {501,time.Millisecond}, {-1,time.Second},
        {5,0}, {5,-time.Millisecond}, {5,time.Microsecond}, {5,2*time.Second},
    } {
        if err:=validateRelayTuning(tc.limit,tc.batchTimeout);err==nil {
            t.Fatalf("limit=%d timeout=%s: expected error",tc.limit,tc.batchTimeout)
        }
    }
}
