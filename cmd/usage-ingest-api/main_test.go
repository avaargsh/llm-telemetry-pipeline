package main

import "testing"

func TestRequiresTLSExceptLoopback(t *testing.T) {
    for _, tc := range []struct {
        address  string
        requires bool
    }{
        {"127.0.0.1:8085", false},
        {"[::1]:8085", false},
        {"localhost:8085", false},
        {":8085", true},
        {"0.0.0.0:8085", true},
        {"[::]:8085", true},
        {"example.com:8085", true},
        {"badaddress", true},
    } {
        if got := requiresTLS(tc.address); got != tc.requires {
            t.Errorf("requiresTLS(%q)=%t want %t", tc.address, got, tc.requires)
        }
    }
}
