package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestRegistryModeFlags(t *testing.T) {
	t.Setenv("USAGE_QUERY_DATABASE_URL","")
	p:=filepath.Join(t.TempDir(),"schedule.json")
	if err:=os.WriteFile(p,[]byte(`[{"from":"2026-11-21T00:00:00Z","to":"2026-11-21T01:00:00Z","table":{"version":"v1","currency":"USD","rates":[]}}]`),0600);err!=nil { t.Fatal(err) }
	for _,tc:=range []struct{args []string;want string}{
		{[]string{"--usage","usage.json","--prices","prices.json","--verify-price-registry"},"--verify-price-registry requires historical mode"},
		{[]string{"--verify-price-registry"},"--verify-price-registry requires historical mode"},
		{[]string{"--schedule",p,"--from","2026-11-21T00:00:00Z","--to","2026-11-21T01:00:00Z","--verify-price-registry"},"USAGE_QUERY_DATABASE_URL is required"},
	}{
		var out bytes.Buffer
		err:=run(tc.args,&out)
		if err==nil||err.Error()!=tc.want||out.Len()!=0 { t.Fatalf("args=%v error=%v output=%s",tc.args,err,out.String()) }
	}
}
