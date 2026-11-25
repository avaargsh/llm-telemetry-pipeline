package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestHistoricalModeRejectsInvalidInputs(t *testing.T) {
	t.Setenv("USAGE_QUERY_DATABASE_URL","")
	t.Setenv("USAGE_QUERY_TENANT_ID","tenant")
	t.Setenv("USAGE_QUERY_PROJECT_ID","project")
	dir := t.TempDir()
	path := filepath.Join(dir,"schedule.json")
	valid := `[{"from":"2026-11-21T00:00:00Z","to":"2026-11-21T02:00:00Z","table":{"version":"v1","currency":"USD","rates":[]}}]`
	if err := os.WriteFile(path,[]byte(valid),0600);err != nil { t.Fatal(err) }
	for _,tc := range []struct{args []string; want string}{
		{[]string{"--schedule",path,"--usage","usage.json","--from","2026-11-21T00:00:00Z","--to","2026-11-21T01:00:00Z"},"choose either offline or historical mode"},
		{[]string{"--schedule",path,"--from","bad","--to","2026-11-21T01:00:00Z"},"invalid historical time window"},
		{[]string{"--schedule",path,"--from","2026-11-20T00:00:00Z","--to","2026-11-21T01:00:00Z"},"price periods do not cover the requested window"},
		{[]string{"--schedule",path,"--from","2026-11-21T00:00:00Z","--to","2026-11-21T01:00:00Z"},"USAGE_QUERY_DATABASE_URL is required"},
	} {
		var out bytes.Buffer
		err := run(tc.args,&out)
		if err==nil || err.Error()!=tc.want || out.Len()!=0 { t.Fatalf("args=%v error=%v output=%s",tc.args,err,out.String()) }
	}
}

func TestHistoricalModeRejectsNullSchedule(t *testing.T) {
	path := filepath.Join(t.TempDir(),"schedule.json")
	if err := os.WriteFile(path,[]byte("null"),0600);err!=nil { t.Fatal(err) }
	var out bytes.Buffer
	err := run([]string{"--schedule",path,"--from","2026-11-21T00:00:00Z","--to","2026-11-21T01:00:00Z"},&out)
	if err==nil || err.Error()!="invalid schedule file: expected a non-null price period array" || out.Len()!=0 { t.Fatalf("error=%v output=%s",err,out.String()) }
}
