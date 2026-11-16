package reconcile

import (
    "context"
    "strings"
    "testing"
    "time"
)

type ledgerFixture struct{ rows []LedgerRecord; err error }
func(f ledgerFixture)ListLedger(context.Context,time.Time,time.Time,int)([]LedgerRecord,error){return f.rows,f.err}
type analysisFixture struct{ rows []AnalyticsRecord; err error }
func(f analysisFixture)ListAnalytics(context.Context,time.Time,time.Time,int)([]AnalyticsRecord,error){return f.rows,f.err}
func options()Options{
    return Options{
        Start:time.Date(2026,11,16,11,0,0,0,time.UTC),
        End:time.Date(2026,11,16,12,0,0,0,time.UTC),
        Now:time.Date(2026,11,16,12,10,0,0,time.UTC),
        Grace:3*time.Minute,MaxRecords:100,
    }
}
func ledgerEntry(id string,publishedAt *time.Time)LedgerRecord{
    return LedgerRecord{EventID:id,TenantID:"tenant_001",ProjectID:"project_001",
        RequestID:"req-"+id,Model:"Qwen3-32B",PayloadHash:strings.Repeat("a",64),
        PromptTokens:100,CompletionTokens:20,HasOutbox:true,PublishedAt:publishedAt}
}
func analysis(id string)AnalyticsRecord{
    return AnalyticsRecord{EventID:id,TenantID:"tenant_001",ProjectID:"project_001",
        RequestID:"req-"+id,Model:"Qwen3-32B",PayloadHash:strings.Repeat("a",64),
        PromptTokens:100,CompletionTokens:20,RawCopies:1,HashVersions:1,PayloadVariants:1}
}
func TestRunClassifiesWithoutDoubleCountingTokens(t *testing.T){
    o:=options()
    old:=o.Now.Add(-10*time.Minute)
    recent:=o.Now.Add(-1*time.Minute)
    ledgers:=[]LedgerRecord{
        ledgerEntry("a", &old), ledgerEntry("b",&old),ledgerEntry("c",&old),
        ledgerEntry("d",&old),ledgerEntry("e",&old),ledgerEntry("f",nil),ledgerEntry("g",&recent),
    }
    views:=[]AnalyticsRecord{
        analysis("a"),analysis("b"),analysis("c"),analysis("e"),analysis("h"),
    }
    views[1].RawCopies=3
    views[2].HashVersions=2
    views[3].CompletionTokens=21 // even matching hash is not enough
    result,err:=Run(context.Background(),o,ledgerFixture{rows:ledgers},analysisFixture{rows:views})
    if err!=nil{t.Fatal(err)}
    expected:=map[string]Status{
        "a":StatusMatched, "b":StatusReplayed, "c":StatusConflict,
        "d":StatusMissing, "e":StatusMismatch,"f":StatusPending,
        "g":StatusInFlight,"h":StatusOrphan,
    }
    for _,f:=range result.Findings {
        want,ok:=expected[f.EventID]
        if !ok{t.Fatalf("unexpected event %s",f.EventID)}
        if f.Status!=want{t.Errorf("%s got %s want %s",f.EventID,f.Status,want)}
        delete(expected,f.EventID)
    }
    if len(expected)>0{t.Fatalf("unreported events: %+v",expected)}
    if result.Result!="FAIL"{t.Fatalf("unsafe report result=%s",result.Result)}
    if result.RawDuplicateCopies!=2{t.Fatalf("expected two extra physical copies, got %d",result.RawDuplicateCopies)}
    if result.LedgerEvents!=7||result.AnalyticsEvents!=5{t.Fatalf("wrong population sizes: %+v",result)}
}
func TestRunTreatsPendingAndGraceAsIncompleteNotLoss(t *testing.T){
    o:=options()
    recent:=o.Now.Add(-time.Minute)
    r,err:=Run(context.Background(),o,
        ledgerFixture{rows:[]LedgerRecord{ledgerEntry("unpublished",nil),ledgerEntry("recent",&recent)}},
        analysisFixture{})
    if err!=nil{t.Fatal(err)}
    if r.Result!="INCOMPLETE"{t.Fatalf("expected incomplete, got %s",r.Result)}
    if r.Counts[StatusMissing]!=0{t.Fatalf("prematurely called data lost: %+v",r.Counts)}
}
func TestRunRequiresFullBoundedScanAndUniqueIDs(t *testing.T){
    o:=options();o.MaxRecords=1
    _,err:=Run(context.Background(),o,ledgerFixture{rows:[]LedgerRecord{ledgerEntry("a",nil),ledgerEntry("b",nil)}},analysisFixture{})
    if err==nil||!strings.Contains(err.Error(),"limit"){t.Fatalf("truncated scan accepted: %v",err)}
    o.MaxRecords=10
    _,err=Run(context.Background(),o,ledgerFixture{},analysisFixture{rows:[]AnalyticsRecord{analysis("a"),analysis("a")}})
    if err==nil||!strings.Contains(err.Error(),"duplicate"){t.Fatalf("duplicate CH source accepted: %v",err)}
}
func TestMissingOutboxIsIntegrityViolation(t *testing.T){
    r:=ledgerEntry("orphan-ledger",nil);r.HasOutbox=false
    out,err:=Run(context.Background(),options(),ledgerFixture{rows:[]LedgerRecord{r}},analysisFixture{})
    if err!=nil{t.Fatal(err)}
    if out.Counts[StatusOutboxMissing]!=1||out.Result!="FAIL"{t.Fatalf("lost Outbox record not detected: %+v",out)}
}
func TestRejectInvalidOrUnboundedOptions(t *testing.T){
    invalid:=[]Options{ {}, options(),options(),options(),options() }
    invalid[1].End=invalid[1].Start
    invalid[2].MaxRecords=0
    invalid[3].Grace=-time.Second
    invalid[4].MaxRecords=100001
    for i,o:=range invalid {
        _,err:=Run(context.Background(),o,ledgerFixture{},analysisFixture{})
        if err==nil{t.Fatalf("invalid options %d accepted",i)}
    }
}
