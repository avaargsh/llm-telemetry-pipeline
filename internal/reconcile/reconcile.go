// Package reconcile compares bounded snapshots of the authoritative
// PostgreSQL usage ledger with the replayable ClickHouse analytical projection.
// It does not perform charging, offset commits, or automatic data repair.
package reconcile

import (
    "context"
    "errors"
    "fmt"
    "sort"
    "time"
)

type Status string
const (
    StatusMatched Status = "matched"
    StatusReplayed Status = "replayed"
    StatusPending Status = "pending"
    StatusInFlight Status = "in_flight"
    StatusMissing Status = "missing"
    StatusOrphan Status = "orphan"
    StatusConflict Status = "conflict"
    StatusMismatch Status = "mismatch"
    StatusOutboxMissing Status = "outbox_missing"
)

// LedgerRecord is read from PostgreSQL, which is authoritative for billing.
type LedgerRecord struct {
    EventID string
    TenantID string
    ProjectID string
    RequestID string
    Model string
    PayloadHash string
    PromptTokens int64
    CompletionTokens int64
    HasOutbox bool
    PublishedAt *time.Time
}

// AnalyticsRecord summarizes all raw ClickHouse copies of an Event ID.
type AnalyticsRecord struct {
    EventID string
    TenantID string
    ProjectID string
    RequestID string
    Model string
    PayloadHash string
    PromptTokens uint64
    CompletionTokens uint64
    RawCopies uint64
    HashVersions uint64
    PayloadVariants uint64
    MissingHashRows uint64
}
type LedgerSource interface {
    ListLedger(context.Context,time.Time,time.Time,int)([]LedgerRecord,error)
}
type AnalyticsSource interface {
    ListAnalytics(context.Context,time.Time,time.Time,int)([]AnalyticsRecord,error)
}
type Options struct {
    Start time.Time `json:"start"`
    End time.Time `json:"end"`
    Now time.Time `json:"evaluated_at"`
    Grace time.Duration `json:"-"`
    MaxRecords int `json:"max_records"`
}
type Finding struct {
    EventID string `json:"event_id"`
    Status Status `json:"status"`
    Reason string `json:"reason,omitempty"`
    RawCopies uint64 `json:"raw_copies,omitempty"`
    LedgerTokens int64 `json:"ledger_tokens,omitempty"`
    AnalyticsTokens uint64 `json:"analytics_tokens,omitempty"`
}
type Report struct {
    SchemaVersion int `json:"schema_version"`
    Start time.Time `json:"start"`
    End time.Time `json:"end"`
    EvaluatedAt time.Time `json:"evaluated_at"`
    GraceSeconds int64 `json:"grace_seconds"`
    MaxRecords int `json:"max_records"`
    LedgerEvents int `json:"ledger_events"`
    AnalyticsEvents int `json:"analytics_events"`
    RawDuplicateCopies uint64 `json:"raw_duplicate_copies"`
    Counts map[Status]int `json:"counts"`
    Result string `json:"result"`
    Findings []Finding `json:"findings"`
}

func validate(o Options)error{
    if o.Start.IsZero()||o.End.IsZero()||o.Now.IsZero()||!o.Start.Before(o.End){
        return errors.New("start/end/now required and start must precede end")
    }
    if o.Grace<0{return errors.New("grace must be nonnegative")}
    if o.MaxRecords<1||o.MaxRecords>10000{return errors.New("max-records must be between 1 and 10000")}
    return nil
}

func Run(ctx context.Context,o Options,ledger LedgerSource,analytics AnalyticsSource)(Report,error){
    if err:=validate(o);err!=nil{return Report{},err}
    if ledger==nil||analytics==nil{return Report{},errors.New("both sources required")}
    lr,err:=ledger.ListLedger(ctx,o.Start,o.End,o.MaxRecords)
    if err!=nil{return Report{},fmt.Errorf("read PostgreSQL ledger: %w",err)}
    if len(lr)>o.MaxRecords{return Report{},fmt.Errorf("ledger exceeds max-records limit %d; narrow window",o.MaxRecords)}
    ar,err:=analytics.ListAnalytics(ctx,o.Start,o.End,o.MaxRecords)
    if err!=nil{return Report{},fmt.Errorf("read ClickHouse analytics: %w",err)}
    if len(ar)>o.MaxRecords{return Report{},fmt.Errorf("analytics exceeds max-records limit %d; narrow window",o.MaxRecords)}
    ledgers:=make(map[string]LedgerRecord,len(lr))
    for _,l:=range lr{
        if l.EventID==""{return Report{},errors.New("ledger returned empty event_id")}
        if _,found:=ledgers[l.EventID];found{return Report{},fmt.Errorf("duplicate ledger source event_id %s",l.EventID)}
        if l.PromptTokens<0||l.CompletionTokens<0{return Report{},fmt.Errorf("negative ledger tokens for %s",l.EventID)}
        ledgers[l.EventID]=l
    }
    views:=make(map[string]AnalyticsRecord,len(ar))
    for _,a:=range ar{
        if a.EventID==""{return Report{},errors.New("analytics returned empty event_id")}
        if _,found:=views[a.EventID];found{return Report{},fmt.Errorf("duplicate analytics source event_id %s",a.EventID)}
        if a.RawCopies==0{return Report{},fmt.Errorf("analytics returned zero raw copies for %s",a.EventID)}
        views[a.EventID]=a
    }
    keys:=make([]string,0,len(lr)+len(ar))
    for id:=range ledgers{keys=append(keys,id)}
    for id:=range views{if _,ok:=ledgers[id];!ok{keys=append(keys,id)}}
    sort.Strings(keys)
    report:=Report{
        SchemaVersion:1,Start:o.Start.UTC(),End:o.End.UTC(),
        EvaluatedAt:o.Now.UTC(),GraceSeconds:int64(o.Grace/time.Second),
        MaxRecords:o.MaxRecords,LedgerEvents:len(lr),AnalyticsEvents:len(ar),
        Counts:make(map[Status]int),Findings:make([]Finding,0,len(keys)),
    }
    critical,pending:=false,false
    for _,id:=range keys {
        l,hasLedger:=ledgers[id]
        a,hasAnalysis:=views[id]
        finding:=Finding{EventID:id}
        if hasLedger{finding.LedgerTokens=l.PromptTokens+l.CompletionTokens}
        if hasAnalysis{
            finding.RawCopies=a.RawCopies
            finding.AnalyticsTokens=a.PromptTokens+a.CompletionTokens
            report.RawDuplicateCopies+=a.RawCopies-1
        }
        switch {
        case !hasLedger:
            finding.Status=StatusOrphan
            finding.Reason="ClickHouse event has no authoritative ledger record in this window"
        case !l.HasOutbox:
            finding.Status=StatusOutboxMissing
            finding.Reason="ledger event is missing its transactional outbox record"
        case !hasAnalysis:
            switch {
            case l.PublishedAt==nil:
                finding.Status=StatusPending
                finding.Reason="outbox has not been confirmed as published"
            case o.Now.Sub(*l.PublishedAt)<o.Grace:
                finding.Status=StatusInFlight
                finding.Reason="published recently; delivery grace window not elapsed"
            default:
                finding.Status=StatusMissing
                finding.Reason="published but no ClickHouse event after grace window"
            }
        case a.HashVersions!=1||a.MissingHashRows>0||a.PayloadVariants!=1||a.PayloadHash!=l.PayloadHash:
            finding.Status=StatusConflict
            finding.Reason="raw payload hashes or physical event variants disagree with the ledger"
        case a.TenantID!=l.TenantID||a.ProjectID!=l.ProjectID||
            a.RequestID!=l.RequestID||a.Model!=l.Model||
            a.PromptTokens!=uint64(l.PromptTokens)||a.CompletionTokens!=uint64(l.CompletionTokens):
            finding.Status=StatusMismatch
            finding.Reason="ClickHouse event metadata or Token values differ from authoritative ledger"
        case a.RawCopies>1:
            finding.Status=StatusReplayed
            finding.Reason="identical physical replay copies count once in trusted analytics"
        default:
            finding.Status=StatusMatched
        }
        report.Counts[finding.Status]++
        switch finding.Status {
        case StatusMissing,StatusOrphan,StatusConflict,StatusMismatch,StatusOutboxMissing:
            critical=true
        case StatusPending,StatusInFlight:
            pending=true
        }
        report.Findings=append(report.Findings,finding)
    }
    report.Result="PASS"
    if pending{report.Result="INCOMPLETE"}
    if critical{report.Result="FAIL"}
    return report,nil
}
