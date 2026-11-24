package ingestapi

import (
    "bytes"
    "context"
    "encoding/json"
    "errors"
    "net/http"
    "net/http/httptest"
    "strings"
    "testing"
    "time"

    "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
    "github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
)

const testToken = "4f0c1537b9464417a51d3a61d738aa277b65299b4c9354b3b91c5c32288187fe"
const otherToken = "b1be7d48858e9c9b90e05aa1c8395525c29cbb0ef76c56f94700364ccde882cf"

type fakeRecorder struct {
    calls int
    last event.UsageEvent
    outcomes []ledger.Outcome
    err error
}
func (f *fakeRecorder) Record(_ context.Context, e event.UsageEvent)(ledger.Outcome,error){
    f.calls++;f.last=e
    if f.err!=nil{return "",f.err}
    if len(f.outcomes)==0{return ledger.Created,nil}
    result:=f.outcomes[0];f.outcomes=f.outcomes[1:];return result,nil
}
func sampleEvent(t *testing.T) event.UsageEvent{
    t.Helper()
    e,err:=event.SyntheticUsageEvent("m5a-api",0,time.Date(2026,11,21,10,0,0,0,time.UTC))
    if err!=nil{t.Fatal(err)}
    return e
}
func newTestHandler(t *testing.T,s *fakeRecorder)http.Handler{
    t.Helper()
    h,err:=NewHandler(s,[]Credential{{Token:testToken,TenantID:"tenant_001",ProjectID:"project_001"},{Token:otherToken,TenantID:"tenant_002",ProjectID:"project_002"}})
    if err!=nil{t.Fatal(err)}
    return h
}
func send(t *testing.T,h http.Handler,raw []byte,token string,method string) *httptest.ResponseRecorder{
    t.Helper()
    req:=httptest.NewRequest(method,"/v1/usage-events",bytes.NewReader(raw))
    req.Header.Set("Content-Type","application/json")
    if token!=""{req.Header.Set("Authorization","Bearer "+token)}
    rr:=httptest.NewRecorder();h.ServeHTTP(rr,req);return rr
}
func toJSON(t *testing.T,e event.UsageEvent)[]byte{t.Helper();b,err:=json.Marshal(e);if err!=nil{t.Fatal(err)};return b}

func TestIngestPersistsThenReportsExistingIdentity(t *testing.T){
    s:=&fakeRecorder{outcomes:[]ledger.Outcome{ledger.Created,ledger.Duplicate}}
    h:=newTestHandler(t,s);e:=sampleEvent(t);body:=toJSON(t,e)
    for _,tc:=range []struct{status int;outcome string}{{http.StatusCreated,"created"},{http.StatusOK,"duplicate"}}{
        rr:=send(t,h,body,testToken,http.MethodPost)
        if rr.Code!=tc.status{t.Fatalf("got status %d body %s, want %d",rr.Code,rr.Body.String(),tc.status)}
        if !strings.Contains(rr.Body.String(),`"result":"`+tc.outcome+`"`){t.Fatalf("response=%s",rr.Body.String())}
        if rr.Header().Get("Cache-Control")!="no-store"{t.Fatalf("missing no-store")}
    }
    if s.calls!=2||s.last.EventID!=e.EventID{t.Fatalf("recorder calls=%d event=%s",s.calls,s.last.EventID)}
}
func TestIngestRejectsUnauthenticatedAndCrossTenantWrites(t *testing.T){
    e:=sampleEvent(t);raw:=toJSON(t,e)
    for _,tc:=range []struct{name,token string;want int}{
        {"missing","",http.StatusUnauthorized},
        {"invalid","not-a-secret",http.StatusUnauthorized},
        {"other-tenant",otherToken,http.StatusForbidden},
    }{
        t.Run(tc.name,func(t *testing.T){s:=&fakeRecorder{};rr:=send(t,newTestHandler(t,s),raw,tc.token,http.MethodPost);if rr.Code!=tc.want{t.Fatalf("status %d, want %d",rr.Code,tc.want)};if s.calls!=0{t.Fatalf("unauthorized record called")}})
    }
    t.Run("same-tenant-different-project",func(t *testing.T){
        s:=&fakeRecorder{};h,err:=NewHandler(s,[]Credential{{Token:testToken,TenantID:e.TenantID,ProjectID:"another-project"}});if err!=nil{t.Fatal(err)}
        rr:=send(t,h,raw,testToken,http.MethodPost)
        if rr.Code!=http.StatusForbidden||s.calls!=0{t.Fatalf("status=%d calls=%d",rr.Code,s.calls)}
    })
}
func TestIngestRejectsMalformedOversizedOrInvalidEvents(t *testing.T){
    e:=sampleEvent(t)
    tampered:=e;tampered.CompletionTokens++
    for _,tc:=range []struct{name string;raw []byte;want int}{
        {"tampered",toJSON(t,tampered),400},
        {"unknown-field",append(bytes.TrimSuffix(toJSON(t,e),[]byte("}")),[]byte(`,"undeclared":true}`)...),400},
        {"trailing-json",append(toJSON(t,e),[]byte(` {}`)...),400},
        {"body-too-large",append(toJSON(t,e),[]byte(strings.Repeat(" ",MaxBodyBytes))...),413},
        {"invalid-json",[]byte("{"),400},
    }{
        t.Run(tc.name,func(t *testing.T){s:=&fakeRecorder{};rr:=send(t,newTestHandler(t,s),tc.raw,testToken,http.MethodPost);if rr.Code!=tc.want||s.calls!=0{t.Fatalf("status=%d calls=%d body=%s",rr.Code,s.calls,rr.Body.String())}})
    }
}
func TestIngestConflictAndUncertainDatabaseOutcome(t *testing.T){
    e:=sampleEvent(t)
    for _,tc:=range []struct{name string;err error;want int}{
        {"conflict",ledger.ErrConflict,409},
        {"unknown-commit",errors.New("commit uncertain"),503},
    }{
        t.Run(tc.name,func(t *testing.T){s:=&fakeRecorder{err:tc.err};rr:=send(t,newTestHandler(t,s),toJSON(t,e),testToken,http.MethodPost);if rr.Code!=tc.want||s.calls!=1{t.Fatalf("status=%d calls=%d",rr.Code,s.calls)};if strings.Contains(rr.Body.String(),tc.err.Error()){t.Fatal("internal database error leaked")}})
    }
}
func TestIngestLimitsMethodsContentTypeAndAuthorizationHeaders(t *testing.T){
    s:=&fakeRecorder{};h:=newTestHandler(t,s);e:=sampleEvent(t);raw:=toJSON(t,e)
    rr:=send(t,h,raw,testToken,http.MethodGet);if rr.Code!=405{t.Fatalf("GET=%d",rr.Code)}
    req:=httptest.NewRequest(http.MethodPost,"/v1/usage-events",bytes.NewReader(raw));req.Header.Set("Authorization","Bearer "+testToken);req.Header.Set("Content-Type","text/plain")
    rr=httptest.NewRecorder();h.ServeHTTP(rr,req);if rr.Code!=415{t.Fatalf("content-type=%d",rr.Code)}
    req=httptest.NewRequest(http.MethodPost,"/v1/usage-events",bytes.NewReader(raw));req.Header.Add("Authorization","Bearer "+testToken);req.Header.Add("Authorization","Bearer "+otherToken);req.Header.Set("Content-Type","application/json")
    rr=httptest.NewRecorder();h.ServeHTTP(rr,req);if rr.Code!=401{t.Fatalf("ambiguous auth=%d",rr.Code)}
    if s.calls!=0{t.Fatalf("unexpected %d records",s.calls)}
    req=httptest.NewRequest(http.MethodGet,"/healthz",nil);rr=httptest.NewRecorder();h.ServeHTTP(rr,req);if rr.Code!=200{t.Fatalf("healthz=%d",rr.Code)}
}
func TestNewHandlerRejectsWeakOrDuplicateCredentials(t *testing.T){
    f:=&fakeRecorder{}
    for _,cfg:=range [][]Credential{
        nil,
        {{Token:"short",TenantID:"tenant_001",ProjectID:"project_001"}},
        {{Token:testToken,TenantID:"",ProjectID:"project_001"}},
        {{Token:testToken,TenantID:"tenant_001",ProjectID:"project_001"},{Token:testToken,TenantID:"tenant_002",ProjectID:"project_002"}},
    }{
        if _,err:=NewHandler(f,cfg);err==nil{t.Fatalf("accepted invalid credentials: %+v",cfg)}
    }
    if _,err:=NewHandler(nil,[]Credential{{Token:testToken,TenantID:"tenant_001",ProjectID:"project_001"}});err==nil{t.Fatal("accepted nil store")}
}


func TestIngestRejectsAmbiguousJSONFields(t *testing.T) {
    e := sampleEvent(t)
    raw := toJSON(t, e)
    for _, tc := range []struct{name, prefix string}{
        {"duplicate-tenant", `"tenant_id":"other-tenant",`},
        {"duplicate-token-count", `"prompt_tokens":999999,`},
        {"case-folded-alias", `"TENANT_ID":"other-tenant",`},
        {"escaped-key-alias", `"tenant_\u0069d":"other-tenant",`},
        {"same-value-duplicate", `"tenant_id":"tenant_001",`},
    } {
        t.Run(tc.name,func(t *testing.T){
            s := &fakeRecorder{}
            body := append([]byte("{"+tc.prefix),raw[1:]...)
            rr := send(t,newTestHandler(t,s),body,testToken,http.MethodPost)
            if rr.Code != http.StatusBadRequest || s.calls != 0 {
                t.Fatalf("ambiguous input accepted: status=%d calls=%d body=%s",rr.Code,s.calls,rr.Body.String())
            }
        })
    }
    s := &fakeRecorder{}
    rr := send(t,newTestHandler(t,s),append([]byte(" \n"),raw...),testToken,http.MethodPost)
    if rr.Code != http.StatusCreated || s.calls != 1 {
        t.Fatalf("unambiguous input rejected: status=%d calls=%d",rr.Code,s.calls)
    }
}


func TestIngestRequiresExplicitV1Fields(t *testing.T) {
    e := sampleEvent(t)
    e.PromptTokens, e.CompletionTokens, e.LatencyMS = 0, 0, 0
    if err := e.Seal(); err != nil { t.Fatal(err) }
    raw := toJSON(t, e)
    for _, tc := range []struct{name, old, replacement string}{
        {"case-only-alias", `"tenant_id"`, `"TENANT_ID"`},
        {"null-input", `"prompt_tokens":0`, `"prompt_tokens":null`},
        {"missing-input", `"prompt_tokens":0,`, ""},
        {"null-output", `"completion_tokens":0`, `"completion_tokens":null`},
        {"missing-output", `"completion_tokens":0,`, ""},
        {"null-latency", `"latency_ms":0`, `"latency_ms":null`},
        {"missing-latency", `"latency_ms":0,`, ""},
    } {
        t.Run(tc.name, func(t *testing.T) {
            body := bytes.Replace(raw, []byte(tc.old), []byte(tc.replacement), 1)
            if bytes.Equal(body, raw) { t.Fatal("fixture did not mutate") }
            s := &fakeRecorder{}
            rr := send(t, newTestHandler(t, s), body, testToken, http.MethodPost)
            if rr.Code != http.StatusBadRequest || s.calls != 0 {
                t.Fatalf("status=%d calls=%d", rr.Code, s.calls)
            }
        })
    }
    s := &fakeRecorder{}
    rr := send(t, newTestHandler(t, s), raw, testToken, http.MethodPost)
    if rr.Code != http.StatusCreated || s.calls != 1 {
        t.Fatalf("explicit zero usage rejected: status=%d calls=%d", rr.Code, s.calls)
    }
}
