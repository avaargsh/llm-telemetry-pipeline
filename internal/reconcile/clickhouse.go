package reconcile

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "strconv"
    "strings"
    "time"
)

// ClickHouseHTTP groups all visible raw records in the requested event-time
// window, including conflicting copies that the dedup view excludes.
type ClickHouseHTTP struct {
    Endpoint string
    Database string
    User string
    Password string
    Client *http.Client
}

type analyticsRow struct{
    EventID string `json:"event_id"`
    TenantID string `json:"sample_tenant_id"`
    ProjectID string `json:"sample_project_id"`
    RequestID string `json:"sample_request_id"`
    Model string `json:"sample_model"`
    PayloadHash string `json:"canonical_hash"`
    PromptTokens string `json:"min_prompt_tokens"`
    CompletionTokens string `json:"min_completion_tokens"`
    RawCopies string `json:"raw_copies"`
    HashVersions string `json:"hash_versions"`
    PayloadVariants string `json:"payload_variants"`
    MissingHashRows string `json:"missing_hash_rows"`
}
func parseUnsigned(field,value string)(uint64,error){
    n,err:=strconv.ParseUint(value,10,64)
    if err!=nil{return 0,fmt.Errorf("invalid %s value %q: %w",field,value,err)}
    return n,nil
}
func(a ClickHouseHTTP)ListAnalytics(ctx context.Context,start,end time.Time,max int)([]AnalyticsRecord,error){
    endpoint,err:=url.Parse(a.Endpoint)
    if err!=nil||endpoint==nil||endpoint.Host==""||(endpoint.Scheme!="http"&&endpoint.Scheme!="https"){
        return nil,fmt.Errorf("invalid ClickHouse HTTP endpoint")
    }
    q:=endpoint.Query()
    db:=a.Database
    if db==""{db="telemetry"}
    q.Set("database",db)
    endpoint.RawQuery=q.Encode()
    // Use event-time bounds and bounded result rows, never an unscoped
    // global COUNT or MAX. The two sources are read sequentially: this is
    // a diagnostic comparison, not a cross-database transaction/snapshot.
    query:=fmt.Sprintf(`
SELECT
    toString(event_id) AS event_id,
    any(tenant_id) AS sample_tenant_id,
    any(project_id) AS sample_project_id,
    any(request_id) AS sample_request_id,
    any(model) AS sample_model,
    toString(min(payload_hash)) AS canonical_hash,
    toString(min(prompt_tokens)) AS min_prompt_tokens,
    toString(min(completion_tokens)) AS min_completion_tokens,
    toString(count()) AS raw_copies,
    toString(uniqExact(payload_hash)) AS hash_versions,
    toString(uniqExact(tuple(tenant_id,project_id,request_id,model,
       prompt_tokens,completion_tokens,latency_ms,status,event_time))) AS payload_variants,
    toString(countIf(payload_hash = toFixedString('',64))) AS missing_hash_rows
FROM inference_events_raw
WHERE event_time >= fromUnixTimestamp64Milli(%d)
  AND event_time < fromUnixTimestamp64Milli(%d)
GROUP BY event_id
ORDER BY event_id LIMIT %d FORMAT JSONEachRow
`,start.UTC().UnixMilli(),end.UTC().UnixMilli(),max+1)
    req,err:=http.NewRequestWithContext(ctx,http.MethodPost,endpoint.String(),strings.NewReader(query))
    if err!=nil{return nil,err}
    if a.User!=""{req.SetBasicAuth(a.User,a.Password)}
    client:=a.Client
    if client==nil{client=&http.Client{Timeout:45*time.Second}}
    resp,err:=client.Do(req)
    if err!=nil{return nil,fmt.Errorf("ClickHouse query: %w",err)}
    defer resp.Body.Close()
    if resp.StatusCode!=http.StatusOK{
        body,_:=io.ReadAll(io.LimitReader(resp.Body,4096))
        return nil,fmt.Errorf("ClickHouse query status %d: %s",resp.StatusCode,strings.TrimSpace(string(body)))
    }
    decoder:=json.NewDecoder(io.LimitReader(resp.Body,64<<20))
    result:=make([]AnalyticsRecord,0)
    for {
        var r analyticsRow
        if err:=decoder.Decode(&r);err!=nil{
            if err==io.EOF{break}
            return nil,fmt.Errorf("decode ClickHouse JSONEachRow: %w",err)
        }
        counts:=[]struct{name,value string}{
            {"min_prompt_tokens",r.PromptTokens},{"min_completion_tokens",r.CompletionTokens},
            {"raw_copies",r.RawCopies},{"hash_versions",r.HashVersions},
            {"payload_variants",r.PayloadVariants},{"missing_hash_rows",r.MissingHashRows},
        }
        values:=make([]uint64,len(counts))
        for i,v:=range counts{
            n,err:=parseUnsigned(v.name,v.value)
            if err!=nil{return nil,err}
            values[i]=n
        }
        result=append(result,AnalyticsRecord{
            EventID:r.EventID,TenantID:r.TenantID,ProjectID:r.ProjectID,
            RequestID:r.RequestID,Model:r.Model,PayloadHash:r.PayloadHash,
            PromptTokens:values[0],CompletionTokens:values[1],
            RawCopies:values[2],HashVersions:values[3],
            PayloadVariants:values[4],MissingHashRows:values[5],
        })
        if len(result)>max{return nil,fmt.Errorf("ClickHouse analytics exceeds max-records limit %d",max)}
    }
    return result,nil
}
