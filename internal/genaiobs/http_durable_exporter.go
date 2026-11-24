package genaiobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type otlpJSONExportAttribute struct {
	Key string `json:"key"`
	Value map[string]string `json:"value"`
}

type otlpJSONExportSpan struct {
	TraceID string `json:"traceId"`
	SpanID string `json:"spanId"`
	Name string `json:"name"`
	Kind int `json:"kind"`
	Status struct {
		Code int `json:"code"`
	} `json:"status"`
	Attributes []otlpJSONExportAttribute `json:"attributes"`
}

type otlpJSONExportScope struct {
	Name string `json:"name"`
	Version string `json:"version"`
}

type otlpJSONExportScopeSpans struct {
	Scope otlpJSONExportScope `json:"scope"`
	Spans []otlpJSONExportSpan `json:"spans"`
}

type otlpJSONExportResourceSpans struct {
	ScopeSpans []otlpJSONExportScopeSpans `json:"scopeSpans"`
}

type otlpJSONExportRequest struct {
	ResourceSpans []otlpJSONExportResourceSpans `json:"resourceSpans"`
}

// HTTPDurableExporter sends one normalized observation to the authenticated
// OTLP/HTTP JSON receiver. Delivery keys are stable across worker retries.
type HTTPDurableExporter struct {
	endpoint string
	token string
	client *http.Client
}

// NewHTTPDurableExporter requires HTTPS for remote receivers. Plain HTTP is
// accepted only for loopback fault tests. Redirects are never followed.
func NewHTTPDurableExporter(endpoint,token string,client *http.Client)(*HTTPDurableExporter,error) {
	u,err:=url.Parse(endpoint)
	if err!=nil || u==nil || u.Hostname()=="" || u.User!=nil || u.RawQuery!="" || u.Fragment!="" || u.Opaque!="" || u.Path!="/v1/traces" {
		return nil,errors.New("invalid OTLP receiver endpoint")
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
	case "http":
		ip:=net.ParseIP(u.Hostname())
		if ip==nil || !ip.IsLoopback() {
			return nil,errors.New("plain HTTP receiver must use a loopback IP")
		}
	default:
		return nil,errors.New("OTLP receiver must use HTTPS")
	}
	if !validOTLPToken(token) {
		return nil,errors.New("invalid OTLP receiver credential")
	}
	var configured http.Client
	if client!=nil {
		configured=*client
	}
	if configured.Timeout<=0 {
		configured.Timeout=10*time.Second
	}
	configured.CheckRedirect=func(_ *http.Request,_ []*http.Request)error {
		return http.ErrUseLastResponse
	}
	return &HTTPDurableExporter{endpoint:endpoint,token:token,client:&configured},nil
}

func (e *HTTPDurableExporter) Export(ctx context.Context,d Delivery)error {
	if e==nil || e.client==nil || e.endpoint=="" {
		return Permanent(errors.New("HTTP durable exporter is not configured"))
	}
	if ctx==nil {
		return errors.New("context required")
	}
	if err:=ctx.Err();err!=nil {
		return err
	}
	if d.ID<1 || !validIdempotencyKey(d.IdempotencyKey()) {
		return Permanent(errors.New("invalid durable delivery identity"))
	}
	body,err:=encodeOTLPJSONObservation(d.Observation)
	if err!=nil {
		return Permanent(err)
	}
	req,err:=http.NewRequestWithContext(ctx,http.MethodPost,e.endpoint,bytes.NewReader(body))
	if err!=nil {
		return Permanent(errors.New("could not construct OTLP receiver request"))
	}
	req.Header.Set("Authorization","Bearer "+e.token)
	req.Header.Set("Content-Type","application/json")
	req.Header.Set("Accept","application/json")
	req.Header.Set("Idempotency-Key",d.IdempotencyKey())
	resp,err:=e.client.Do(req)
	if err!=nil {
		return fmt.Errorf("OTLP receiver request failed: %w",err)
	}
	defer resp.Body.Close()
	_,_ = io.Copy(io.Discard,io.LimitReader(resp.Body,4096))
	if resp.StatusCode>=200 && resp.StatusCode<300 {
		return nil
	}
	statusErr:=fmt.Errorf("OTLP receiver returned HTTP %d",resp.StatusCode)
	switch resp.StatusCode {
	case http.StatusRequestTimeout,http.StatusTooEarly,http.StatusTooManyRequests:
		return statusErr
	default:
		if resp.StatusCode>=500 {
			return statusErr
		}
		return Permanent(statusErr)
	}
}

func encodeOTLPJSONObservation(o Observation)([]byte,error) {
	if err:=validateObservation(o);err!=nil {
		return nil,err
	}
	span:=otlpJSONExportSpan{TraceID:o.TraceID,SpanID:o.SpanID,Name:"gen_ai."+o.Operation,Kind:1}
	switch o.Status {
	case "unset":
		span.Status.Code=0
	case "ok":
		span.Status.Code=1
	case "error":
		span.Status.Code=2
	default:
		return nil,ErrInvalid
	}
	addString:=func(key,value string) {
		if value!="" {
			span.Attributes=append(span.Attributes,otlpJSONExportAttribute{Key:key,Value:map[string]string{"stringValue":value}})
		}
	}
	addString("gen_ai.provider.name",o.Provider)
	addString("gen_ai.operation.name",o.Operation)
	addString("gen_ai.request.model",o.RequestModel)
	addString("gen_ai.response.model",o.ResponseModel)
	addString("llm_telemetry.request_id",o.RequestID)
	addString("llm_telemetry.attempt_id",o.AttemptID)
	if o.InputTokens!=nil {
		span.Attributes=append(span.Attributes,otlpJSONExportAttribute{Key:"gen_ai.usage.input_tokens",Value:map[string]string{"intValue":strconv.FormatInt(*o.InputTokens,10)}})
	}
	if o.OutputTokens!=nil {
		span.Attributes=append(span.Attributes,otlpJSONExportAttribute{Key:"gen_ai.usage.output_tokens",Value:map[string]string{"intValue":strconv.FormatInt(*o.OutputTokens,10)}})
	}
	request:=otlpJSONExportRequest{
		ResourceSpans:[]otlpJSONExportResourceSpans{{
			ScopeSpans:[]otlpJSONExportScopeSpans{{
				Scope:otlpJSONExportScope{Name:"llm-telemetry-pipeline",Version:Version},
				Spans:[]otlpJSONExportSpan{span},
			}},
		}},
	}
	return json.Marshal(request)
}
