package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/avaargsh/llm-telemetry-pipeline/internal/finops"
	"github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type scopedHistoricalOutput struct {
	TenantID string `json:"tenant_id"`
	ProjectID string `json:"project_id"`
	PriceRegistryVerified bool `json:"price_registry_verified,omitempty"`
	finops.HistoricalEstimate
}

func runHistorical(path, fromText, toText string,bindings priceBindings,verifyRegistry bool,out io.Writer) error {
	from,err := time.Parse(time.RFC3339Nano,fromText)
	if err != nil { return errors.New("invalid historical time window") }
	to,err := time.Parse(time.RFC3339Nano,toText)
	if err != nil { return errors.New("invalid historical time window") }
	var periods []finops.PricePeriod
	if err := readJSON(path,&periods);err != nil || periods==nil {
		return errors.New("invalid schedule file: expected a non-null price period array")
	}
	if _,err := finops.PlanPriceWindows(from,to,periods);err != nil { return err }
	if bindings != nil {
		for _, period := range periods {
			estimate,err := finops.Estimate(nil,period.Table)
			if err != nil { return err }
			if err := bindings.verify(estimate); err != nil { return err }
		}
	}
	dsn := os.Getenv("USAGE_QUERY_DATABASE_URL")
	if dsn=="" { return errors.New("USAGE_QUERY_DATABASE_URL is required") }
	tenant,project := os.Getenv("USAGE_QUERY_TENANT_ID"),os.Getenv("USAGE_QUERY_PROJECT_ID")
	if strings.TrimSpace(tenant)=="" || strings.TrimSpace(project)=="" { return errors.New("USAGE_QUERY_TENANT_ID and USAGE_QUERY_PROJECT_ID are required") }
	ctx,stop := signal.NotifyContext(context.Background(),syscall.SIGINT,syscall.SIGTERM)
	defer stop()
	ctx,cancel := context.WithTimeout(ctx,15*time.Second)
	defer cancel()
	db,err := sql.Open("pgx",dsn)
	if err != nil { return errors.New("open query database failed") }
	defer db.Close()
	db.SetMaxOpenConns(1)
	reader,err := ledger.NewScopedUsageReader(ctx,db,tenant,project)
	if err != nil { return errors.New("query database scope readiness failed") }
	var estimate finops.HistoricalEstimate
	if verifyRegistry {
		estimate,err = finops.EstimateVerifiedHistory(ctx,reader,tenant,project,from,to,periods)
	} else {
		estimate,err = finops.EstimateScopedHistory(ctx,reader,tenant,project,from,to,periods)
	}
	if err != nil {
		if errors.Is(err,ledger.ErrPriceVersionNotFound) || errors.Is(err,ledger.ErrPriceVersionMismatch) || errors.Is(err,ledger.ErrInvalidPriceExpectation) { return err }
		if errors.Is(err,finops.ErrMissingRate) || errors.Is(err,finops.ErrInvalidEstimate) || errors.Is(err,finops.ErrUsageWindowMismatch) { return err }
		return errors.New("historical ledger query failed")
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("","  ")
	return enc.Encode(scopedHistoricalOutput{TenantID:tenant,ProjectID:project,PriceRegistryVerified:verifyRegistry,HistoricalEstimate:estimate})
}
