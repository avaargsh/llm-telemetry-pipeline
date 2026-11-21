// pgha-probe is a disposable two-node PostgreSQL physical replication lab.
// It does not elect a leader, route customer traffic or claim production HA.
package main

import (
 "context"
 "database/sql"
 "encoding/json"
 "errors"
 "flag"
 "fmt"
 "os"
 "path/filepath"
 "regexp"
 "time"

 "github.com/avaargsh/llm-telemetry-pipeline/internal/event"
 "github.com/avaargsh/llm-telemetry-pipeline/internal/ledger"
 "github.com/avaargsh/llm-telemetry-pipeline/internal/outbox"
 _ "github.com/jackc/pgx/v5/stdlib"
)

const committedCount=12
var safeSeed=regexp.MustCompile(`^[a-zA-Z0-9_-]{3,70}$`)
var seedStart=time.Date(2026,11,16,11,0,0,0,time.UTC)

type failoverReceipt struct {
 SchemaVersion int `json:"schema_version"`
 HeadSHA string `json:"head_sha"`
 Mode string `json:"mode"`
 CommittedBefore int `json:"committed_before"`
 ReplayedOnStandby int `json:"replayed_on_standby"`
 PromotedLedger int `json:"promoted_ledger"`
 PromotedOutbox int `json:"promoted_outbox"`
 UncommittedOldPrimaryRolledBack bool `json:"uncommitted_old_primary_rolled_back"`
 StableIDRetryCreated bool `json:"stable_id_retry_created"`
 ConfirmedBeforeRetriedAsDuplicate int `json:"confirmed_before_retried_as_duplicate"`
 StaleLeaseFenced bool `json:"stale_lease_fenced"`
 OldPrimaryRemainedStopped bool `json:"old_primary_remained_stopped"`
 PublishedWithoutKafkaACK int `json:"published_without_kafka_ack"`
 Result string `json:"result"`
}
func requireRole(phase string,recovery bool) error {
 want:=false
 switch phase {
 case "primary","promoted":want=false
 case "standby":want=true
 default:return fmt.Errorf("unexpected PostgreSQL phase")
 }
 if recovery!=want {return fmt.Errorf("phase %s unexpected pg_is_in_recovery=%t",phase,recovery)}
 return nil
}
func requireAtomicCounts(actualLedger,actualOutbox,expected int) error{
 if expected<1||actualLedger!=expected||actualOutbox!=expected {
  return fmt.Errorf("ledger=%d outbox=%d want both=%d",actualLedger,actualOutbox,expected)
 }
 return nil
}
func validateReceipt(e failoverReceipt)error{
 if e.SchemaVersion!=1||e.Mode!="manual_synchronous_pg17"||
    e.CommittedBefore!=committedCount||e.ReplayedOnStandby!=committedCount||
    e.PromotedLedger!=committedCount+1||e.PromotedOutbox!=committedCount+1||
    !e.UncommittedOldPrimaryRolledBack||!e.StableIDRetryCreated||
    e.ConfirmedBeforeRetriedAsDuplicate!=committedCount||!e.StaleLeaseFenced||
    !e.OldPrimaryRemainedStopped||e.PublishedWithoutKafkaACK!=0{
    return errors.New("incomplete PGHA failover proof; never classify as PASS")
 }
 return nil
}
func eventAt(seed string,i int)(event.UsageEvent,error){
 return event.SyntheticUsageEvent(seed,i,seedStart)
}
func counts(ctx context.Context,db *sql.DB,seed string)(int,int,error){
 pattern:=seed+"_req_%"
 var l,o int
 if err:=db.QueryRowContext(ctx,"SELECT count(*) FROM usage_ledger WHERE request_id LIKE $1",pattern).Scan(&l);err!=nil{return 0,0,err}
 if err:=db.QueryRowContext(ctx,`SELECT count(*) FROM event_outbox e JOIN usage_ledger l ON l.event_id=e.event_id
 WHERE l.request_id LIKE $1`,pattern).Scan(&o);err!=nil{return 0,0,err}
 return l,o,nil
}
func verifyBase(ctx context.Context,db *sql.DB,seed string)error{
 l,o,err:=counts(ctx,db,seed);if err!=nil{return err}
 if err:=requireAtomicCounts(l,o,committedCount);err!=nil{return err}
 for i:=0;i<committedCount;i++{
  e,err:=eventAt(seed,i);if err!=nil{return err}
  var ledgerHash,rawHash string
  err=db.QueryRowContext(ctx,`SELECT l.payload_hash::text,e.event_payload->>'payload_hash'
 FROM usage_ledger l JOIN event_outbox e ON e.event_id=l.event_id
 WHERE l.event_id=$1::uuid`,e.EventID).Scan(&ledgerHash,&rawHash)
  if err!=nil{return fmt.Errorf("verify EventID index=%d: %w",i,err)}
  if ledgerHash!=e.PayloadHash||rawHash!=e.PayloadHash{return fmt.Errorf("source hash mismatch for EventID index=%d",i)}
 }
 return nil
}
func isRecovery(ctx context.Context,db *sql.DB)(bool,error){
 var v bool
 err:=db.QueryRowContext(ctx,"SELECT pg_is_in_recovery()").Scan(&v)
 return v,err
}
func run()error{
 phase:=flag.String("phase","","seed|standby|hold|promoted")
 seed:=flag.String("seed","","unique test seed")
 dsn:=flag.String("dsn",os.Getenv("TEST_PGHA_DSN"),"lab PostgreSQL DSN")
 marker:=flag.String("hold-ready","","file marker indicating uncommitted transaction is holding")
 evidence:=flag.String("evidence","","redacted final verification JSON")
 oldStopped:=flag.Bool("old-primary-stopped",false,"validated by runner when old primary remains stopped")
 flag.Parse()
 if !safeSeed.MatchString(*seed)||*dsn==""{return errors.New("safe seed and TEST_PGHA_DSN are required")}
 if *phase!="seed"&&*phase!="standby"&&*phase!="hold"&&*phase!="promoted"{return errors.New("unknown phase")}
 db,err:=sql.Open("pgx",*dsn);if err!=nil{return err};defer db.Close()
 ctx,cancel:=context.WithTimeout(context.Background(),15*time.Second);defer cancel()
 if err:=db.PingContext(ctx);err!=nil{return err}
 recovering,err:=isRecovery(ctx,db);if err!=nil{return err}
 requestedRole:=*phase
 if requestedRole=="seed"||requestedRole=="hold"{requestedRole="primary"}
 if err:=requireRole(requestedRole,recovering);err!=nil{return err}
 store:=&ledger.Store{DB:db}
 switch *phase {
 case "seed":
  for i:=0;i<committedCount;i++{
   e,err:=eventAt(*seed,i);if err!=nil{return err}
   status,err:=store.Record(ctx,e)
   if err!=nil||status!=ledger.Created{return fmt.Errorf("seed create %d status=%s: %w",i,status,err)}
  }
  if err:=verifyBase(ctx,db,*seed);err!=nil{return err}
  first,_:=eventAt(*seed,0)
  status,err:=store.Record(ctx,first)
  if err!=nil||status!=ledger.Duplicate{return fmt.Errorf("seed duplicate retry status=%s error=%v",status,err)}
  rows,err:=(&outbox.Store{DB:db}).Claim(ctx,"pgha-stale-worker",10*time.Minute,1)
  if err!=nil||len(rows)!=1{return fmt.Errorf("claim one durable lease: %v rows=%d",err,len(rows))}
  fmt.Println("PASS pre-fault: 12 remote-apply ledger/outbox commits; one stale lease generation")
 case "standby":
  if err:=verifyBase(ctx,db,*seed);err!=nil{return err}
  fmt.Println("PASS physical standby: all 12 ledger/outbox hashes visible before promotion")
 case "hold":
  if *marker==""{return errors.New("hold-ready marker required")}
  e,err:=eventAt(*seed,100);if err!=nil{return err}
  payload,err:=json.Marshal(e);if err!=nil{return err}
  tx,err:=db.BeginTx(context.Background(),nil);if err!=nil{return err};defer tx.Rollback()
  _,err=tx.Exec(`INSERT INTO usage_ledger
 (event_id,tenant_id,project_id,request_id,event_type,schema_version,model,
  prompt_tokens,completion_tokens,payload_hash,event_time)
 VALUES ($1::uuid,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
   e.EventID,e.TenantID,e.ProjectID,e.RequestID,e.EventType,e.SchemaVersion,e.Model,
   e.PromptTokens,e.CompletionTokens,e.PayloadHash,e.Timestamp)
  if err!=nil{return err}
  if _,err:=tx.Exec(`INSERT INTO event_outbox(event_id,event_payload) VALUES ($1::uuid,$2::jsonb)`,e.EventID,string(payload));err!=nil{return err}
  if err:=os.WriteFile(*marker,[]byte("uncommitted
"),0600);err!=nil{return err}
  // Stop the primary while the transaction remains open. It must not commit.
  var unused any
  if err:=tx.QueryRow("SELECT pg_sleep(90)").Scan(&unused);err!=nil{return fmt.Errorf("expected uncommitted transaction interrupted by primary failure: %w",err)}
  return errors.New("fault injection missed: uncommitted transaction slept 90s")
 case "promoted":
  if !*oldStopped{return errors.New("old primary not fenced; promotion unsafe")}
  if err:=verifyBase(ctx,db,*seed);err!=nil{return err}
  held,_:=eventAt(*seed,100)
  var orphanCount int
  if err:=db.QueryRowContext(ctx,"SELECT count(*) FROM usage_ledger WHERE event_id=$1::uuid",held.EventID).Scan(&orphanCount);err!=nil{return err}
  if orphanCount!=0{return errors.New("uncommitted old-primary transaction was incorrectly visible")}
  duplicates:=0
  for i:=0;i<committedCount;i++{
   e,_:=eventAt(*seed,i)
   state,err:=store.Record(ctx,e)
   if err!=nil||state!=ledger.Duplicate{return fmt.Errorf("replay committed event %d state=%s err=%v",i,state,err)}
   duplicates++
  }
  state,err:=store.Record(ctx,held)
  if err!=nil||state!=ledger.Created{return fmt.Errorf("same stable ID retry after interrupted commit state=%s err=%v",state,err)}
  l,o,err:=counts(ctx,db,*seed);if err!=nil{return err}
  if err:=requireAtomicCounts(l,o,committedCount+1);err!=nil{return err}

  // The old lease is persisted through physical promotion. Expire it only
  // in the disposable lab and show its prior generation cannot act.
  var row outbox.Row
  if err:=db.QueryRowContext(ctx,`SELECT outbox_id,attempts FROM event_outbox WHERE lease_owner='pgha-stale-worker'`).Scan(&row.ID,&row.Attempts);err!=nil{return fmt.Errorf("replicated old lease missing: %w",err)}
  if _,err:=db.ExecContext(ctx,"UPDATE event_outbox SET lease_until=now()-interval '1 second' WHERE outbox_id=$1",row.ID);err!=nil{return err}
  out:=&outbox.Store{DB:db}
  fresh,err:=out.Claim(ctx,"pgha-promoted-worker",time.Minute,1);if err!=nil||len(fresh)!=1||fresh[0].ID!=row.ID{return fmt.Errorf("promoted primary did not reclaim previous lease: %v",err)}
  if err:=out.MarkPublished(ctx,row,"pgha-stale-worker");!errors.Is(err,outbox.ErrLeaseLost){return fmt.Errorf("old lease was not fenced: %v",err)}
  if err:=out.ScheduleRetry(ctx,fresh[0],"pgha-promoted-worker",0,"lab_requeue_without_kafka_ack");err!=nil{return err}
  var published int
  if err:=db.QueryRowContext(ctx,`SELECT count(*) FROM event_outbox WHERE published_at IS NOT NULL`).Scan(&published);err!=nil{return err}
  e:=failoverReceipt{
   SchemaVersion:1,HeadSHA:os.Getenv("GITHUB_SHA"),Mode:"manual_synchronous_pg17",
   CommittedBefore:committedCount,ReplayedOnStandby:committedCount,
   PromotedLedger:l,PromotedOutbox:o,UncommittedOldPrimaryRolledBack:true,
   StableIDRetryCreated:true,ConfirmedBeforeRetriedAsDuplicate:duplicates,
   StaleLeaseFenced:true,OldPrimaryRemainedStopped:*oldStopped,
   PublishedWithoutKafkaACK:published,Result:"PASS",
  }
  if err:=validateReceipt(e);err!=nil{return err}
  if *evidence==""{return errors.New("evidence file is required")}
  if err:=os.MkdirAll(filepath.Dir(*evidence),0700);err!=nil{return err}
  b,err:=json.MarshalIndent(e,"","  ");if err!=nil{return err}
  if err:=os.WriteFile(*evidence,append(b,'
'),0600);err!=nil{return err}
  fmt.Println("PASS promoted standby: 12 confirmed ledger/outbox events; interrupted transaction absent; 13th stable-ID retry created; stale lease fenced; 0 phantom Kafka ACKs")
 }
 return nil
}
func main(){if err:=run();err!=nil{fmt.Fprintln(os.Stderr,err);os.Exit(1)}}
