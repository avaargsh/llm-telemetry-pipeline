// Package ingest contains generation-fenced Kafka consumer-group execution.
// Each assigned partition has a single in-order worker. The broker generation,
// not a process-local counter, authorizes offset commits.
package ingest

import (
    "context"
    "errors"
    "fmt"
    "log"
    "net"
    "time"

    "github.com/segmentio/kafka-go"
)

// GenerationCommitter commits only the assigned partition and includes the
// broker's actual consumer group Generation ID / Member ID in CommitOffsets.
// The generation context is canceled on revocation; the Kafka coordinator
// independently rejects a stale generation if revocation races with this check.
type GenerationCommitter struct {
    Generation context.Context
    Topic string
    Partition int
    CommitOffsets func(map[string]map[int]int64) error
    OnCommitted func(Message)
}

func(c GenerationCommitter)Commit(ctx context.Context,msgs []Message)error{
    if len(msgs)==0{return nil}
    if c.Generation==nil||c.CommitOffsets==nil||c.Topic==""||c.Partition<0 {
        return errors.New("generation owner and commit callback required")
    }
    for _,m:=range msgs{
        if m.Topic!=c.Topic||m.Partition!=int32(c.Partition)||m.Offset<0{
            return fmt.Errorf("commit rejected for unassigned or invalid partition %s/%d/%d",m.Topic,m.Partition,m.Offset)
        }
    }
    last:=msgs[0]
    for _,m:=range msgs[1:]{
        if m.Offset>last.Offset{last=m}
    }
    if err:=ctx.Err();err!=nil{return fmt.Errorf("generation commit context canceled: %w",err)}
    if err:=c.Generation.Err();err!=nil{return fmt.Errorf("generation ownership ended before commit: %w",err)}
    offsets:=map[string]map[int]int64{c.Topic:{c.Partition:last.Offset+1}}
    // A cancellation can race this network operation. Kafka's Generation ID
    // and Member ID, carried by kafka.Generation.CommitOffsets, are the
    // authoritative broker-side fence.
    if err:=c.CommitOffsets(offsets);err!=nil{return fmt.Errorf("broker rejected generation offset commit: %w",err)}
    if c.OnCommitted!=nil{c.OnCommitted(last)}
    return nil
}

// retryableGroupError only retries short-lived coordinator/bootstrap conditions.
// Kafka authorization and configuration errors are always surfaced immediately.
func retryableGroupError(err error) bool {
    var kafkaError kafka.Error
    if errors.As(err,&kafkaError) {
        return kafkaError.Temporary() ||
            kafkaError==kafka.RebalanceInProgress ||
            kafkaError==kafka.IllegalGeneration ||
            kafkaError==kafka.UnknownMemberId
    }
    var netError net.Error
    return errors.As(err,&netError) && (netError.Timeout()||netError.Temporary())
}

type GroupWorker struct{
    Brokers []string
    Topic string
    GroupID string
    BatchSize int
    FetchWait time.Duration
    Sink Inserter
    DLQ DeadLetterPublisher
    OnCommitted func(Message) // optional observation after broker-confirmed commit
}

// Run starts a kafka-go ConsumerGroup, and binds each partition reader and
// commit to the specific Generation returned by the group coordinator.
// On partition loss the generation context stops fetching and forbids commits.
// Fatal downstream failures stop this process rather than skipping data.
func(w GroupWorker)Run(ctx context.Context)error{
    if len(w.Brokers)==0||w.Topic==""||w.GroupID==""||w.Sink==nil||
       w.BatchSize<1||w.BatchSize>10000||w.FetchWait<=0{
        return errors.New("invalid consumer group worker configuration")
    }
    cg,err:=kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
        ID:w.GroupID,Brokers:w.Brokers,Topics:[]string{w.Topic},
        StartOffset:kafka.FirstOffset,
    })
    if err!=nil{return fmt.Errorf("create Kafka consumer group: %w",err)}
    defer cg.Close()
    runCtx,cancel:=context.WithCancel(ctx)
    defer cancel()
    fatal:=make(chan error,1)
    for {
        gen,err:=cg.Next(runCtx)
        if err!=nil{
            select{case e:=<-fatal:return e;default:}
            if ctx.Err()!=nil{return ctx.Err()}
            if runCtx.Err()!=nil{return runCtx.Err()}
            if retryableGroupError(err){
                log.Printf("temporary group coordinator error, retrying: %v",err)
                select{
                case <-runCtx.Done():return runCtx.Err()
                case <-time.After(350*time.Millisecond):
                }
                continue
            }
            return fmt.Errorf("consumer group next generation: %w",err)
        }
        for topic,assignments:=range gen.Assignments{
            for _,assignment:=range assignments{
                topic,assignment:=topic,assignment
                gen.Start(func(genCtx context.Context){
                    log.Printf("group=%s generation=%d member=%s assigned %s/%d starting %d",
                        w.GroupID,gen.ID,gen.MemberID,topic,assignment.ID,assignment.Offset)
                    commit:=GenerationCommitter{
                        Generation:genCtx,Topic:topic,Partition:assignment.ID,
                        CommitOffsets:gen.CommitOffsets,OnCommitted:w.OnCommitted,
                    }
                    err:=RunAssignedPartition(genCtx,w.Brokers,topic,assignment.ID,
                        assignment.Offset,w.BatchSize,w.FetchWait,w.Sink,w.DLQ,commit)
                    if err!=nil&&genCtx.Err()==nil{
                        select{case fatal<-fmt.Errorf("partition %s/%d generation %d: %w",topic,assignment.ID,gen.ID,err):default:}
                        cancel()
                    }
                })
            }
        }
    }
}

// RunAssignedPartition uses a non-group Reader for exactly one assignment.
// StartOffset comes from the real group generation, not a guessed resume point.
// Only a fully acknowledged batch can move the generation-scoped Kafka offset.
func RunAssignedPartition(ctx context.Context,brokers []string,topic string,partition int,
    startOffset int64,batchSize int,fetchWait time.Duration,
    sink Inserter,dlq DeadLetterPublisher,commit Committer)error{
    if len(brokers)==0||topic==""||partition<0||batchSize<1||fetchWait<=0||
       sink==nil||commit==nil{return errors.New("invalid partition worker arguments")}
    reader:=kafka.NewReader(kafka.ReaderConfig{
        Brokers:brokers,Topic:topic,Partition:partition,MinBytes:1,MaxBytes:10e6,
        MaxWait:250*time.Millisecond,QueueCapacity:batchSize,
    })
    defer reader.Close()
    if err:=reader.SetOffset(startOffset);err!=nil{return fmt.Errorf("set assignment offset: %w",err)}
    batch:=make([]Message,0,batchSize)
    flush:=func()error{
        if len(batch)==0{return nil}
        if err:=ctx.Err();err!=nil{return err}
        if err:=ProcessBatchWithDLQ(ctx,batch,sink,commit,dlq);err!=nil{return err}
        log.Printf("confirmed and committed topic=%s partition=%d records=%d offset=%d",
            topic,partition,len(batch),batch[len(batch)-1].Offset)
        batch=batch[:0]
        return nil
    }
    for {
        fetchCtx,cancel:=context.WithTimeout(ctx,fetchWait)
        msg,err:=reader.FetchMessage(fetchCtx)
        cancel()
        if ctx.Err()!=nil{return ctx.Err()}
        if errors.Is(err,context.DeadlineExceeded){
            if err:=flush();err!=nil{return err}
            continue
        }
        if err!=nil{return fmt.Errorf("fetch %s/%d: %w",topic,partition,err)}
        if msg.Topic!=topic||msg.Partition!=partition{
            return fmt.Errorf("fetched unassigned partition %s/%d",msg.Topic,msg.Partition)
        }
        batch=append(batch,Message{Topic:msg.Topic,Partition:int32(msg.Partition),
            Offset:msg.Offset,Value:msg.Value})
        if len(batch)>=batchSize{
            if err:=flush();err!=nil{return err}
        }
    }
}
