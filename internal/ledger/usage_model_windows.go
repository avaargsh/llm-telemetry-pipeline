package ledger

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

const MaxModelQueryWindows = 128

var ErrUsageQueryWindows = errors.New("model query requires contiguous nonempty windows within 31 days")

// ModelQueryWindow is an event-time interval [From, To).
type ModelQueryWindow struct {
	From time.Time `json:"from"`
	To time.Time `json:"to"`
}

type ModelWindowCapture struct {
	ModelQueryWindow
	Models []ModelUsage `json:"models"`
}

func validateModelQueryWindows(windows []ModelQueryWindow) error {
	if len(windows)==0 || len(windows)>MaxModelQueryWindows { return ErrUsageQueryWindows }
	for i,w := range windows {
		if w.From.IsZero() || w.To.IsZero() || !w.From.Before(w.To) ||
			w.From.Year()<1 || w.From.Year()>9999 || w.To.Year()<1 || w.To.Year()>9999 ||
			w.From.Nanosecond()%1000!=0 || w.To.Nanosecond()%1000!=0 ||
			(i>0 && !windows[i-1].To.Equal(w.From)) {
			return ErrUsageQueryWindows
		}
	}
	if windows[len(windows)-1].To.Sub(windows[0].From)>MaxUsageWindow { return ErrUsageQueryWindows }
	return nil
}

// UsageByModelWindows reads every requested interval from one authenticated,
// scoped, read-only repeatable-read transaction. A failure in any interval
// or transaction commit discards all captures. An empty interval has an
// explicit empty Models array. No current-time completeness is asserted.
func (s *ScopedUsageReader) UsageByModelWindows(ctx context.Context,tenant,project string,windows []ModelQueryWindow)([]ModelWindowCapture,error) {
	if err := validateModelQueryWindows(windows);err != nil { return nil,err }
	return scopedRead(ctx,s,tenant,project,func(tx *sql.Tx)([]ModelWindowCapture,error){
		result := make([]ModelWindowCapture,0,len(windows))
		for _,w := range windows {
			models,err := queryUsageByModel(ctx,tx,scopedUsageRelation,tenant,project,w.From,w.To)
			if err != nil { return nil,err }
			result = append(result,ModelWindowCapture{ModelQueryWindow:ModelQueryWindow{From:w.From.UTC(),To:w.To.UTC()},Models:models})
		}
		return result,nil
	})
}
