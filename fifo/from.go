package fifo

import (
	"context"
	"fmt"
	"time"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/zbuf"
)

// From syncs data from a Kafka topic to a Zed lake in a
// consistent and crash-recoverable fashion.  The data synced to the lake
// is assigned a target offset in the lake that may be used to then sync
// the merged lake's data back to another Kafka topic using To.
type From struct {
	zctx   *zed.Context
	dst    *Lake
	src    *Consumer
	shaper string
	batch  zbuf.Batch
}

func NewFrom(zctx *zed.Context, dst *Lake, src *Consumer, shaper string) *From {
	return &From{
		zctx:   zctx,
		dst:    dst,
		src:    src,
		shaper: shaper,
	}
}

// Sync syncs data.  If thresh is nonnegative, Sync returns after syncing at
// least thresh records.  Sync also returns if timeout elapses while waiting to
// receive new records from the Kafka topic.
func (f *From) Sync(ctx context.Context, thresh int, timeout time.Duration) (int64, int64, error) {
	// Loop over the records from the Kafka consumer and
	// commit a batch at a time to the lake.
	var ncommit, nrec int64
	for {
		batch, err := f.src.Read(ctx, thresh, timeout)
		if err != nil {
			return 0, 0, err
		}
		vals := batch.Values()
		n := len(vals)
		if n == 0 {
			break
		}
		if f.shaper != "" {
			batch, err = RunLocalQuery(f.zctx, batch, f.shaper)
			if err != nil {
				return 0, 0, err
			}
		}
		//XXX We need to track the commitID and use new commit-only-if
		// constraint and recompute offsets if needed.  See zync issue #16.
		commit, err := f.dst.LoadBatch(f.zctx, batch)
		if err != nil {
			return 0, 0, err
		}
		fmt.Printf("commit %s %d record%s\n", commit, n, plural(n))
		nrec += int64(n)
		ncommit++
	}
	return ncommit, nrec, nil
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
