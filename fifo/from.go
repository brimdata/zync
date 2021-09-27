package fifo

import (
	"context"
	"fmt"
	"time"

	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zng"
	"github.com/brimdata/zed/zson"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// From  provides a means to sync from a kafka topic to a Zed lake in a
// consistent and crash-recoverable fashion.  The data sync'd to the lake
// is assigned a target offset in the lake that may be used to then sync
// the merged lake's data back to another Kafka queue using To.
type From struct {
	zctx   *zson.Context
	dst    *Lake
	src    *Consumer
	shaper string
	batch  zbuf.Batch
}

func NewFrom(zctx *zson.Context, dst *Lake, src *Consumer, shaper string) *From {
	return &From{
		zctx:   zctx,
		dst:    dst,
		src:    src,
		shaper: shaper,
	}
}

// These should be configurable.  See issue #18.
const BatchThresh = 10 * 1024 * 1024
const BatchTimeout = 5 * time.Second

func (f *From) Sync(ctx context.Context) (int64, int64, error) {
	offset, err := f.dst.NextProducerOffset()
	if err != nil {
		return 0, 0, err
	}
	// Loop over the records from the kafka consumer and
	// commit a batch at a time to the lake.
	var ncommit, nrec int64
	for {
		batch, err := f.src.Read(ctx, BatchThresh, BatchTimeout)
		if err != nil {
			return 0, 0, err
		}
		batchLen := batch.Length()
		if batchLen == 0 {
			break
		}
		batch, err = AdjustOffsetsAndShape(f.zctx, batch, offset, f.shaper)
		if err != nil {
			return 0, 0, err
		}
		//XXX We need to track the commitID and use new commit-only-if
		// constraint and recompute offsets if needed.  See zinger issue #16.
		commit, err := f.dst.LoadBatch(batch)
		if err != nil {
			return 0, 0, err
		}
		fmt.Printf("commit %s %d record%s\n", commit, batchLen, plural(batchLen))
		offset += kafka.Offset(batchLen)
		nrec += int64(batchLen)
		ncommit++
	}
	return ncommit, nrec, nil
}

// AdjustOffsetsAndShape runs a local Zed program to adjust the kafka offset fields
// for insertion into correct position in the lake and remember the original
// offset along with applying a user-defined shaper.
func AdjustOffsetsAndShape(zctx *zson.Context, batch zbuf.Array, offset kafka.Offset, shaper string) (zbuf.Array, error) {
	rec := batch.Index(0)
	kafkaRec, err := batch.Index(0).Access("kafka")
	if err != nil {
		s, err := zson.FormatValue(rec.Value)
		if err != nil {
			// This should not happen.
			err = fmt.Errorf("[ERR! %w]", err)
		}
		// This shouldn't happen since the consumer automatically adds
		// this field.
		return nil, fmt.Errorf("value read from kafka topic missing kafka meta-data field: %s", s)
	}
	// XXX this should be simplified in zed package
	first, err := zng.NewRecord(kafkaRec.Type, kafkaRec.Bytes).AccessInt("offset")
	if err != nil {
		s, err := zson.FormatValue(kafkaRec)
		if err != nil {
			// This should not happen.
			err = fmt.Errorf("[ERR! %w]", err)
		}
		return nil, fmt.Errorf("kafka meta-data field is missing 'offset' field: %s", s)
	}
	// Send the batch of Zed records through this query to adjust the save
	// the original input offset and adjust the offset so it fits in sequetentially
	// we everything else in the target pool.
	query := fmt.Sprintf("kafka.input_offset:=kafka.offset,kafka.offset:=kafka.offset-%d+%d", first, offset)
	if shaper != "" {
		query = fmt.Sprintf("%s | %s", query, shaper)
	}
	return RunLocalQuery(zctx, batch, query)
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
