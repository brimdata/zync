package fifo

import (
	"errors"
	"fmt"
	"time"

	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zng"
	"github.com/brimdata/zed/zson"
)

// From  provides a means to sync a kafka topic to a Zed lake in a
// consistent and crash-recoverable fashion.  The data sync'd to the lake
// is assigned a target offset in the lake that may be used to then sync
// the merged lake's data back to another Kafka queue using To.
type From struct {
	zctx  *zson.Context
	src   *Consumer
	dst   *Lake
	batch zbuf.Batch
}

func NewFrom(zctx *zson.Context, dst *Lake, src *Consumer) *From {
	return &From{
		zctx: zctx,
		src:  src,
		dst:  dst,
	}
}

// Make theae configurable
const BatchThresh = 10 * 1024 * 1024
const BatchTimeout = time.Second //XXX make this bigger

func (f *From) Sync() error {
	offset, err := f.NextLakeOffset()
	if err != nil {
		return err
	}
	for {
		batch, err := f.src.Read(BatchThresh, BatchTimeout)
		if err != nil {
			//XXX check for timeout
			return err
		}
		batchLen := batch.Length()
		if batchLen == 0 {
			return nil
		}
		batch, err = AdjustOffsets(f.zctx, batch, offset)
		if err != nil {
			return err
		}
		if err := f.dst.LoadBatch(batch); err != nil {
			return err
		}
		offset += int64(batchLen)
	}
	return nil
}

func (f *From) NextLakeOffset() (int64, error) {
	//XXX run a query against the lake to get the max output offset
	// we assume the pool-key is kafka.offset so we just run a head 1
	//XXX this should be extended to query with a commitID so we can
	// detect multiple writers.
	batch, err := f.dst.Query("head 1 | offset:=kafka.offset")
	if err != nil {
		return 0, err
	}
	n := batch.Length()
	if n == 0 {
		return 0, nil
	}
	if n != 1 {
		return 0, errors.New("'head 1' returned more than one record")
	}
	return batch.Index(0).AccessInt("offset")
}

// AdjustOffsets runs a local Zed program to adjust the kafka offset fields
// for insertion into correct position in the lake and remember the original
// offset
func AdjustOffsets(zctx *zson.Context, batch zbuf.Batch, offset int64) (zbuf.Batch, error) {
	rec := batch.Index(0)
	kafkaRec, err := batch.Index(0).Access("kafka")
	if err != nil {
		s, err := zson.FormatValue(rec.Value)
		if err != nil {
			err = fmt.Errorf("[ERR! %w]", err)
		}
		return nil, fmt.Errorf("value read from kafka topic missing kafka meta-data field: %s", s)
	}
	// XXX this should be simplified in zed package
	first, err := zng.NewRecord(kafkaRec.Type, kafkaRec.Bytes).Access("kafka")
	if err != nil {
		return nil, errors.New("kafka meta-data field is missing 'offset' field")
	}
	query := fmt.Sprintf("kafka.input_offset:=kafka.offset,kakfa.offset:=kafka.offset-%d+%d", first, offset)
	return RunLocalQuery(zctx, batch, query)
}
