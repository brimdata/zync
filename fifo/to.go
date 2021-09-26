package fifo

import (
	"context"
	"fmt"

	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zson"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// To provides a means to sync from a Zed data pool to a kafka topic in a
// consistent and crash-recoverable fashion.  The data sync'd to the topic
// has the same offset as the kafka.offset field in the records in the pool.
type To struct {
	zctx  *zson.Context
	dst   *Producer
	src   *Lake
	batch zbuf.Batch
}

func NewTo(zctx *zson.Context, dst *Producer, src *Lake) *To {
	return &To{
		zctx: zctx,
		dst:  dst,
		src:  src,
	}
}

const BatchSize = 200

func (t *To) Sync(ctx context.Context) error {
	offset, err := t.dst.HeadOffset()
	if err != nil {
		return err
	}
	for {
		// Query of batch of records that start at the given offset.
		batch, err := t.src.ReadBatch(ctx, offset, BatchSize)
		if err != nil {
			return err
		}
		batchLen := batch.Length()
		if batchLen == 0 {
			fmt.Printf("reached sync at offset %d\n", offset)
			//XXX should pause and poll again... for now, exit
			break
		}
		if err := t.dst.Send(ctx, offset, batch); err != nil {
			return err
		}
		//XXX logger
		fmt.Printf("committed %d record%s at offset %d to output topic\n", batchLen, plural(batchLen), offset)
		offset += kafka.Offset(batchLen)
	}
	return nil
}
