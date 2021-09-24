package fifo

import (
	"context"
	"errors"
	"fmt"

	"github.com/brimdata/zed/api"
	"github.com/brimdata/zed/compiler"
	"github.com/brimdata/zed/driver"
	"github.com/brimdata/zed/field"
	lakeapi "github.com/brimdata/zed/lake/api"
	"github.com/brimdata/zed/order"
	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zson"
	"github.com/segmentio/ksuid"
	"go.uber.org/zap"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var ErrBadPoolKey = errors.New("pool key must be kafka.offset in descending order")

type Lake struct {
	service *lakeapi.RemoteSession
	shaper  string
	pool    string
	poolID  ksuid.KSUID
}

func NewLake(ctx context.Context, poolName, shaper string, server *lakeapi.RemoteSession) (*Lake, error) {
	pool, err := lakeapi.LookupPoolByName(ctx, server, poolName)
	if err != nil {
		return nil, err
	}
	// The sync algorithm relies on the pool key being kafka.offset desc.
	if pool.Layout.Order != order.Desc || len(pool.Layout.Keys) == 0 || !pool.Layout.Keys[0].Equal(field.Dotted("kafka.offset")) {
		return nil, ErrBadPoolKey
	}
	return &Lake{
		pool:    poolName,
		poolID:  pool.ID,
		service: server,
		shaper:  shaper,
	}, nil
}

func (l *Lake) Query(q string) (zbuf.Array, error) {
	query := fmt.Sprintf("from '%s' | %s", l.pool, q)
	//XXX We need to make this API easier in package zed...
	result := &batchDriver{}
	_, err := l.service.Query(context.TODO(), result, nil, query)
	if err != nil {
		return nil, err
	}
	return result.Array, nil
}

func (l *Lake) LoadBatch(batch zbuf.Array) (ksuid.KSUID, error) {
	return l.service.Load(context.TODO(), l.poolID, "main", &batch, api.CommitMessage{})
}

func (l *Lake) NextProducerOffset() (kafka.Offset, error) {
	// Run a query against the pool to get the max output offset.
	// We assume the pool-key is kafka.offset so we just run a head 1.
	batch, err := l.Query("head 1 | offset:=kafka.offset")
	if err != nil {
		return 0, err
	}
	n := batch.Length()
	if n == 0 {
		return 0, nil
	}
	if n != 1 {
		// This should not happen.
		return 0, errors.New("'head 1' returned more than one record")
	}
	offset, err := batch.Index(0).AccessInt("offset")
	if err != nil {
		return 0, err
	}
	return kafka.Offset(offset + 1), nil
}

func (l *Lake) NextConsumerOffset(topic string) (kafka.Offset, error) {
	// Find the largest input_offset for the given topic.  Since these
	// values are monotonically increasing, we can just do head 1.
	query := fmt.Sprintf("kafka.topic=='%s' | head 1 | offset:=kafka.input_offset", topic)
	batch, err := l.Query(query)
	if err != nil {
		return 0, err
	}
	n := batch.Length()
	if n == 0 {
		return 0, nil
	}
	if n != 1 {
		// This should not happen.
		return 0, errors.New("'head 1' returned more than one record")
	}
	offset, err := batch.Index(0).AccessInt("offset")
	if err != nil {
		return 0, err
	}
	return kafka.Offset(offset + 1), nil
}

func (l *Lake) ReadBatch(ctx context.Context, offset kafka.Offset, size int) (zbuf.Batch, error) {
	query := fmt.Sprintf("kafka.offset >= %d | head %d", offset, size)
	if l.shaper != "" {
		query = fmt.Sprintf("%s | %s  | sort kafka.offset", query, l.shaper)
	} else {
		query += "| sort kafka.offset"
	}
	return l.Query(query)
}

func RunLocalQuery(zctx *zson.Context, batch zbuf.Array, query string) (zbuf.Array, error) {
	//XXX We need to make this API easier in package zed...
	program, err := compiler.ParseProc(query)
	if err != nil {
		return nil, err
	}
	var result zbuf.Array
	if err := driver.Copy(context.TODO(), &result, program, zctx, &batch, zap.NewNop()); err != nil {
		return nil, err
	}
	return result, nil
}

type batchDriver struct {
	zbuf.Array
}

func (b *batchDriver) Write(cid int, batch zbuf.Batch) error {
	if cid != 0 {
		return errors.New("internal error: multiple tails not allowed")
	}
	for i := 0; i < batch.Length(); i++ {
		rec := batch.Index(i)
		rec.Keep()
		b.Append(rec)
	}
	batch.Unref()
	return nil
}

func (*batchDriver) Warn(warning string) error          { return nil }
func (*batchDriver) Stats(stats api.ScannerStats) error { return nil }
func (*batchDriver) ChannelEnd(cid int) error           { return nil }
