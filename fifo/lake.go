package fifo

import (
	"context"
	"errors"
	"fmt"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/api"
	"github.com/brimdata/zed/compiler"
	lakeapi "github.com/brimdata/zed/lake/api"
	"github.com/brimdata/zed/lakeparse"
	"github.com/brimdata/zed/order"
	"github.com/brimdata/zed/pkg/field"
	"github.com/brimdata/zed/runtime"
	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zync/etl"
	"github.com/segmentio/ksuid"
)

var ErrBadPoolKey = errors.New("pool key must be 'kafka.offset' in ascending order")

type Lake struct {
	service lakeapi.Interface
	shaper  string
	pool    string
	poolID  ksuid.KSUID
}

func NewLake(ctx context.Context, poolName, shaper string, server lakeapi.Interface) (*Lake, error) {
	pool, err := lakeapi.LookupPoolByName(ctx, server, poolName)
	if err != nil {
		return nil, err
	}
	// The sync algorithm relies on the pool key being kafka.offset asc.
	if pool.Layout.Order != order.Asc || len(pool.Layout.Keys) == 0 || !pool.Layout.Keys[0].Equal(field.Dotted("kafka.offset")) {
		return nil, ErrBadPoolKey
	}
	return &Lake{
		pool:    poolName,
		poolID:  pool.ID,
		service: server,
		shaper:  shaper,
	}, nil
}

func (l *Lake) Query(src string) (*zbuf.Array, error) {
	zr, err := l.service.Query(context.TODO(), &lakeparse.Commitish{Pool: l.pool}, src)
	if err != nil {
		return nil, err
	}
	return etl.NewArrayFromReader(zr)
}

func (l *Lake) LoadBatch(zctx *zed.Context, batch *zbuf.Array) (ksuid.KSUID, error) {
	return l.service.Load(context.TODO(), zctx, l.poolID, "main", batch, api.CommitMessage{})
}

func (l *Lake) NextProducerOffset(topic string) (int64, error) {
	// Run a query against the pool to get the max output offset.
	// We assume the pool key is kafka.offset:asc so we just do "tail 1".
	query := fmt.Sprintf("kafka.topic=='%s' | tail 1 | offset:=kafka.offset", topic)
	batch, err := l.Query(query)
	if err != nil {
		return 0, err
	}
	vals := batch.Values()
	n := len(vals)
	if n == 0 {
		return 0, nil
	}
	if n != 1 {
		// This should not happen.
		return 0, errors.New("'tail 1' returned more than one record")
	}
	offset, err := etl.FieldAsInt(&vals[0], "offset")
	if err != nil {
		return 0, err
	}
	return offset + 1, nil
}

func (l *Lake) NextConsumerOffset(topic string) (int64, error) {
	// Find the largest input_offset for the given topic.  Since these
	// values are monotonically increasing, we can just do "tail 1".
	query := fmt.Sprintf("kafka.topic=='%s' | tail 1 | offset:=kafka.input_offset", topic)
	batch, err := l.Query(query)
	if err != nil {
		return etl.KafkaOffsetEarliest, err
	}
	vals := batch.Values()
	n := len(vals)
	if n == 0 {
		return etl.KafkaOffsetEarliest, nil
	}
	if n != 1 {
		// This should not happen.
		return 0, errors.New("'tail 1' returned more than one record")
	}
	offset, err := etl.FieldAsInt(&vals[0], "offset")
	if err != nil {
		return 0, err
	}
	return offset + 1, nil
}

func (l *Lake) ReadBatch(ctx context.Context, topic string, offset int64, size int) (zbuf.Batch, error) {
	query := fmt.Sprintf("kafka.topic=='%s' kafka.offset >= %d | head %d", topic, offset, size)
	if l.shaper != "" {
		query = fmt.Sprintf("%s | %s  | sort kafka.offset", query, l.shaper)
	} else {
		query += "| sort kafka.offset"
	}
	return l.Query(query)
}

func RunLocalQuery(zctx *zed.Context, batch *zbuf.Array, query string) (*zbuf.Array, error) {
	program, err := compiler.ParseProc(query)
	if err != nil {
		return nil, err
	}
	q, err := runtime.NewQueryOnReader(context.TODO(), zctx, program, batch, nil)
	if err != nil {
		return nil, err
	}
	return etl.NewArrayFromReader(q.AsReader())
}
