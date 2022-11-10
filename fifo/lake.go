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
	"github.com/brimdata/zed/zio"
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

func (l *Lake) Pool() string { return l.pool }

func (l *Lake) Query(ctx context.Context, src string) (*zbuf.Array, error) {
	zr, err := l.service.Query(ctx, &lakeparse.Commitish{Pool: l.pool}, src)
	if err != nil {
		return nil, err
	}
	return etl.NewArrayFromReader(zr)
}

func (l *Lake) LoadBatch(ctx context.Context, zctx *zed.Context, batch *zbuf.Array) (ksuid.KSUID, error) {
	return l.service.Load(ctx, zctx, l.poolID, "main", batch, api.CommitMessage{})
}

func (l *Lake) NextConsumerOffset(ctx context.Context, topic string) (int64, error) {
	// Find the largest offset for the given topic.  Since these
	// values are monotonically increasing, we can just do "tail 1".
	query := fmt.Sprintf("kafka.topic=='%s' | tail 1 | yield kafka", topic)
	batch, err := l.Query(ctx, query)
	if err != nil {
		return etl.KafkaOffsetEarliest, err
	}
	vals := batch.Values()
	if n := len(vals); n == 0 {
		return etl.KafkaOffsetEarliest, nil
	} else if n > 1 {
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
	return l.Query(ctx, query)
}

func RunLocalQuery(ctx context.Context, zctx *zed.Context, batch *zbuf.Array, query string) (*zbuf.Array, error) {
	comp := compiler.NewCompiler()
	program, err := comp.Parse(query)
	if err != nil {
		return nil, err
	}
	q, err := runtime.CompileQuery(ctx, zctx, comp, program, []zio.Reader{batch})
	if err != nil {
		return nil, err
	}
	return etl.NewArrayFromReader(q.AsReader())
}
