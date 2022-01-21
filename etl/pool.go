package etl

import (
	"context"
	"errors"
	"fmt"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/api"
	"github.com/brimdata/zed/compiler"
	"github.com/brimdata/zed/compiler/ast"
	"github.com/brimdata/zed/compiler/ast/dag"
	"github.com/brimdata/zed/expr/extent"
	"github.com/brimdata/zed/field"
	lakeapi "github.com/brimdata/zed/lake/api"
	"github.com/brimdata/zed/lakeparse"
	"github.com/brimdata/zed/order"
	"github.com/brimdata/zed/proc"
	"github.com/brimdata/zed/runtime"
	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zio"
	"github.com/segmentio/ksuid"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var ErrBadPoolKey = errors.New("pool key must be 'kafka.offset' in ascending order")

type Pool struct {
	service lakeapi.Interface
	pool    string
	poolID  ksuid.KSUID
}

func OpenPool(ctx context.Context, poolName string, server lakeapi.Interface) (*Pool, error) {
	pool, err := lakeapi.LookupPoolByName(ctx, server, poolName)
	if err != nil {
		return nil, err
	}
	// The sync algorithm relies on the pool key being kafka.offset asc.
	if pool.Layout.Order != order.Asc || len(pool.Layout.Keys) == 0 || !pool.Layout.Keys[0].Equal(field.Dotted("kafka.offset")) {
		return nil, ErrBadPoolKey
	}
	return &Pool{
		pool:    poolName,
		poolID:  pool.ID,
		service: server,
	}, nil
}

func (p *Pool) Query(src string) (*zbuf.Array, error) {
	zr, err := p.service.Query(context.TODO(), &lakeparse.Commitish{Pool: p.pool}, src)
	if err != nil {
		return nil, err
	}
	return NewArrayFromReader(zr)
}

func (p *Pool) LoadBatch(zctx *zed.Context, batch *zbuf.Array) (ksuid.KSUID, error) {
	return p.service.Load(context.TODO(), zctx, p.poolID, "main", batch, api.CommitMessage{})
}

func (p *Pool) NextProducerOffsets() (map[string]kafka.Offset, error) {
	// Run a query against the pool to get the max output offset.
	batch, err := p.Query("offset:=max(kafka.offset) by topic:=kafka.topic")
	if err != nil {
		return nil, err
	}
	// Note at start-up if there are no offsets, then we will return an empty
	// map and the caller will get offset 0 for the next offset of any lookups.
	offsets := make(map[string]kafka.Offset)
	vals := batch.Values()
	for k := range vals {
		rec := &vals[k]
		offset, err := rec.AccessInt("offset")
		if err != nil {
			return nil, err
		}
		topic, err := rec.AccessString("topic")
		if err != nil {
			return nil, err
		}
		offsets[topic] = kafka.Offset(offset + 1)
	}
	return offsets, nil
}

func (p *Pool) NextConsumerOffset(topic string) (kafka.Offset, error) {
	// Find the largest input_offset for the given topic.  Since these
	// values are monotonically increasing, we can just do "tail 1".
	query := fmt.Sprintf("kafka.topic=='%s' | tail 1 | offset:=kafka.input_offset", topic)
	batch, err := p.Query(query)
	if err != nil {
		return kafka.OffsetBeginning, err
	}
	vals := batch.Values()
	n := len(vals)
	if n == 0 {
		return kafka.OffsetBeginning, nil
	}
	if n != 1 {
		// This should not happen.
		return 0, errors.New("'tail 1' returned more than one record")
	}
	offset, err := vals[0].AccessInt("offset")
	if err != nil {
		return 0, err
	}
	return kafka.Offset(offset + 1), nil
}

func (p *Pool) ReadBatch(ctx context.Context, offset kafka.Offset, size int) (zbuf.Batch, error) {
	query := fmt.Sprintf("kafka.offset >= %d | head %d", offset, size)
	//XXX
	query += "| sort kafka.offset"
	return p.Query(query)
}

func RunLocalQuery(zctx *zed.Context, batch *zbuf.Array, query string) (*zbuf.Array, error) {
	program, err := parse(query)
	if err != nil {
		return nil, err
	}
	q, err := runtime.NewQueryOnReader(context.TODO(), zctx, program, batch, nil)
	if err != nil {
		return nil, err
	}
	return NewArrayFromReader(q.AsReader())
}

func RunLocalJoin(zctx *zed.Context, left, right zio.Reader, query string) (*zbuf.Array, error) {
	program, err := parse(query)
	if err != nil {
		return nil, err
	}
	readers := []zio.Reader{left, right}
	q, err := runtime.NewQueryOnFileSystem(context.TODO(), zctx, program, readers, &adaptor{})
	if err != nil {
		return nil, err
	}
	return NewArrayFromReader(q.AsReader())
}

type adaptor struct{}

func (*adaptor) Layout(context.Context, dag.Source) order.Layout {
	return order.Nil
}

func (*adaptor) NewScheduler(context.Context, *zed.Context, dag.Source, extent.Span, zbuf.Filter, *dag.Filter) (proc.Scheduler, error) {
	return nil, errors.New("mock.Lake.NewScheduler() should not be called")
}

func (*adaptor) Open(context.Context, *zed.Context, string, zbuf.Filter) (zbuf.PullerCloser, error) {
	return nil, errors.New("mock.Lake.Open() should not be called")
}

func (*adaptor) PoolID(context.Context, string) (ksuid.KSUID, error) {
	return ksuid.Nil, nil
}

func (*adaptor) CommitObject(_ context.Context, _ ksuid.KSUID, _ string) (ksuid.KSUID, error) {
	return ksuid.Nil, nil
}

func parse(z string) (ast.Proc, error) {
	program, err := compiler.ParseProc(z)
	if err != nil {
		return nil, fmt.Errorf("Zed parse error: %w\nZed source:\n%s", err, z)
	}
	return program, err
}

func NewArrayFromReader(zr zio.Reader) (*zbuf.Array, error) {
	var a zbuf.Array
	for {
		val, err := zr.Read()
		if err != nil {
			return nil, err
		}
		if val == nil {
			return &a, nil
		}
		a.Append(val.Copy())
	}
}
