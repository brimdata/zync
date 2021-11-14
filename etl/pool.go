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
	"github.com/brimdata/zed/driver"
	"github.com/brimdata/zed/expr/extent"
	"github.com/brimdata/zed/field"
	lakeapi "github.com/brimdata/zed/lake/api"
	"github.com/brimdata/zed/order"
	"github.com/brimdata/zed/proc"
	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zio"
	"github.com/segmentio/ksuid"
	"go.uber.org/zap"
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

func (p *Pool) Query(q string) (zbuf.Array, error) {
	query := fmt.Sprintf("from '%s' | %s", p.pool, q)
	return p.QueryWithFrom(query)
}

func (p *Pool) QueryWithFrom(q string) (zbuf.Array, error) {
	//XXX We need to make this API easier in package zed...
	result := &batchDriver{}
	_, err := p.service.Query(context.TODO(), result, nil, q)
	if err != nil {
		return nil, err
	}
	return result.Array, nil
}

func (p *Pool) LoadBatch(batch zbuf.Array) (ksuid.KSUID, error) {
	return p.service.Load(context.TODO(), p.poolID, "main", &batch, api.CommitMessage{})
}

func (p *Pool) NextProducerOffsets() (map[string]kafka.Offset, error) {
	// Run a query against the pool to get the max output offset.
	// We assume the pool-key is kafka.offset so we just run a head 1.
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
	// values are monotonically increasing, we can just do head 1.
	query := fmt.Sprintf("kafka.topic=='%s' | head 1 | offset:=kafka.input_offset", topic)
	batch, err := p.Query(query)
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
		return 0, errors.New("'head 1' returned more than one record")
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

func RunLocalQuery(zctx *zed.Context, batch zbuf.Array, query string) (zbuf.Array, error) {
	//XXX We need to make this API easier in package zed...
	program, err := parse(query)
	if err != nil {
		return nil, err
	}
	var result zbuf.Array
	if err := driver.Copy(context.TODO(), &result, program, zctx, &batch, zap.NewNop()); err != nil {
		return nil, err
	}
	return result, nil
}

func RunLocalJoin(zctx *zed.Context, left, right zio.Reader, query string) (zbuf.Array, error) {
	program, err := parse(query)
	if err != nil {
		return nil, err
	}
	readers := []zio.Reader{left, right}
	d := &batchDriver{}
	dummy := &adaptor{}
	_, err = driver.RunJoinWithFileSystem(context.TODO(), d, program, zctx, readers, dummy)
	return d.Array, err
}

//XXX this needs to go and be replaced by easier zed package API
type batchDriver struct {
	zbuf.Array
}

func (b *batchDriver) Write(cid int, batch zbuf.Batch) error {
	if cid != 0 {
		return errors.New("internal error: multiple tails not allowed")
	}
	vals := batch.Values()
	for i := range vals {
		rec := &vals[i]
		rec.Keep() //XXX
		b.Append(rec)
	}
	batch.Unref()
	return nil
}

func (*batchDriver) Warn(warning string) error           { return nil }
func (*batchDriver) Stats(stats zbuf.ScannerStats) error { return nil }
func (*batchDriver) ChannelEnd(cid int) error            { return nil }

type adaptor struct{}

func (*adaptor) Layout(_ context.Context, src dag.Source) order.Layout {
	return order.Nil
}

func (*adaptor) NewScheduler(context.Context, *zed.Context, dag.Source, extent.Span, zbuf.Filter, *dag.Filter) (proc.Scheduler, error) {
	return nil, fmt.Errorf("mock.Lake.NewScheduler() should not be called")
}

func (*adaptor) Open(_ context.Context, _ *zed.Context, _ string, _ zbuf.Filter) (zbuf.PullerCloser, error) {
	return nil, fmt.Errorf("mock.Lake.Open() should not be called")
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
