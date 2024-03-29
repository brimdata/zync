package etl

import (
	"context"
	"errors"
	"fmt"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/api"
	lakeapi "github.com/brimdata/zed/lake/api"
	"github.com/brimdata/zed/lakeparse"
	"github.com/brimdata/zed/order"
	"github.com/brimdata/zed/pkg/field"
	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zio"
	"github.com/brimdata/zed/zson"
	"github.com/segmentio/ksuid"
)

// KafkaOffsetEarliest is used to begin consuming at the earliest (oldest)
// offset.
const KafkaOffsetEarliest = -2

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
	if pool.SortKey.Order != order.Asc || len(pool.SortKey.Keys) == 0 || !pool.SortKey.Keys[0].Equal(field.Dotted("kafka.offset")) {
		return nil, ErrBadPoolKey
	}
	return &Pool{
		pool:    poolName,
		poolID:  pool.ID,
		service: server,
	}, nil
}

func (p *Pool) Query(ctx context.Context, src string) (*zbuf.Array, error) {
	zr, err := p.service.Query(ctx, &lakeparse.Commitish{Pool: p.pool}, src)
	if err != nil {
		return nil, err
	}
	return NewArrayFromReader(zr)
}

func (p *Pool) LoadBatch(ctx context.Context, zctx *zed.Context, batch *zbuf.Array) (ksuid.KSUID, error) {
	return p.service.Load(ctx, zctx, p.poolID, "main", batch, api.CommitMessage{})
}

func (p *Pool) NextProducerOffsets(ctx context.Context) (map[string]int64, error) {
	// Run a query against the pool to get the max output offset.
	batch, err := p.Query(ctx, "offset:=max(kafka.offset) by topic:=kafka.topic")
	if err != nil {
		return nil, err
	}
	// Note at start-up if there are no offsets, then we will return an empty
	// map and the caller will get offset 0 for the next offset of any lookups.
	offsets := make(map[string]int64)
	for _, rec := range batch.Values() {
		offset, err := FieldAsInt(rec, "offset")
		if err != nil {
			return nil, err
		}
		topic, err := FieldAsString(rec, "topic")
		if err != nil {
			return nil, err
		}
		offsets[topic] = offset + 1
	}
	return offsets, nil
}

func NewArrayFromReader(zr zio.Reader) (*zbuf.Array, error) {
	var a zbuf.Array
	if err := zio.Copy(&a, zr); err != nil {
		return nil, err
	}
	return &a, nil
}

func Field(val zed.Value, field string) (zed.Value, error) {
	fieldVal := val.Deref(field)
	if fieldVal == nil {
		return zed.Null, fmt.Errorf("field %q not found in %q", field, zson.FormatValue(val))
	}
	if fieldVal.IsNull() {
		return zed.Null, fmt.Errorf("field %q null in %q", field, zson.FormatValue(val))
	}
	return *fieldVal, nil
}

func FieldAsInt(val zed.Value, field string) (int64, error) {
	fieldVal, err := Field(val, field)
	if err != nil {
		return 0, err
	}
	if !zed.IsInteger(fieldVal.Type().ID()) {
		return 0, fmt.Errorf("field %q not an interger in %q", field, zson.FormatValue(val))
	}
	return fieldVal.AsInt(), nil
}

func FieldAsString(val zed.Value, field string) (string, error) {
	fieldVal, err := Field(val, field)
	if err != nil {
		return "", err
	}
	if fieldVal.Type().ID() != zed.IDString {
		return "", fmt.Errorf("field %q not a string in %q", field, zson.FormatValue(val))
	}
	return fieldVal.AsString(), nil
}
