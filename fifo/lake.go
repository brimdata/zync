package fifo

import (
	"context"
	"errors"
	"fmt"

	// XXX we should reconsider packages names so we don't have two apis

	"github.com/brimdata/zed/api"
	"github.com/brimdata/zed/compiler"
	"github.com/brimdata/zed/driver"
	lakeapi "github.com/brimdata/zed/lake/api"
	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zson"
	"github.com/segmentio/ksuid"
	"go.uber.org/zap"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Lake struct {
	service *lakeapi.RemoteSession
	pool    string
	poolID  ksuid.KSUID
}

func NewLake(pool string, server *lakeapi.RemoteSession) (*Lake, error) {
	//XXX should check that pool key is kafka.offset desc,
	// it will be easy to forget this when creating zinger pools
	poolID, err := server.PoolID(context.TODO(), pool)
	if err != nil {
		return nil, err
	}
	return &Lake{
		pool:    pool,
		poolID:  poolID,
		service: server,
	}, nil
}

//XXX run a synchronous query on the lake and return results as a batch
func (l *Lake) Query(q string) (zbuf.Array, error) {
	//XXX use commitish instead of pool
	query := fmt.Sprintf("from '%s' | %s", l.pool, q)
	//XXX need to make this API easier
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
	//XXX run a query against the lake to get the max output offset
	// we assume the pool-key is kafka.offset so we just run a head 1
	//XXX this should be extended to query with a commitID so we can
	// detect multiple writers.
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
	//XXX run a query against the lake to get the max output offset
	// we assume the pool-key is kafka.offset so we just run a head 1
	//XXX this should be extended to query with a commitID so we can
	// detect multiple writers.
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
	query := fmt.Sprintf("kafka.offset >= %d | head %d | sort kafka.offset", offset, size)
	return l.Query(query)
}

func RunLocalQuery(zctx *zson.Context, batch zbuf.Array, query string) (zbuf.Array, error) {
	//XXX this stuff should be wrapped into an easier API csall
	program := compiler.MustParseProc(query)
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
