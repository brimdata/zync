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
)

type Lake struct {
	service *lakeapi.RemoteSession
	pool    string
	poolID  ksuid.KSUID
}

func NewLake(pool string, server *lakeapi.RemoteSession) (*Lake, error) {
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

func RunLocalQuery(zctx *zson.Context, batch zbuf.Array, query string) (zbuf.Array, error) {
	//XXX this stuff should be wrapped into an easier entry point
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
