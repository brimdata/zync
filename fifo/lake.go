package fifo

import (
	"context"
	"errors"

	"github.com/brimdata/zed/compiler"
	"github.com/brimdata/zed/driver"
	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zio"
	"github.com/brimdata/zed/zng"
	"github.com/brimdata/zed/zson"
	"go.uber.org/zap"
)

type Lake struct {
	pool string
}

func NewLake(pool string) *Lake {
	return &Lake{
		pool: pool,
	}
}

//XXX run a synchronous query on the lake and return results as a batch
func (l *Lake) Query(q string) (zbuf.Batch, error) {
	//XXX
	return nil, nil
}

//XXX this should return commit ID
func (l *Lake) LoadBatch(zbuf.Batch) error {
	return nil
}

func RunLocalQuery(zctx *zson.Context, batch zbuf.Batch, query string) (zbuf.Batch, error) {
	array, ok := batch.(*zbuf.Array)
	if !ok {
		//XXX
		return nil, errors.New("internal error: RunLocalQuery: batch must be a zbuf.Array")
	}
	//XXX this stuff should be wrapped into an easier entry point
	program := compiler.MustParseProc(query)
	result := &queryResult{}
	if err := driver.Copy(context.TODO(), result, program, zctx, array, zap.NewNop()); err != nil {
		return nil, err
	}
	return result, nil
}

type queryResult struct {
	zbuf.Array
}

var _ zio.Writer = (*queryResult)(nil)

func (q *queryResult) Write(rec *zng.Record) error {
	q.Append(rec)
	return nil
}
