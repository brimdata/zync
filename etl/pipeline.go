package etl

import (
	"context"
	"fmt"
	"strings"

	"github.com/brimdata/zed"
	lakeapi "github.com/brimdata/zed/lake/api"
	"github.com/brimdata/zed/runtime"
	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zio/zsonio"
	"github.com/brimdata/zed/zson"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Pipeline struct {
	zctx       *zed.Context
	transform  *Transform
	outputPool *Pool
	inputPools map[string]*Pool
	cursors    map[string]kafka.Offset
	doneType   zed.Type
}

func NewPipeline(ctx context.Context, transform *Transform, service lakeapi.Interface) (*Pipeline, error) {
	zctx := zed.NewContext()
	doneType, err := doneType(zctx)
	if err != nil {
		return nil, err
	}
	p := &Pipeline{
		zctx:       zctx,
		transform:  transform,
		inputPools: make(map[string]*Pool),
		cursors:    make(map[string]kafka.Offset),
		doneType:   doneType,
	}
	if p.outputPool, err = OpenPool(ctx, transform.Output.Pool, service); err != nil {
		return nil, err
	}
	for _, route := range transform.Inputs {
		name := route.Pool
		if _, ok := p.inputPools[name]; ok {
			continue
		}
		pool, err := OpenPool(ctx, name, service)
		if err != nil {
			return nil, err
		}
		p.inputPools[name] = pool
	}
	return p, nil
}

func (p *Pipeline) Run(ctx context.Context) (int, error) {
	pool := p.getInputPool()
	zeds, err := Build(p.transform)
	if err != nil {
		return 0, err
	}
	if len(zeds) == 0 {
		panic("no zed")
	}
	if len(zeds) != 1 {
		panic("TBD: only one ETL works right now")
	}
	batch, err := pool.Query(zeds[0])
	if err != nil {
		return 0, err
	}
	n := len(batch.Values())
	if n > 0 {
		err = p.writeToOutputPool(ctx, batch)
	}
	return n, err
}

func (p *Pipeline) getInputPool() *Pool {
	for _, pool := range p.inputPools {
		return pool
	}
	panic("no input pool")
}

/*
func (p *Pipeline) loadCursors() error {
	_, err := p.outputPool.Query("is(<done>) | max(offset) by topic")
	return err
}
*/

//XXX TBD: this is currently taking all records for an update in memory.
// We need to work out a streaming model so we can do a very large updates
// that could involve large anti-joins and spills and we stream the output
// to the target pool.  This will scale fine since the anti-join design
// allows any commits to be reliably accounted for on an interruption and restart.
func (p *Pipeline) writeToOutputPool(ctx context.Context, batch *zbuf.Array) error {
	offsets, err := p.outputPool.NextProducerOffsets()
	if err != nil {
		return err
	}
	out, err := insertOffsets(ctx, p.zctx, p.doneType, batch, offsets)
	if err != nil {
		return err
	}
	vals := batch.Values()
	for k := range vals {
		//XXX This still doesn't work with the zctx bug fix.  See issue #31
		//if vals[k].Type == p.doneType {
		//	out.Append(&vals[k])
		//}
		if typedef, ok := vals[k].Type.(*zed.TypeNamed); ok && typedef.Name == "done" {
			out.Append(&vals[k])
		}
		if extra := vals[k].Deref("left"); extra != nil {
			out.Append(extra)
		}
		if extra := vals[k].Deref("right"); extra != nil {
			out.Append(extra)
		}
	}
	//XXX We need to track the commitID and use new commit-only-if
	// constraint and recompute offsets if needed.  See zync issue #16.
	commit, err := p.outputPool.LoadBatch(p.zctx, out)
	if err != nil {
		return err
	}
	batchLen := len(out.Values())
	fmt.Printf("commit %s %d record%s\n", commit, batchLen, plural(batchLen))
	return nil
}

func insertOffsets(ctx context.Context, zctx *zed.Context, doneType zed.Type, batch zbuf.Batch, offsets map[string]kafka.Offset) (*zbuf.Array, error) {
	// It will be more efficient to compute the new kafka output offsets using
	// flow count() but that is not implemented yet.  Instead, we just format
	// up some ZSON that can then insert the proper offsets in each record.
	var zsons strings.Builder
	vals := batch.Values()
	for k := range vals {
		// This pointer comparison should work but it doesn't right now.
		// Are they all allocated in the same zctx?
		//if vals[k].Type == doneType {
		//	continue
		//}
		if typedef, ok := vals[k].Type.(*zed.TypeNamed); ok && typedef.Name == "done" {
			continue
		}
		if vals[k].Deref("left") != nil {
			continue
		}
		rec, err := zson.FormatValue(vals[k])
		if err != nil {
			return nil, err
		}
		topic, _, err := getKafkaMeta(&vals[k])
		if err != nil {
			return nil, err
		}
		off := offsets[topic]
		zsons.WriteString(fmt.Sprintf("{rec:%s,offset:%d}\n", rec, off))
		offsets[topic] = off + 1
	}
	reader := zsonio.NewReader(strings.NewReader(zsons.String()), zctx)
	program, err := parse("rec.kafka.offset:=offset | yield rec")
	if err != nil {
		return nil, err
	}
	q, err := runtime.NewQueryOnReader(ctx, zctx, program, reader, nil)
	if err != nil {
		return nil, err
	}
	return NewArrayFromReader(q.AsReader())
}

func getKafkaMeta(rec *zed.Value) (string, kafka.Offset, error) {
	// XXX this API should be simplified in zed package
	kafkaRec := rec.Deref("kafka")
	if kafkaRec == nil {
		return "", 0, fmt.Errorf("value missing 'kafka' metadata field: %s", zson.MustFormatValue(*rec))
	}
	topic, err := FieldAsString(kafkaRec, "topic")
	if err != nil {
		return "", 0, err
	}
	offset, err := FieldAsInt(kafkaRec, "offset")
	if err != nil {
		return "", 0, err
	}
	return topic, kafka.Offset(offset), nil
}

func doneType(zctx *zed.Context) (zed.Type, error) {
	recType, err := zctx.LookupTypeRecord([]zed.Column{
		zed.NewColumn("topic", zed.TypeString),
		zed.NewColumn("offset", zed.TypeInt64),
	})
	if err != nil {
		return nil, err
	}
	return zctx.LookupTypeNamed("done", recType)
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
