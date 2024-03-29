package etl

import (
	"context"
	"fmt"
	"strings"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/compiler"
	lakeapi "github.com/brimdata/zed/lake/api"
	"github.com/brimdata/zed/runtime"
	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zio"
	"github.com/brimdata/zed/zio/zsonio"
	"github.com/brimdata/zed/zson"
)

type Pipeline struct {
	zctx       *zed.Context
	transform  *Transform
	outputPool *Pool
	inputPools map[string]*Pool
	cursors    map[string]int64
	doneType   zed.Type
}

func NewPipeline(ctx context.Context, transform *Transform, service lakeapi.Interface) (*Pipeline, error) {
	zctx := zed.NewContext()
	doneType, err := zson.ParseType(zctx, "{kafka:{topic:string,offset:int64}}(=done)")
	if err != nil {
		return nil, err
	}
	p := &Pipeline{
		zctx:       zctx,
		transform:  transform,
		inputPools: make(map[string]*Pool),
		cursors:    make(map[string]int64),
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
	batch, err := pool.Query(ctx, zeds[0])
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

// XXX TBD: this is currently taking all records for an update in memory.
// We need to work out a streaming model so we can do a very large updates
// that could involve large anti-joins and spills and we stream the output
// to the target pool.  This will scale fine since the anti-join design
// allows any commits to be reliably accounted for on an interruption and restart.
func (p *Pipeline) writeToOutputPool(ctx context.Context, batch *zbuf.Array) error {
	offsets, err := p.outputPool.NextProducerOffsets(ctx)
	if err != nil {
		return err
	}
	out, err := insertOffsets(ctx, p.zctx, p.doneType, batch, offsets)
	if err != nil {
		return err
	}
	for _, rec := range batch.Values() {
		//XXX This still doesn't work with the zctx bug fix.  See issue #31
		//if vals[k].Type == p.doneType {
		//	out.Append(&vals[k])
		//}
		if named, ok := rec.Type().(*zed.TypeNamed); ok && named.Name == "done" {
			out.Append(rec)
		}
		if extra := rec.Deref("left"); extra != nil {
			out.Append(*extra)
		}
		if extra := rec.Deref("right"); extra != nil {
			out.Append(*extra)
		}
	}
	//XXX We need to track the commitID and use new commit-only-if
	// constraint and recompute offsets if needed.  See zync issue #16.
	commit, err := p.outputPool.LoadBatch(ctx, p.zctx, out)
	if err != nil {
		return err
	}
	batchLen := len(out.Values())
	fmt.Printf("commit %s %d record%s\n", commit, batchLen, plural(batchLen))
	return nil
}

func insertOffsets(ctx context.Context, zctx *zed.Context, doneType zed.Type, batch zbuf.Batch, offsets map[string]int64) (*zbuf.Array, error) {
	// It will be more efficient to compute the new kafka output offsets using
	// flow count() but that is not implemented yet.  Instead, we just format
	// up some ZSON that can then insert the proper offsets in each record.
	var zsons strings.Builder
	for _, rec := range batch.Values() {
		// This pointer comparison should work but it doesn't right now.
		// Are they all allocated in the same zctx?
		//if vals[k].Type == doneType {
		//	continue
		//}
		if named, ok := rec.Type().(*zed.TypeNamed); ok && named.Name == "done" {
			continue
		}
		if rec.Deref("left") != nil {
			continue
		}
		topic, _, err := getKafkaMeta(rec)
		if err != nil {
			return nil, err
		}
		off := offsets[topic]
		zsons.WriteString(fmt.Sprintf("{rec:%s,offset:%d}\n", zson.FormatValue(rec), off))
		offsets[topic] = off + 1
	}
	comp := compiler.NewCompiler()
	program, err := comp.Parse("rec.kafka.offset:=offset | yield rec")
	if err != nil {
		return nil, err
	}
	reader := zsonio.NewReader(zctx, strings.NewReader(zsons.String()))
	q, err := runtime.CompileQuery(ctx, zctx, comp, program, []zio.Reader{reader})
	if err != nil {
		return nil, err
	}
	defer q.Pull(true)
	return NewArrayFromReader(zbuf.PullerReader(q))
}

func getKafkaMeta(rec zed.Value) (string, int64, error) {
	// XXX this API should be simplified in zed package
	kafkaRec := rec.Deref("kafka")
	if kafkaRec == nil {
		return "", 0, fmt.Errorf("value missing 'kafka' metadata field: %s", zson.FormatValue(rec))
	}
	topic, err := FieldAsString(*kafkaRec, "topic")
	if err != nil {
		return "", 0, err
	}
	offset, err := FieldAsInt(*kafkaRec, "offset")
	if err != nil {
		return "", 0, err
	}
	return topic, int64(offset), nil
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
