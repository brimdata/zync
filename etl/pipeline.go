package etl

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/driver"
	lakeapi "github.com/brimdata/zed/lake/api"
	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zson"
	"go.uber.org/zap"
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
	zeds, err := p.Build()
	if err != nil {
		return 0, err
	}
	if len(zeds) == 0 {
		panic("no zed")
	}
	if len(zeds) != 1 {
		panic("TBD: only one ETL works right now")
	}
	batch, err := pool.QueryWithFrom(zeds[0])
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

// Currently, we take a brute force approach and compile a Zed script for
// each output topic from scratch and we scan the entire range of each pool
// for each step.  Later we can range-limit the scans based on the cursor of
// each topic (note in this more generalized approach the cursor of an input
// topic requires info from possible more than one output topic, but maybe
// we won't handle this more generalized case until later).  For now, we require
// all the input topics go through ETLs that land in the same *pool* but do not
// require that they all land in the same output topic.  This way a cursor
// query on the *pool* can reliably give us the cursor and the completed offsets
// for anti-join.

func (p *Pipeline) Build() ([]string, error) {
	routes, err := newRoutes(p.transform)
	if err != nil {
		return nil, err
	}
	// First, for each output topic, we compute all of the input topics
	// needed by all of the ETLs to that output.  Note that an input
	// topic may be routed to multiple output topics but all of those
	// topics must be in the same pool (XXX add a check for this).
	for _, etl := range p.transform.ETLs {
		switch etl.Type {
		case "denorm":
			if etl.Left == "" || etl.Right == "" {
				return nil, errors.New("both 'left' and 'right' topics must be specified for denorm ETL")
			}
			if etl.In != "" {
				return nil, errors.New("'in' topic cannot be specified for denorm ETL")
			}
			if err := routes.enter(etl.Left, etl.Out); err != nil {
				return nil, err
			}
			if err := routes.enter(etl.Right, etl.Out); err != nil {
				return nil, err
			}
		case "stateless":
			if etl.In == "" {
				return nil, errors.New("'in' topic must be specified for stateless ETL")
			}
			if etl.Left != "" || etl.Right != "" {
				return nil, errors.New("'left' or 'right' topic cannot be specified for stateless ETL")
			}
			if err := routes.enter(etl.In, etl.Out); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unknown ETL type: %q", etl.Type)
		}
	}
	// For every input topic, we now know what output pool it is rounted to.
	// There may be multiple output topics for a given input, but they all
	// land in the same pool.

	// For each output topic, we'll build a Zed for all the ETLs that
	// land on that topic.   It's okay to have multiple ETLs landing
	// on the same topic (e.g., multiple ETLs from different tables
	// land on one denormalized table).

	var zeds []string
	for _, outputTopic := range routes.Outputs() {
		var etls []Rule
		for _, etl := range p.transform.ETLs {
			if etl.Out == outputTopic {
				etls = append(etls, etl)
			}
		}
		inputTopics := routes.InputsOf(outputTopic)
		s, err := buildZed(inputTopics, outputTopic, routes, etls)
		if err != nil {
			return nil, err
		}
		zeds = append(zeds, s)
	}
	return zeds, nil
}

//XXX TBD: this is currently taking all records for an update in memory.
// We need to work out a streaming model so we can do a very large updates
// that could involve large anti-joins and spills and we stream the output
// to the target pool.  This will scale fine since the anti-join design
// allows any commits to be reliably accounted for on an interruption and restart.
func (p *Pipeline) writeToOutputPool(ctx context.Context, batch zbuf.Array) error {
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
		if typedef, ok := vals[k].Type.(*zed.TypeAlias); ok && typedef.Name == "done" {
			out.Append(&vals[k])
		}
		if extra := hasExtra(&vals[k], "left"); extra != nil {
			out.Append(extra)
		}
		if extra := hasExtra(&vals[k], "right"); extra != nil {
			out.Append(extra)
		}
	}
	//XXX We need to track the commitID and use new commit-only-if
	// constraint and recompute offsets if needed.  See zync issue #16.
	commit, err := p.outputPool.LoadBatch(out)
	if err != nil {
		return err
	}
	batchLen := len(out.Values())
	fmt.Printf("commit %s %d record%s\n", commit, batchLen, plural(batchLen))
	return nil
}

func hasExtra(val *zed.Value, which string) *zed.Value {
	extra, err := val.Access(which)
	if err != nil {
		return nil
	}
	return &extra
}

func insertOffsets(ctx context.Context, zctx *zed.Context, doneType zed.Type, batch zbuf.Array, offsets map[string]kafka.Offset) (zbuf.Array, error) {
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
		if typedef, ok := vals[k].Type.(*zed.TypeAlias); ok && typedef.Name == "done" {
			continue
		}
		if extra := hasExtra(&vals[k], "left"); extra != nil {
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
	reader := zson.NewReader(strings.NewReader(zsons.String()), zctx)
	program, err := parse("rec.kafka.offset:=offset | yield rec")
	if err != nil {
		return nil, err
	}
	d := &batchDriver{}
	err = driver.RunWithReader(ctx, d, program, zctx, reader, zap.NewNop())
	if err != nil {
		return nil, err
	}
	return d.Array, nil
}

func getKafkaMeta(rec *zed.Value) (string, kafka.Offset, error) {
	// XXX this API should be simplified in zed package
	kafkaRec, err := rec.Access("kafka")
	if err != nil {
		//XXX should do this formatting from above since
		// the callee may expect errors in some cases...
		s, err := zson.FormatValue(*rec)
		if err != nil {
			// This should not happen.
			err = fmt.Errorf("[ERR! %w]", err)
		}
		return "", 0, fmt.Errorf("value missing 'kafka' metadata field: %s", s)
	}
	topic, err := kafkaRec.AccessString("topic")
	if err != nil {
		s, err := zson.FormatValue(*rec)
		if err != nil {
			// This should not happen.
			err = fmt.Errorf("[ERR! %w]", err)
		}
		return "", 0, fmt.Errorf("value missing 'kafka.topic' metadata field: %s", s)
	}
	off, err := kafkaRec.AccessInt("offset")
	if err != nil {
		s, err := zson.FormatValue(*rec)
		if err != nil {
			// This should not happen.
			err = fmt.Errorf("[ERR! %w]", err)
		}
		return "", 0, fmt.Errorf("value missing 'kafka.offset' metadata field: %s", s)
	}
	return topic, kafka.Offset(off), nil
}

func doneType(zctx *zed.Context) (zed.Type, error) {
	recType, err := zctx.LookupTypeRecord([]zed.Column{
		{"topic", zed.TypeString},
		{"offset", zed.TypeInt64},
	})
	if err != nil {
		return nil, err
	}
	return zctx.LookupTypeAlias("done", recType)
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
