package fifo

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zcode"
	"github.com/brimdata/zed/zio"
	"github.com/brimdata/zed/zson"
	"github.com/brimdata/zync/zavro"
	"github.com/go-avro/avro"
	"github.com/riferrei/srclient"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Consumer struct {
	zctx     *zed.Context
	kclient  *kgo.Client
	registry *srclient.SchemaRegistryClient
	topic    string
	metaType zed.Type
	types    map[zed.Type]map[zed.Type]zed.Type
	schemas  map[int]typeSchema
}

type typeSchema struct {
	zed.Type
	avro.Schema
}

func NewConsumer(zctx *zed.Context, opts []kgo.Opt, reg *srclient.SchemaRegistryClient, topic string, startAt int64, meta bool) (*Consumer, error) {
	var metaType zed.Type
	if meta {
		var err error
		metaType, err = zson.ParseType(zctx, "{topic:string,partition:int64,offset:int64}")
		if err != nil {
			return nil, err
		}
	}
	opts = append([]kgo.Opt{
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().At(startAt)),
	}, opts...)
	kclient, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		zctx:     zctx,
		kclient:  kclient,
		registry: reg,
		topic:    topic,
		metaType: metaType,
		types:    make(map[zed.Type]map[zed.Type]zed.Type),
		schemas:  make(map[int]typeSchema),
	}, nil
}

func (c *Consumer) Close() {
	c.kclient.Close()
}

type Flusher interface {
	Flush() error
}

func (c *Consumer) Run(ctx context.Context, w zio.Writer, timeout time.Duration) error {
	return c.run(ctx, w, -1, timeout)
}

func (c *Consumer) Read(ctx context.Context, thresh int, timeout time.Duration) (*zbuf.Array, error) {
	var a zbuf.Array
	return &a, c.run(ctx, &a, thresh, timeout)
}

func (c *Consumer) run(ctx context.Context, w zio.Writer, thresh int, timeout time.Duration) error {
	var size int
	for {
		timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
		fetches := c.kclient.PollFetches(timeoutCtx)
		timedOut := timeoutCtx.Err() == context.DeadlineExceeded
		cancel()
		if err := ctx.Err(); err != nil {
			return err
		}
		if timedOut {
			return nil
		}
		cancel()
		for _, e := range fetches.Errors() {
			return fmt.Errorf("topic %s, partition %d: %w", e.Topic, e.Partition, e.Err)
		}
		for it := fetches.RecordIter(); !it.Done(); {
			rec, err := c.handle(it.Next())
			if err != nil {
				return err
			}
			if err := w.Write(rec); err != nil {
				return err
			}
			size += len(rec.Bytes)
		}
		if thresh > -1 && size > thresh {
			return nil
		}
	}
}

func (c *Consumer) handle(krec *kgo.Record) (*zed.Value, error) {
	key, err := c.decodeAvro(krec.Key)
	if err != nil {
		return nil, err
	}
	val, err := c.decodeAvro(krec.Value)
	if err != nil {
		return nil, err
	}
	outerType, err := c.outerType(key.Type, val.Type)
	if err != nil {
		return nil, err
	}
	var b zcode.Builder
	if c.metaType != nil {
		// kafka:{topic:string,partition:int64,offset:int64}
		b.BeginContainer()
		b.Append([]byte(krec.Topic))
		b.Append(zed.EncodeInt(int64(krec.Partition)))
		b.Append(zed.EncodeInt(krec.Offset))
		b.EndContainer()
	}
	b.Append(key.Bytes)
	b.Append(val.Bytes)
	return zed.NewValue(outerType, b.Bytes()), nil
}

func (c *Consumer) decodeAvro(b []byte) (zed.Value, error) {
	if len(b) == 0 {
		return zed.Value{Type: zed.TypeNull}, nil
	}
	if len(b) < 5 {
		return zed.Value{}, fmt.Errorf("Kafka-Avro header is too short: len %d", len(b))
	}
	schemaID := binary.BigEndian.Uint32(b[1:5])
	schema, typ, err := c.getSchema(int(schemaID))
	if err != nil {
		return zed.Value{}, fmt.Errorf("could not retrieve schema id %d: %w", schemaID, err)
	}
	bytes, err := zavro.Decode(b[5:], schema)
	if err != nil {
		return zed.Value{}, err
	}
	return *zed.NewValue(typ, bytes), nil
}

func (c *Consumer) getSchema(id int) (avro.Schema, zed.Type, error) {
	if both, ok := c.schemas[id]; ok {
		return both.Schema, both.Type, nil
	}
	schema, err := c.registry.GetSchema(id)
	if err != nil {
		return nil, nil, fmt.Errorf("could not retrieve schema id %d: %w", id, err)
	}
	avroSchema, err := avro.ParseSchema(schema.Schema())
	if err != nil {
		return nil, nil, err
	}
	typ, err := zavro.DecodeSchema(c.zctx, avroSchema)
	if err != nil {
		return nil, nil, err
	}
	c.schemas[id] = typeSchema{Type: typ, Schema: avroSchema}
	return avroSchema, typ, nil
}

func (c *Consumer) outerType(key, val zed.Type) (zed.Type, error) {
	m, ok := c.types[key]
	if !ok {
		c.makeType(key, val)
	} else if typ, ok := m[val]; ok {
		return typ, nil
	} else {
		c.makeType(key, val)
	}
	return c.types[key][val], nil
}

func (c *Consumer) makeType(key, val zed.Type) (*zed.TypeRecord, error) {
	cols := []zed.Column{
		zed.NewColumn("kafka", c.metaType),
		zed.NewColumn("key", key),
		zed.NewColumn("value", val),
	}
	if c.metaType == nil {
		cols = cols[1:]
	}
	typ, err := c.zctx.LookupTypeRecord(cols)
	if err != nil {
		return nil, err
	}
	m, ok := c.types[key]
	if !ok {
		m = make(map[zed.Type]zed.Type)
		c.types[key] = m
	}
	m[val] = typ
	return typ, nil
}

func (c *Consumer) Watermarks(ctx context.Context) (int64, int64, error) {
	client := kadm.NewClient(c.kclient)
	start, err := minOffset(client.ListStartOffsets(ctx, c.topic))
	if err != nil {
		return 0, 0, err
	}
	end, err := maxOffset(client.ListEndOffsets(ctx, c.topic))
	if err != nil {
		return 0, 0, err
	}
	return start, end, nil
}

func maxOffset(offsets kadm.ListedOffsets, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	if err := offsets.Error(); err != nil {
		return 0, err
	}
	max := int64(math.MinInt64)
	offsets.Each(func(l kadm.ListedOffset) {
		if l.Offset > max {
			max = l.Offset
		}
	})
	return max, nil
}

func minOffset(offsets kadm.ListedOffsets, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	if err := offsets.Error(); err != nil {
		return 0, err
	}
	min := int64(math.MaxInt64)
	offsets.Each(func(l kadm.ListedOffset) {
		if l.Offset < min {
			min = l.Offset
		}
	})
	return min, nil
}
