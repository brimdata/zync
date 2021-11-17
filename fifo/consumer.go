package fifo

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zcode"
	"github.com/brimdata/zed/zio"
	"github.com/brimdata/zed/zson"
	"github.com/brimdata/zync/zavro"
	"github.com/go-avro/avro"
	"github.com/riferrei/srclient"
	"github.com/segmentio/ksuid"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Consumer struct {
	zctx     *zed.Context
	consumer *kafka.Consumer
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

func NewConsumer(zctx *zed.Context, config *kafka.ConfigMap, reg *srclient.SchemaRegistryClient, topic, group string, startAt kafka.Offset, meta bool) (*Consumer, error) {
	var metaType zed.Type
	if meta {
		var err error
		metaType, err = zson.ParseType(zctx, "{topic:string,partition:int64,offset:int64}")
		if err != nil {
			return nil, err
		}
	}
	if group == "" {
		group = ksuid.New().String()
	}
	if err := config.SetKey("group.id", group); err != nil {
		return nil, err
	}
	if err := config.SetKey("go.events.channel.enable", true); err != nil {
		return nil, err
	}
	// Note that we do not conifgure "auto.offset.reset" since we assign
	// the topic offset explicitly instead of doing a dynamic subscribe.
	if err := config.SetKey("enable.auto.commit", false); err != nil {
		return nil, err
	}
	c, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}
	partition := kafka.TopicPartition{
		Topic:  &topic,
		Offset: kafka.Offset(startAt),
	}
	if err := c.Assign([]kafka.TopicPartition{partition}); err != nil {
		return nil, err
	}
	return &Consumer{
		zctx:     zctx,
		consumer: c,
		registry: reg,
		topic:    topic,
		metaType: metaType,
		types:    make(map[zed.Type]map[zed.Type]zed.Type),
		schemas:  make(map[int]typeSchema),
	}, nil
}

func (c *Consumer) Close() {
	c.consumer.Close()
}

type Flusher interface {
	Flush() error
}

func (c *Consumer) Run(ctx context.Context, w zio.Writer, timeout time.Duration) error {
	events := c.consumer.Events()
	for {
		select {
		case ev := <-events:
			if ev == nil {
				// channel closed
				return nil
			}
			rec, err := c.handle(ev)
			if err != nil {
				return err
			}
			if rec == nil {
				// unknown event
				continue
			}
			if err := w.Write(rec); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(timeout):
			return nil
		}
	}
}

func (c *Consumer) Read(ctx context.Context, thresh int, timeout time.Duration) (zbuf.Array, error) {
	var batch zbuf.Array
	var size int
	events := c.consumer.Events()
	for {
		select {
		case ev := <-events:
			if ev == nil {
				// channel closed
				return batch, nil
			}
			rec, err := c.handle(ev)
			if err != nil {
				return nil, err
			}
			if rec == nil {
				// unknown event
				continue
			}
			batch.Append(rec)
			size += len(rec.Bytes)
			if size > thresh {
				return batch, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(timeout):
			return batch, nil
		}
	}
}

func (c *Consumer) handle(ev kafka.Event) (*zed.Value, error) {
	switch ev := ev.(type) {
	case kafka.Error:
		return nil, ev
	case *kafka.Message:
		key, err := c.decodeAvro(ev.Key)
		if err != nil {
			return nil, err
		}
		val, err := c.decodeAvro(ev.Value)
		if err != nil {
			return nil, err
		}
		return c.wrapRecord(key, val, ev.TopicPartition)
	default:
		return nil, nil
	}
}

func (c *Consumer) wrapRecord(key, val zed.Value, meta kafka.TopicPartition) (*zed.Value, error) {
	outerType, err := c.outerType(key.Type, val.Type)
	if err != nil {
		return nil, err
	}
	var b zcode.Builder
	if c.metaType != nil {
		// kafka:{topic:string,partition:int64,offset:int64}
		b.BeginContainer()
		b.AppendPrimitive([]byte(*meta.Topic))
		b.AppendPrimitive(zed.EncodeInt(int64(meta.Partition)))
		b.AppendPrimitive(zed.EncodeInt(int64(meta.Offset)))
		b.EndContainer()
	}
	b.AppendContainer(key.Bytes)
	b.AppendContainer(val.Bytes)
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
	return zed.Value{typ, bytes}, nil
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
		{"kafka", c.metaType},
		{"key", key},
		{"value", val},
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

func (c *Consumer) Watermarks() (int64, int64, error) {
	return c.consumer.QueryWatermarkOffsets(c.topic, 0, 5*1000)
}
