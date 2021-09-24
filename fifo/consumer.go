package fifo

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/brimdata/zed/zcode"
	"github.com/brimdata/zed/zio"
	"github.com/brimdata/zed/zng"
	"github.com/brimdata/zed/zson"
	"github.com/brimdata/zinger/zavro"
	"github.com/go-avro/avro"
	"github.com/riferrei/srclient"
	"github.com/segmentio/ksuid"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Consumer struct {
	consumer  *kafka.Consumer
	registry  *srclient.SchemaRegistryClient
	highWater kafka.Offset
	wrap      bool
	types     map[zng.Type]map[zng.Type]zng.Type
}

func NewConsumer(config *kafka.ConfigMap, reg *srclient.SchemaRegistryClient, topic string, wrap bool) (*Consumer, error) {
	if err := config.SetKey("group.id", ksuid.New().String()); err != nil {
		return nil, err
	}
	if err := config.SetKey("auto.offset.reset", "earliest"); err != nil {
		return nil, err
	}
	if err := config.SetKey("enable.auto.commit", false); err != nil {
		return nil, err
	}
	c, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}
	c.SubscribeTopics([]string{topic}, nil)
	// Get the last offset at start time and read up to that point.
	// XXX we should add an option to stream or stop at the end,
	// and should check that there is only one partition
	_, offset, err := c.QueryWatermarkOffsets(topic, 0, 5*1000)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		consumer:  c,
		registry:  reg,
		highWater: kafka.Offset(offset),
		wrap:      wrap,
		types:     make(map[zng.Type]map[zng.Type]zng.Type),
	}, nil
}

func (c *Consumer) Close() {
	c.consumer.Close()
}

func (c *Consumer) Run(zctx *zson.Context, w zio.Writer) error {
	metaType, err := zson.ParseType(zctx, "{topic:string,partition:int64,offset:int64}")
	if err != nil {
		return err
	}
	for {
		msg, err := c.consumer.ReadMessage(time.Second)
		if err != nil {
			//XXX
			fmt.Printf("Error consuming the message: %v (%v)\n", err, msg)
		} else {
			key, err := c.decodeAvro(zctx, msg.Key)
			if err != nil {
				return err
			}
			val, err := c.decodeAvro(zctx, msg.Value)
			if err != nil {
				return err
			}
			rec, err := c.wrapRecord(zctx, metaType, key, val, msg.TopicPartition)
			if err != nil {
				return err
			}
			if err := w.Write(rec); err != nil {
				return err
			}
			if (msg.TopicPartition.Offset + 1) >= c.highWater {
				break
			}
		}
	}
	return nil
}

func (c *Consumer) wrapRecord(zctx *zson.Context, metaType zng.Type, key, val zng.Value, meta kafka.TopicPartition) (*zng.Record, error) {
	outerType, err := c.outerType(zctx, metaType, key.Type, val.Type)
	if err != nil {
		return nil, err
	}
	var b zcode.Builder
	// {topic:string,partition:int64,offset:int64}
	b.BeginContainer()
	b.AppendPrimitive([]byte(*meta.Topic))
	b.AppendPrimitive(zng.EncodeInt(int64(meta.Partition)))
	b.AppendPrimitive(zng.EncodeInt(int64(meta.Offset)))
	b.EndContainer()
	b.AppendContainer(key.Bytes)
	b.AppendContainer(val.Bytes)
	return zng.NewRecord(outerType, b.Bytes()), nil
}

func (c *Consumer) decodeAvro(zctx *zson.Context, b []byte) (zng.Value, error) {
	if len(b) == 0 {
		return zng.Value{Type: zng.TypeNull}, nil
	}
	if len(b) < 6 {
		return zng.Value{}, fmt.Errorf("bad kafka-avro value in topic: len %d", len(b))
	}
	schemaID := binary.BigEndian.Uint32(b[1:5])
	schema, err := c.registry.GetSchema(int(schemaID))
	if err != nil {
		return zng.Value{}, fmt.Errorf("could not retrieve schema id %d: %w", schemaID, err)
	}
	avroSchema, err := avro.ParseSchema(schema.Schema())
	if err != nil {
		return zng.Value{}, err
	}
	typ, err := zavro.DecodeSchema(zctx, avroSchema)
	if err != nil {
		return zng.Value{}, err
	}
	recType := zng.TypeRecordOf(typ)
	if recType == nil {
		return zng.Value{}, errors.New("avro schema not a Zed record")
	}
	avroTypeRecord, ok := avroSchema.(*avro.RecordSchema)
	if !ok {
		return zng.Value{}, errors.New("schema not an avrod record")
	}
	bytes, err := zavro.Decode(b[5:], avroTypeRecord)
	if err != nil {
		return zng.Value{}, err
	}
	return zng.Value{recType, bytes}, nil
}

func (c *Consumer) outerType(zctx *zson.Context, meta, key, val zng.Type) (zng.Type, error) {
	m, ok := c.types[key]
	if !ok {
		c.makeType(zctx, meta, key, val)
	} else if typ, ok := m[val]; ok {
		return typ, nil
	} else {
		c.makeType(zctx, meta, key, val)
	}
	return c.types[key][val], nil
}

func (c *Consumer) makeType(zctx *zson.Context, meta, key, val zng.Type) (*zng.TypeRecord, error) {
	cols := []zng.Column{
		{"kafka", meta},
		{"key", key},
		{"value", val},
	}
	typ, err := zctx.LookupTypeRecord(cols)
	if err != nil {
		return nil, err
	}
	m, ok := c.types[key]
	if !ok {
		m = make(map[zng.Type]zng.Type)
		c.types[key] = m
	}
	m[val] = typ
	return typ, nil
}
