package fifo

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

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
}

func NewConsumer(config *kafka.ConfigMap, reg *srclient.SchemaRegistryClient, topic string) (*Consumer, error) {
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
	}, nil
}

func (c *Consumer) Close() {
	c.consumer.Close()
}

func (c *Consumer) Run(zctx *zson.Context, w zio.Writer) error {
	for {
		msg, err := c.consumer.ReadMessage(time.Second)
		if err == nil {
			if len(msg.Value) < 6 {
				return fmt.Errorf("bad kafka-avro value in topic: len %d", len(msg.Value))
			}
			schemaID := binary.BigEndian.Uint32(msg.Value[1:5])
			schema, err := c.registry.GetSchema(int(schemaID))
			if err != nil {
				return fmt.Errorf("could not retrieve schema id %d: %w", schemaID, err)
			}
			avroSchema, err := avro.ParseSchema(schema.Schema())
			if err != nil {
				return err
			}
			typ, err := zavro.DecodeSchema(zctx, avroSchema)
			if err != nil {
				return err
			}
			recType := zng.TypeRecordOf(typ)
			if recType == nil {
				return errors.New("avro schema not a Zed record")
			}
			avroTypeRecord, ok := avroSchema.(*avro.RecordSchema)
			if !ok {
				return errors.New("schema not an avrod record")
			}
			bytes, err := zavro.Decode(msg.Value[5:], avroTypeRecord)
			if err != nil {
				return err
			}
			rec := zng.NewRecord(recType, bytes)
			if err := w.Write(rec); err != nil {
				return err
			}
			if (msg.TopicPartition.Offset + 1) >= c.highWater {
				break
			}
		} else {
			//XXX
			fmt.Printf("Error consuming the message: %v (%v)\n", err, msg)
		}
	}
	return nil
}
