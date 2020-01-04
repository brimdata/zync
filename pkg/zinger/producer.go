package zinger

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-avro/avro"
	"github.com/mccanne/zinger/pkg/registry"
	"github.com/mccanne/zinger/zavro"
	"github.com/mccanne/zq/pkg/zng"
	"github.com/mccanne/zq/pkg/zng/resolver"
)

type Producer struct {
	//XXX don't use SyncProducer
	Producer sarama.SyncProducer
	Resolver *resolver.Table
	// For now there is a single topic written to.  We camn add support
	// later to route different records to different topics based on rules.
	topic     string
	namespace string
	registry  *registry.Connection
	schemas   map[int]avro.Schema
	mapper    map[int]int
}

func NewProducer(servers []string, reg *registry.Connection, topic, namespace string) (*Producer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_1_0
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Compression = sarama.CompressionNone
	config.Producer.MaxMessageBytes = 10000000
	config.Producer.Retry.Max = 10
	config.Producer.Retry.Backoff = 1000 * time.Millisecond
	//XXX don't use sync
	p, err := sarama.NewSyncProducer(servers, config)
	if err != nil {
		return nil, err
	}
	return &Producer{
		Producer:  p,
		Resolver:  resolver.NewTable(),
		registry:  reg,
		topic:     topic,
		namespace: namespace,
		schemas:   make(map[int]avro.Schema),
		mapper:    make(map[int]int),
	}, nil
}

func (p *Producer) Write(rec *zng.Record) error {
	id := rec.Descriptor.ID
	kid, ok := p.mapper[id]
	if !ok {
		s := zavro.GenSchema(rec.Descriptor.Type, p.namespace)
		record, ok := s.(*avro.RecordSchema)
		if !ok {
			return errors.New("internal error: avro schema not of type record")
		}
		schema, err := json.Marshal(record)
		if err != nil {
			fmt.Println("schema creation error:", err) // logger
			return err
		}
		kid, err = p.registry.Create(schema)
		if err != nil {
			return err
		}
		p.mapper[id] = kid
		p.schemas[kid] = s
		fmt.Println("new schema:", kid, string(schema)) // logger
	}
	b, err := zavro.Encode(nil, uint32(kid), rec)
	if err != nil {
		return err
	}
	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Value: sarama.ByteEncoder(b),
	}
	_, _, err = p.Producer.SendMessage(msg)
	return err
}
