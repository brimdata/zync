package zinger

import (
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-avro/avro"
	"github.com/mccanne/zinger/pkg/registry"
	"github.com/mccanne/zinger/pkg/zavro"
	"github.com/mccanne/zq/zng"
	"github.com/mccanne/zq/zng/resolver"
)

type Producer struct {
	sync.Mutex
	// For now we use SyncProducer to provide simplicity and an easy-to-understand
	// example.  Before putting into production or any perf testing, we need to
	// change this to async and probably refactor the data path to have a
	// seprate Producer thread for each http connection (and a shared schema
	// manager).  For now, any parallel connections share a single producer.
	// XXX note: change this to sarama.AsyncProducer
	Producer sarama.SyncProducer
	Context *resolver.Context
	// For now there is a single topic written to.  We can add support
	// later to route different records to different topics based on rules.
	topic     string
	namespace string
	registry  *registry.Connection
	// mapper translates zng descriptor IDs to kafka/avro schema IDs
	mapper map[int]int
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
	p, err := sarama.NewSyncProducer(servers, config)
	if err != nil {
		return nil, err
	}
	return &Producer{
		Producer:  p,
		Context:  resolver.NewContext(),
		registry:  reg,
		topic:     topic,
		namespace: namespace,
		mapper:    make(map[int]int),
	}, nil
}

func (p *Producer) Write(rec *zng.Record) error {
	id := rec.Type.ID()
	// For now wrap a lock around the entire creation of a new schema.
	// This would be more efficient with a condition variable where the
	// first arriver gets to create the schema.
	p.Lock()
	kid, ok := p.mapper[id]
	if !ok {
		s := zavro.GenSchema(rec.Type, p.namespace)
		record, ok := s.(*avro.RecordSchema)
		if !ok {
			p.Unlock()
			return errors.New("internal error: avro schema not of type record")
		}
		schema, err := json.Marshal(record)
		if err != nil {
			log.Println("schema creation error:", err)
			p.Unlock()
			return err
		}
		kid, err = p.registry.Create(schema)
		if err != nil {
			p.Unlock()
			return err
		}
		p.mapper[id] = kid
		log.Println("new schema:", kid, string(schema))
	}
	p.Unlock()
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
