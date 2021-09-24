package fifo

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/brimdata/zed/zio"
	"github.com/brimdata/zed/zng"
	"github.com/brimdata/zinger/zavro"
	"github.com/go-avro/avro"
	"github.com/riferrei/srclient"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Producer struct {
	producer  *kafka.Producer
	registry  *srclient.SchemaRegistryClient
	topic     string
	namespace string
	mapper    map[zng.Type]int
}

func NewProducer(config *kafka.ConfigMap, reg *srclient.SchemaRegistryClient, topic, namespace string) (*Producer, error) {
	p, err := kafka.NewProducer(config)
	if err != nil {
		return nil, err
	}
	return &Producer{
		producer:  p,
		registry:  reg,
		topic:     topic,
		namespace: namespace,
		mapper:    make(map[zng.Type]int),
	}, nil
}

func (p *Producer) Run(reader zio.Reader) error {
	go func() {
		//XXX need to handle errors gracefully from this goroutine
		for e := range p.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					//fmt.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
					//	*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()
	var n int
	var closeErr error
	for {
		rec, err := reader.Read()
		if rec == nil || err != nil {
			closeErr = err
			break
		}
		if err := p.write(rec); err != nil {
			closeErr = err
			break
		}
		n++
	}
	// Wait for all messages to be delivered XXX
	p.producer.Flush(5 * 1000)
	fmt.Printf("%d messages produced to topic %q\n", n, p.topic)
	p.producer.Close()
	return closeErr
}

func (p *Producer) write(rec *zng.Record) error {
	key, err := rec.Access("key")
	if err != nil {
		key = zng.Value{Type: zng.TypeNull}
	}
	keySchemaID, err := p.lookupSchema(key.Type)
	if err != nil {
		return err
	}
	val, err := rec.Access("value")
	if err != nil {
		val = rec.Value
	}
	valSchemaID, err := p.lookupSchema(val.Type)
	if err != nil {
		return err
	}
	keyBytes, err := zavro.Encode(nil, uint32(keySchemaID), key)
	if err != nil {
		return err
	}
	valBytes, err := zavro.Encode(nil, uint32(valSchemaID), val)
	if err != nil {
		return err
	}
	p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &p.topic,
			Partition: kafka.PartitionAny,
		},
		Key:   keyBytes,
		Value: valBytes},
		nil)
	return err
}

func (p *Producer) lookupSchema(typ zng.Type) (int, error) {
	id, ok := p.mapper[typ]
	if !ok {
		//XXX this shouldn't be a record in general
		s, err := zavro.EncodeSchema(zng.TypeRecordOf(typ), p.namespace)
		if err != nil {
			return 0, err
		}
		record, ok := s.(*avro.RecordSchema)
		if !ok {
			return 0, errors.New("internal error: avro schema not of type record")
		}
		schema, err := json.Marshal(record)
		if err != nil {
			return 0, err
		}
		id, err = p.CreateSchema(string(schema))
		if err != nil {
			return 0, err
		}
		p.mapper[typ] = id
	}
	return id, nil
}

func (p *Producer) CreateSchema(schema string) (int, error) {
	// We use RecordNameStrategy for the subject name so we can have
	// different schemas on the same topic.
	subject := schema
	s, err := p.registry.CreateSchema(subject, schema, srclient.Avro)
	if err != nil {
		return -1, err
	}
	return s.ID(), nil
}
