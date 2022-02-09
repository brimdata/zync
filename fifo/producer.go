package fifo

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zio"
	"github.com/brimdata/zed/zson"
	"github.com/brimdata/zync/zavro"
	"github.com/riferrei/srclient"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Producer struct {
	producer  *kafka.Producer
	registry  *srclient.SchemaRegistryClient
	topic     string
	namespace string
	mapper    map[zed.Type]int
}

func NewProducer(config *kafka.ConfigMap, reg *srclient.SchemaRegistryClient, topic, namespace string) (*Producer, error) {
	if err := config.SetKey("enable.idempotence", true); err != nil {
		return nil, err
	}
	p, err := kafka.NewProducer(config)
	if err != nil {
		return nil, err
	}
	return &Producer{
		producer:  p,
		registry:  reg,
		topic:     topic,
		namespace: namespace,
		mapper:    make(map[zed.Type]int),
	}, nil
}

func (p *Producer) HeadOffset() (kafka.Offset, error) {
	_, high, err := p.producer.QueryWatermarkOffsets(p.topic, 0, 1000*10)
	return kafka.Offset(high), err
}

func (p *Producer) Run(ctx context.Context, reader zio.Reader) error {
	fmt.Printf("producing messages to topic %q...\n", p.topic)
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan error)
	go func() {
		defer close(done)
		for {
			select {
			case ev := <-p.producer.Events():
				msg, ok := ev.(*kafka.Message)
				if !ok {
					continue
				}
				if msg.TopicPartition.Error != nil {
					done <- msg.TopicPartition.Error
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	var n int
	var closeErr error
loop:
	for {
		select {
		case err := <-done:
			closeErr = err
			break loop
		default:
			rec, err := reader.Read()
			if rec == nil || err != nil {
				closeErr = err
				break loop
			}
			if err := p.write(rec); err != nil {
				closeErr = err
				break loop
			}
			n++
		}
	}
	fmt.Println("waiting for Kafka flush...")
	for {
		nleft := p.producer.Flush(1000)
		if nleft == 0 {
			break
		}
		fmt.Printf("waiting for %d Kafka events...\n", nleft)
	}
	cancel()
	fmt.Printf("%d messages produced to topic %q\n", n, p.topic)
	p.producer.Close()
	return closeErr
}

func (p *Producer) Send(ctx context.Context, offset kafka.Offset, batch zbuf.Batch) error {
	vals := batch.Values()
	batchLen := len(vals)
	done := make(chan error)
	ctx, cancel := context.WithCancel(ctx)
	go func(start, end kafka.Offset) {
		defer close(done)
		off := start
		for {
			select {
			case ev := <-p.producer.Events():
				msg, ok := ev.(*kafka.Message)
				if !ok {
					continue
				}
				if msg.TopicPartition.Error != nil {
					done <- msg.TopicPartition.Error
					return
				}
				if msg.TopicPartition.Offset != off {
					done <- fmt.Errorf("out of sync: expected %d, got %d (in batch %d,%d)", off, msg.TopicPartition.Offset, start, end)
					return
				}
				off++
				if off >= end {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}(offset, offset+kafka.Offset(batchLen))
	for k := range vals {
		if err := p.write(&vals[k]); err != nil {
			cancel()
			return err
		}
	}
	// Wait for all messages to be delivered.
	for p.producer.Flush(1000) != 0 {
	}
	cancel()
	return <-done
}

func (p *Producer) write(rec *zed.Value) error {
	key := rec.Deref("key")
	if key == nil {
		key = zed.Null
	}
	keySchemaID, err := p.lookupSchema(key.Type)
	if err != nil {
		return err
	}
	val := rec.Deref("value")
	if val == nil {
		val = rec
	}
	valSchemaID, err := p.lookupSchema(val.Type)
	if err != nil {
		return err
	}
	keyBytes, err := zavro.Encode(nil, uint32(keySchemaID), *key)
	if err != nil {
		return err
	}
	valBytes, err := zavro.Encode(nil, uint32(valSchemaID), *val)
	if err != nil {
		return err
	}
	p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &p.topic,
			Partition: 0,
		},
		Key:   keyBytes,
		Value: valBytes},
		nil)
	return err
}

func (p *Producer) lookupSchema(typ zed.Type) (int, error) {
	id, ok := p.mapper[typ]
	if !ok {
		s, err := zavro.EncodeSchema(typ, p.namespace)
		if err != nil {
			return 0, err
		}
		schema, err := json.Marshal(s)
		if err != nil {
			return 0, err
		}
		// We use RecordNameStrategy for the subject name so we can have
		// different schemas on the same topic.
		subject := fmt.Sprintf("zng_%x", md5.Sum([]byte(zson.FormatType(typ))))
		if p.namespace != "" {
			subject = p.namespace + "." + subject
		}
		id, err = p.CreateSchema(subject, string(schema))
		if err != nil {
			return 0, err
		}
		p.mapper[typ] = id
	}
	return id, nil
}

func (p *Producer) CreateSchema(subject, schema string) (int, error) {
	s, err := p.registry.CreateSchema(subject, schema, srclient.Avro)
	if err != nil {
		return -1, err
	}
	return s.ID(), nil
}
