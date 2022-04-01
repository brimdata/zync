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
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	kclient   *kgo.Client
	registry  *srclient.SchemaRegistryClient
	topic     string
	namespace string
	mapper    map[zed.Type]int
}

func NewProducer(opts []kgo.Opt, reg *srclient.SchemaRegistryClient, topic, namespace string) (*Producer, error) {
	kclient, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Producer{
		kclient:   kclient,
		registry:  reg,
		topic:     topic,
		namespace: namespace,
		mapper:    make(map[zed.Type]int),
	}, nil
}

func (p *Producer) HeadOffset(ctx context.Context) (int64, error) {
	return maxOffset(kadm.NewClient(p.kclient).ListEndOffsets(ctx, p.topic))
}

func (p *Producer) Run(ctx context.Context, reader zio.Reader) error {
	fmt.Printf("producing messages to topic %q...\n", p.topic)
	var err error
	var n int
	for {
		var rec *zed.Value
		rec, err = reader.Read()
		if rec == nil || err != nil {
			break
		}
		err = p.write(ctx, rec)
		if err != nil {
			break
		}
		n++
	}
	fmt.Println("waiting for Kafka flush...")
	if err2 := p.kclient.Flush(ctx); err == nil {
		err = err2
	}
	fmt.Printf("%d messages produced to topic %q\n", n, p.topic)
	p.kclient.Close()
	return err
}

func (p *Producer) Send(ctx context.Context, batch zbuf.Batch) error {
	values := batch.Values()
	for k := range values {
		if err := p.write(ctx, &values[k]); err != nil {
			return err
		}
	}
	return p.kclient.Flush(ctx)
}

func (p *Producer) write(ctx context.Context, rec *zed.Value) error {
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
	return p.kclient.ProduceSync(ctx, &kgo.Record{
		Key:   keyBytes,
		Value: valBytes,
		Topic: p.topic,
	}).FirstErr()
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
