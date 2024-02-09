package fifo

import (
	"context"
	"fmt"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zio"
	"github.com/brimdata/zync/connectjson"
	"github.com/brimdata/zync/zavro"
	"github.com/riferrei/srclient"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	encode  func(zed.Value) ([]byte, error)
	kclient *kgo.Client
	topic   string
}

func NewProducer(opts []kgo.Opt, reg *srclient.SchemaRegistryClient, format, topic, namespace string) (*Producer, error) {
	var encode func(zed.Value) ([]byte, error)
	switch format {
	case "avro":
		encode = zavro.NewEncoder(namespace, reg).Encode
	case "json":
		encode = connectjson.Encode
	default:
		return nil, fmt.Errorf("unknonwn format %q", format)
	}
	kclient, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Producer{
		encode:  encode,
		kclient: kclient,
		topic:   topic,
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
		err = p.write(ctx, *rec)
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
	for _, rec := range batch.Values() {
		if err := p.write(ctx, rec); err != nil {
			return err
		}
	}
	return p.kclient.Flush(ctx)
}

func (p *Producer) write(ctx context.Context, rec zed.Value) error {
	key := rec.Deref("key").MissingAsNull()
	val := rec.Deref("value")
	if val == nil {
		val = &rec
	}
	keyBytes, err := p.encode(key)
	if err != nil {
		return err
	}
	valBytes, err := p.encode(*val)
	if err != nil {
		return err
	}
	return p.kclient.ProduceSync(ctx, &kgo.Record{
		Key:   keyBytes,
		Value: valBytes,
		Topic: p.topic,
	}).FirstErr()
}
