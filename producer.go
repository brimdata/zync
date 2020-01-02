package zinger

import (
	"time"

	"github.com/Shopify/sarama"
	kafka "github.com/dangkaka/go-kafka-avro"
	"github.com/go-avro/avro"
	"github.com/mccanne/zinger/zavro"
	"github.com/mccanne/zq/pkg/zng"
	"github.com/mccanne/zq/pkg/zng/resolver"
)

//XXX TBD
type Producer struct {
	//XXX don't use SyncProducer
	Producer sarama.SyncProducer
	Registry *Registry
	Resolver *resolver.Table
	schemas  map[uint32]avro.Schema
	mapper   map[int]uint32
}

func NewProducer(kServers, rServes []string) (*Producer, error) {
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
	p, err := sarama.NewSyncProducer(kServers, config)
	if err != nil {
		return nil, err
	}
	return &Producer{
		Producer: p,
		Resolver: resolver.NewTable(),
		schemas:  make(map[uint32]avro.Schema),
		mapper:   make(map[int]uint32),
	}, nil
}

//XXX could have config to map records onto different topics based on path
func (p *Producer) Write(rec *zng.Record) error {
	id := rec.Descriptor.ID // XXX need to map
	kid, ok := p.mapper[id]
	if !ok {
		//XXX need to talk to registry, nop for now
		kid = uint32(id)
		s := zavro.GenSchema(rec.Descriptor.Type)
		p.mapper[id] = kid
		p.schemas[kid] = s
	}
	//XXX
	kid = 1
	b, err := zavro.Encode(nil, kid, rec)
	if err != nil {
		return err
	}
	//XXX get rid of this layer
	value := &kafka.AvroEncoder{
		SchemaID: int(kid),
		Content:  b[5:],
	}
	topic := "kavro-test" //XXX
	key := ""             //XXX
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: value,
	}
	//pretty.Println(msg)
	_, _, err = p.Producer.SendMessage(msg)
	return err
}
