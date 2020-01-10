package zinger

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mccanne/zinger/pkg/registry"
	"github.com/mccanne/zinger/pkg/zavro"
	"github.com/mccanne/zq/pkg/zng"
	"github.com/mccanne/zq/pkg/zng/resolver"
)

type Route struct {
	schemaID int
	*zavro.Route
}

type Translator struct {
	// For now we use SyncProducer to provide simplicity and an easy-to-understand
	// example.  Before putting into production or any perf testing, we need to
	// change this to async and probably refactor the data path to have a
	// seprate Producer thread for each http connection (and a shared schema
	// manager).  For now, any parallel connections share a single producer.
	// XXX note: change this to sarama.AsyncProducer
	Producer sarama.SyncProducer
	Resolver *resolver.Table
	// For now there is a single topic written to.  We can add support
	// later to route different records to different topics based on rules.
	topic     string
	namespace string
	registry  *registry.Connection
	// mapper translates zng descriptor IDs to a configured kafka/avro schema
	sync.Mutex
	mapper map[int]Route
	router map[string]*zavro.Route
}

func NewTranslator(servers []string, reg *registry.Connection, topic, namespace, schemaDir, configFile string) (*Translator, error) {
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
	router, err := zavro.LoadConfig(schemaDir, configFile)
	if err != nil {
		return nil, err
	}
	return &Translator{
		Producer:  p,
		Resolver:  resolver.NewTable(),
		registry:  reg,
		topic:     topic,
		namespace: namespace,
		mapper:    make(map[int]Route),
		router:    router,
	}, nil
}

func (p *Translator) Write(rec *zng.Record) error {
	id := rec.Descriptor.ID
	// For now wrap a lock around the entire creation of a new schema.
	// This would be more efficient with a condition variable where the
	// first arriver gets to create the schema.
	p.Lock()
	route, ok := p.mapper[id]
	if !ok {
		schema, err := json.Marshal(route.Schema)
		if err != nil {
			p.Unlock()
			return err
		}
		schemaID, err := p.registry.Create(schema)
		if err != nil {
			p.Unlock()
			return err
		}
		p.mapper[id] = Route{
			schemaID: schemaID,
			Route:    route.Route,
		}
		log.Println("schema lookup:", schemaID, string(schema))
	}
	p.Unlock()
	b, err := zavro.Translate(uint32(route.schemaID), route.Route, rec)
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
