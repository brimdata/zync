package tokafka

import (
	"context"
	"errors"
	"flag"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/pkg/charm"
	"github.com/brimdata/zync/cli"
	"github.com/brimdata/zync/cmd/zync/root"
	"github.com/brimdata/zync/fifo"
	"github.com/riferrei/srclient"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func init() {
	root.Zync.Add(ToSpec)
}

var ToSpec = &charm.Spec{
	Name:  "to-kafka",
	Usage: "to-kafka [options]",
	Short: "sync a Zed lake pool to a Kafka topic",
	Long: `
The "to-kafka" command syncs data from a Zed lake to a Kafka topic acting
as a source of Zed data for Kafka.
The Zed records are transcoded from Zed to Avro and synced
to the target Kafka topic.
The data pool must have the pool key "kafka.offset" sorted
in ascending order.

Only a single writer is allowed at any given time to the Kafka topic.
At start up, the to command queries the topic for its high-water mark
and continues replicating data from the source data pool at that offset
according to the kafka.offset value in the data pool.
`,
	New: NewTo,
}

type To struct {
	*root.Command
	flags       cli.Flags
	lakeFlags   cli.LakeFlags
	shaper      cli.ShaperFlags
	partitions  int
	pool        string
	replication int
}

func NewTo(parent charm.Command, fs *flag.FlagSet) (charm.Command, error) {
	f := &To{Command: parent.(*root.Command)}
	fs.IntVar(&f.partitions, "partitions", 0, "if nonzero, create new Kafka topic with this many partitions")
	fs.StringVar(&f.pool, "pool", "", "name of Zed data pool")
	fs.IntVar(&f.replication, "replication", 1, "replication factor for new Kafka topic")
	f.flags.SetFlags(fs)
	f.lakeFlags.SetFlags(fs)
	f.shaper.SetFlags(fs)
	return f, nil
}

func (t *To) Run(args []string) error {
	if t.flags.Topic == "" {
		return errors.New("no topic provided")
	}
	if t.pool == "" {
		return errors.New("no pool provided")

	}
	shaper, err := t.shaper.Load()
	if err != nil {
		return err
	}
	ctx := context.Background()
	service, err := t.lakeFlags.Open(ctx)
	if err != nil {
		return err
	}
	lk, err := fifo.NewLake(ctx, t.pool, shaper, service)
	if err != nil {
		return err
	}
	url, secret, err := cli.SchemaRegistryEndpoint()
	if err != nil {
		return err
	}
	config, err := cli.LoadKafkaConfig()
	if err != nil {
		return err
	}
	if t.partitions > 0 {
		adminClient, err := kafka.NewAdminClient(config)
		if err != nil {
			return err
		}
		result, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{
			{
				Topic:             t.flags.Topic,
				NumPartitions:     t.partitions,
				ReplicationFactor: t.replication,
			},
		})
		if err != nil {
			return err
		}
		if err := result[0].Error; err.Code() != kafka.ErrNoError && err.Code() != kafka.ErrTopicAlreadyExists {
			return err
		}
	}
	registry := srclient.CreateSchemaRegistryClient(url)
	registry.SetCredentials(secret.User, secret.Password)
	zctx := zed.NewContext()
	producer, err := fifo.NewProducer(config, registry, t.flags.Topic, t.flags.Namespace)
	if err != nil {
		return err
	}
	to := fifo.NewTo(zctx, producer, lk)
	return to.Sync(ctx)
}
