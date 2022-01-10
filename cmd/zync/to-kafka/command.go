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
	flags     cli.Flags
	lakeFlags cli.LakeFlags
	shaper    cli.ShaperFlags
	pool      string
}

func NewTo(parent charm.Command, fs *flag.FlagSet) (charm.Command, error) {
	f := &To{Command: parent.(*root.Command)}
	fs.StringVar(&f.pool, "pool", "", "name of Zed data pool")
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
