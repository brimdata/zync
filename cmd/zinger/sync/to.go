package sync

import (
	"context"
	"errors"
	"flag"

	lakeapi "github.com/brimdata/zed/lake/api"
	"github.com/brimdata/zed/pkg/charm"
	"github.com/brimdata/zed/zson"
	"github.com/brimdata/zinger/cli"
	"github.com/brimdata/zinger/fifo"
	"github.com/riferrei/srclient"
)

var ToSpec = &charm.Spec{
	Name:  "to",
	Usage: "to [options]",
	Short: "sync a Zed lake pool to a Kafka topic",
	Long: `
The "to" command syncs data from a Zed lake to a Kafka topic acting
as a source of Zed data for Kafka.
The Zed records are transcoded from Zed to Avro and synced
to the target Kafka topic.
The data pool is expected to have the pool key "kafka.offset" sorted
in descending order.

Only a single writer is allowed at any given time to the Kafka topic.
At start up, the to command queries the topic for its high-water mark
and continues replicating data from the source data pool at that offset
according to the kafka.offset value in the data pool.
`,
	New: NewTo,
}

type To struct {
	*Sync
	flags cli.Flags
}

func NewTo(parent charm.Command, fs *flag.FlagSet) (charm.Command, error) {
	f := &To{Sync: parent.(*Sync)}
	f.flags.SetFlags(fs)
	return f, nil
}

func (t *To) Run(args []string) error {
	if t.flags.Topic == "" {
		return errors.New("no topic provided")
	}
	if t.pool == "" {
		return errors.New("no pool provided")

	}
	shaper, err := t.loadShaper()
	if err != nil {
		return err
	}
	ctx := context.Background()
	service, err := lakeapi.OpenRemoteLake(ctx, t.flags.ZedLakeHost)
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
	zctx := zson.NewContext()
	producer, err := fifo.NewProducer(config, registry, t.flags.Topic, t.flags.Namespace)
	if err != nil {
		return err
	}
	to := fifo.NewTo(zctx, producer, lk)
	return to.Sync(ctx)
}
