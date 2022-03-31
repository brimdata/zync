package produce

import (
	"context"
	"errors"
	"flag"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/cli/inputflags"
	"github.com/brimdata/zed/pkg/charm"
	"github.com/brimdata/zed/pkg/storage"
	"github.com/brimdata/zed/zio"
	"github.com/brimdata/zync/cli"
	"github.com/brimdata/zync/cmd/zync/root"
	"github.com/brimdata/zync/fifo"
	"github.com/riferrei/srclient"
	"github.com/twmb/franz-go/pkg/kgo"
)

var Produce = &charm.Spec{
	Name:  "produce",
	Usage: "produce [options] [file1, file2, ...]",
	Short: "produce Zed data into a Kafka topic",
	Long: `
The produce command copies the input Zed data into a Kafka topic.
No effort is made to provide synchronization as data as simply copied from
input to the topic and any failures are not recovered from.
Use the "zync sync" command to provide synchronization and
fail-safe, restartable operation.`,
	New: New,
}

func init() {
	root.Zync.Add(Produce)
}

type Command struct {
	*root.Command
	flags      cli.Flags
	inputFlags inputflags.Flags
}

func New(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{Command: parent.(*root.Command)}
	c.flags.SetFlags(f)
	c.inputFlags.SetFlags(f, false)
	return c, nil
}

func (c *Command) Run(args []string) error {
	ctx := context.Background()
	if len(args) == 0 {
		return errors.New("no inputs provided")
	}
	if c.flags.Topic == "" {
		return errors.New("no topic provided")
	}
	if err := c.inputFlags.Init(); err != nil {
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
	config = append(config, kgo.AllowAutoTopicCreation())
	registry := srclient.CreateSchemaRegistryClient(url)
	registry.SetCredentials(secret.User, secret.Password)
	readers, err := c.inputFlags.Open(ctx, zed.NewContext(), storage.NewLocalEngine(), args, true)
	if err != nil {
		return err
	}
	defer zio.CloseReaders(readers)
	producer, err := fifo.NewProducer(config, registry, c.flags.Topic, c.flags.Namespace)
	if err != nil {
		return err
	}
	return producer.Run(ctx, zio.ConcatReader(readers...))
}
