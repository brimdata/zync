package produce

import (
	"errors"
	"flag"

	"github.com/brimdata/zed/cli/inputflags"
	"github.com/brimdata/zed/pkg/charm"
	"github.com/brimdata/zed/pkg/storage"
	"github.com/brimdata/zed/zio"
	"github.com/brimdata/zed/zson"
	"github.com/brimdata/zinger/cli"
	"github.com/brimdata/zinger/cmd/zinger/root"
	"github.com/brimdata/zinger/fifo"
	"github.com/riferrei/srclient"
)

var Produce = &charm.Spec{
	Name:  "produce",
	Usage: "produce [options] [file1, file2, ...]",
	Short: "produce Zed data into a Kafka topic",
	Long: `
The produce command copies the input Zed data into a Kafka topic.
No effort is made to provide synchronization as data as simply coped from
input to the topic and any failures are not recovered from.
Use the "zinger sync" command to provide synchronization and
fail-safe, restartable operation.`,
	New: New,
}

func init() {
	root.Zinger.Add(Produce)
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
	if len(args) == 0 {
		return errors.New("no inputs provided")
	}
	if c.flags.Topic == "" {
		return errors.New("no topic (-t) provided")
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
	registry := srclient.CreateSchemaRegistryClient(url)
	registry.SetCredentials(secret.User, secret.Password)
	zctx := zson.NewContext()
	local := storage.NewLocalEngine()
	readers, err := c.inputFlags.Open(zctx, local, args, true)
	if err != nil {
		return err
	}
	defer zio.CloseReaders(readers)
	reader := zio.ConcatReader(readers...)
	producer, err := fifo.NewProducer(config, registry, c.flags.Topic, c.flags.Namespace)
	if err != nil {
		return err
	}
	return producer.Run(reader)
}
