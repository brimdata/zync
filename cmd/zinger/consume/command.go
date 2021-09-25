package consume

import (
	"context"
	"errors"
	"flag"

	"github.com/brimdata/zed/cli/outputflags"
	"github.com/brimdata/zed/pkg/charm"
	"github.com/brimdata/zed/pkg/storage"
	"github.com/brimdata/zed/zson"
	"github.com/brimdata/zinger/cli"
	"github.com/brimdata/zinger/cmd/zinger/root"
	"github.com/brimdata/zinger/fifo"
	"github.com/riferrei/srclient"
)

var Consume = &charm.Spec{
	Name:  "consume",
	Usage: "consume [options]",
	Short: "consume data from a Kafka topic and format as Zed output",
	Long: `
TBD`,
	New: New,
}

func init() {
	root.Zinger.Add(Consume)
}

type Command struct {
	*root.Command
	flags       cli.Flags
	outputFlags outputflags.Flags
}

func New(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{Command: parent.(*root.Command)}
	c.flags.SetFlags(f)
	c.outputFlags.SetFlags(f)
	return c, nil
}

func (c *Command) Run(args []string) error {
	if len(args) != 0 {
		return errors.New("extra arguments not allowed")
	}
	if c.flags.Topic == "" {
		return errors.New("no topic (-t) provided")
	}
	if err := c.outputFlags.Init(); err != nil {
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
	writer, err := c.outputFlags.Open(context.TODO(), storage.NewLocalEngine())
	if err != nil {
		return err
	}
	zctx := zson.NewContext()
	consumer, err := fifo.NewConsumer(zctx, config, registry, c.flags.Topic, 0)
	if err != nil {
		return err
	}
	err = consumer.Run(writer)
	if closeErr := writer.Close(); err == nil {
		err = closeErr
	}
	// XXX skip close for now since we are not committing any new offsets
	// and there is a long timeout in the close path...
	//if err == nil {
	//	consumer.Close()
	//}
	return err
}
