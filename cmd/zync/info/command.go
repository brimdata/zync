package ls

import (
	"flag"
	"fmt"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/pkg/charm"
	"github.com/brimdata/zync/cli"
	"github.com/brimdata/zync/cmd/zync/root"
	"github.com/brimdata/zync/fifo"
	"github.com/riferrei/srclient"
)

var Info = &charm.Spec{
	Name:  "info",
	Usage: "info -topic topic",
	Short: "show info about a topic",
	Long: `
The info command displays information about a Kafka topic.
Currently it simply prints the low and high watermarks.
`,
	New: New,
}

func init() {
	root.Zync.Add(Info)
}

type Command struct {
	*root.Command
	flags cli.Flags
}

func New(parent charm.Command, fs *flag.FlagSet) (charm.Command, error) {
	c := &Command{Command: parent.(*root.Command)}
	c.flags.SetFlags(fs)
	return c, nil
}

func (c *Command) Run(args []string) error {
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
	consumer, err := fifo.NewConsumer(zctx, config, registry, c.flags.Topic, 0, false)
	if err != nil {
		return err
	}
	low, high, err := consumer.Watermarks()
	if err != nil {
		return err
	}
	fmt.Printf("low %d high %d\n", low, high)
	return nil
}
