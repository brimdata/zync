package ls

import (
	"flag"
	"fmt"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/pkg/charm"
	"github.com/brimdata/zinger/cli"
	"github.com/brimdata/zinger/cmd/zinger/root"
	"github.com/brimdata/zinger/fifo"
	"github.com/riferrei/srclient"
)

var Info = &charm.Spec{
	Name:  "info",
	Usage: "info -topic topic",
	Short: "show info about a topic",
	Long: `
The info command displays information about a Kafka topic.
Currently it simply prints the low and high water marks for the
indicated consumer group, or the abslute low and high water marks
if no group is given.
`,
	New: New,
}

func init() {
	root.Zinger.Add(Info)
}

type Command struct {
	*root.Command
	group string
	flags cli.Flags
}

func New(parent charm.Command, fs *flag.FlagSet) (charm.Command, error) {
	c := &Command{Command: parent.(*root.Command)}
	fs.StringVar(&c.group, "group", "", "Kafka consumer group name")
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
	consumer, err := fifo.NewConsumer(zctx, config, registry, c.flags.Topic, c.group, 0, false)
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
