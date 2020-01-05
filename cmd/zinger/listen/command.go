package listen

import (
	"flag"
	"fmt"
	"os"

	"github.com/mccanne/charm"
	"github.com/mccanne/zinger/cmd/zinger/root"
	"github.com/mccanne/zinger/pkg/registry"
	"github.com/mccanne/zinger/pkg/zinger"
)

var Listen = &charm.Spec{
	Name:  "listen",
	Usage: "listen [options]",
	Short: "listen as a daemon and transcode streaming posts onto kafka",
	Long: `
The listen command launches a process to listen on the provided interface and
port for HTTP POST requests of streaming data.  For each incoming connection,
the stream is transcoded into Avro and published on the specified Kafka topic.
`,
	New: New,
}

func init() {
	root.Zinger.Add(Listen)
}

type Command struct {
	*root.Command
	listenAddr string
}

func New(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{Command: parent.(*root.Command)}
	f.StringVar(&c.listenAddr, "l", ":9890", "[addr]:port to listen on")
	return c, nil
}

func (c *Command) Run(args []string) error {
	servers := c.KafkaCluster()
	reg := registry.NewConnection(c.Subject, c.RegistryCluster())
	producer, err := zinger.NewProducer(servers, reg, c.Topic, c.Namespace)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return zinger.Run(c.listenAddr, producer)
}
