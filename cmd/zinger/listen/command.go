package listen

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/mccanne/charm"
	"github.com/mccanne/zinger/cmd/zinger/root"
	"github.com/mccanne/zinger/pkg/registry"
	"github.com/mccanne/zinger/pkg/zinger"
	"github.com/mccanne/zq/zng/resolver"
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
	kafkaCluster    string
	registryCluster string
	Topic           string
	Namespace       string
	Subject         string
	listenAddr      string
}

func New(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{Command: parent.(*root.Command)}
	f.StringVar(&c.kafkaCluster, "k", "localhost:9092", "[addr]:port list of one or more kafka servers")
	f.StringVar(&c.registryCluster, "r", "localhost:8081", "[addr]:port list of one more kafka registry servers")
	//XXX change these defaults?
	f.StringVar(&c.Topic, "t", "kavro-test", "subject name for kafka schema registry")
	f.StringVar(&c.Namespace, "n", "com.example", "namespace to use when creating new schemas")
	f.StringVar(&c.Subject, "s", "kavrotest-value", "subject name for kafka schema registry")
	f.StringVar(&c.listenAddr, "l", ":9890", "[addr]:port to listen on")
	return c, nil
}

func servers(s string) []string {
	return strings.Split(s, ",")
}

func (c *Command) Run(args []string) error {
	reg := registry.NewConnection(c.Subject, servers(c.registryCluster))
	producer, err := zinger.NewProducer(servers(c.kafkaCluster), reg, c.Topic, c.Namespace)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	zctx := resolver.NewContext()
	return zinger.Run(c.listenAddr, producer, zctx)
}
