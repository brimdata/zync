package root

import (
	"flag"
	"log"
	"strings"

	"github.com/mccanne/charm"
)

// These variables are populated via the Go linker.
var (
	Version   = "unknown"
	ZqVersion = "unknown"
)

var Zinger = &charm.Spec{
	Name:  "zinger",
	Usage: "zinger [global options] command [options] [arguments...]",
	Short: "use zinger to interconnect zeek and kafka",
	Long: `
Zinger inteconnects zeek and Kafka/Acvro using the Kafka Schema Registery.

Use -k to specify the Kafka bootstrap servers as a comma-separated list
of addresses (with form address:port).  Likewise, use -r to specify
one or more registry servers.

Use -t to specify the Kafka topic. Currently, all data is published to
a single topic.

Use -n to specify a namespace and -s to specify a subject both used when
utomatically creating new schemas for transcoded Zeek/ZNG data.
`,
	New: New,
}

type Command struct {
	charm.Command
	kafkaCluster    string
	registryCluster string
	Topic           string
	Namespace       string
	Subject         string
}

func init() {
	Zinger.Add(charm.Help)
}

func Servers(s string) []string {
	return strings.Split(s, ",")
}

func New(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{}
	f.StringVar(&c.kafkaCluster, "k", "localhost:9092", "[addr]:port list of one or more kafka servers")
	f.StringVar(&c.registryCluster, "r", "localhost:8081", "[addr]:port list of one more kafka registry servers")
	//XXX change these defaults?
	f.StringVar(&c.Topic, "t", "kavro-test", "subject name for kafka schema registry")
	f.StringVar(&c.Namespace, "n", "com.example", "namespace to use when creating new schemas")
	f.StringVar(&c.Subject, "s", "kavrotest-value", "subject name for kafka schema registry")
	log.SetPrefix("zinger")
	return c, nil
}

func (c *Command) KafkaCluster() []string {
	return Servers(c.kafkaCluster)
}

func (c *Command) RegistryCluster() []string {
	return Servers(c.registryCluster)
}

func (c *Command) Run(args []string) error {
	return Zinger.Exec(c, []string{"help"})
}
