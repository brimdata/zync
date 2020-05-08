package listen

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/brimsec/zinger/cmd/zinger/root"
	"github.com/brimsec/zinger/pkg/registry"
	"github.com/brimsec/zinger/pkg/zinger"
	"github.com/brimsec/zq/emitter"
	"github.com/brimsec/zq/zbuf"
	"github.com/brimsec/zq/zio"
	"github.com/brimsec/zq/zng/resolver"
	"github.com/mccanne/charm"
)

var Listen = &charm.Spec{
	Name:  "listen",
	Usage: "listen [options]",
	Short: "listen as a daemon and transcode streaming posts onto kafka",
	Long: `
The listen command launches a process to listen on the provided interface and
port for HTTP POST requests of streaming data.  For each incoming connection,
the stream is forward to the configured output(s). Current outputs are local
file writer and Kafka, where the stream is transcoded into Avro and published
on the specified Kafka topic.

When the Kafka output is enabled:

Use -b to specify the Kafka bootstrap servers as a comma-separated list
of addresses (with form address:port).  Likewise, use -r to specify
one or more registry servers.

Use -t to specify the Kafka topic. Currently, all data is published to
a single topic.

Use -n to specify a namespace and -s to specify a subject both used when
automatically creating new schemas for transcoded Zeek/ZNG data.

`,
	New: New,
}

func init() {
	root.Zinger.Add(Listen)
}

type Command struct {
	*root.Command

	listenAddr string

	doFile bool
	fOpts  struct {
		dir  string
		ofmt string
	}

	doKafka bool
	kOpts   struct {
		kafkaCluster    string
		registryCluster string
		topic           string
		namespace       string
		subject         string
	}
}

func New(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{Command: parent.(*root.Command)}
	f.StringVar(&c.listenAddr, "l", ":9890", "[addr]:port to listen on")

	f.BoolVar(&c.doFile, "f", false, "enable file output")
	f.StringVar(&c.fOpts.dir, "d", ".", "file: path to write logs to")
	f.StringVar(&c.fOpts.ofmt, "o", "zeek", "file: format to write logs in")

	f.BoolVar(&c.doKafka, "k", false, "enable kafka output")
	f.StringVar(&c.kOpts.kafkaCluster, "b", "localhost:9092", "kafka: [addr]:port list of one or more brokers")
	f.StringVar(&c.kOpts.registryCluster, "r", "localhost:8081", "kafka: [addr]:port list of one more registry servers")
	//XXX change these defaults?
	f.StringVar(&c.kOpts.topic, "t", "kavro-test", "kafka: subject name for kafka schema registry")
	f.StringVar(&c.kOpts.namespace, "n", "com.example", "kafka: namespace to use when creating new schemas")
	f.StringVar(&c.kOpts.subject, "s", "kavrotest-value", "kafka: subject name for kafka schema registry")
	return c, nil
}

func servers(s string) []string {
	return strings.Split(s, ",")
}

func (c *Command) Run(args []string) error {
	zctx := resolver.NewContext()
	var outputs []zbuf.Writer

	if c.doFile {
		flags := &zio.WriterFlags{
			Format: c.fOpts.ofmt,
		}
		emitter, err := emitter.NewDir(c.fOpts.dir, "", os.Stderr, flags)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		outputs = append(outputs, emitter)
	}
	if c.doKafka {
		reg := registry.NewConnection(c.kOpts.subject, servers(c.kOpts.registryCluster))
		producer, err := zinger.NewProducer(servers(c.kOpts.kafkaCluster), reg, c.kOpts.topic, c.kOpts.namespace)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		outputs = append(outputs, producer)
	}
	if len(outputs) == 0 {
		fmt.Fprintln(os.Stderr, "Error: Must specify at least one output")
		os.Exit(1)
	}

	return zinger.Run(c.listenAddr, outputs, zctx)
}
