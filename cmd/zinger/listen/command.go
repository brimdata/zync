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
	"github.com/mccanne/zq/emitter"
	"github.com/mccanne/zq/zbuf"
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
		emitter, err := emitter.NewDir(c.fOpts.dir, "", c.fOpts.ofmt, os.Stderr, nil)
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
