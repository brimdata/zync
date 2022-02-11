package consume

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"time"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/cli/outputflags"
	"github.com/brimdata/zed/pkg/charm"
	"github.com/brimdata/zed/pkg/nano"
	"github.com/brimdata/zed/pkg/storage"
	"github.com/brimdata/zync/cli"
	"github.com/brimdata/zync/cmd/zync/root"
	"github.com/brimdata/zync/etl"
	"github.com/brimdata/zync/fifo"
	"github.com/riferrei/srclient"
)

var Consume = &charm.Spec{
	Name:  "consume",
	Usage: "consume [options]",
	Short: "consume data from a Kafka topic and format as Zed output",
	Long: `
The consume command reads Avro records from a Kafka topic.

Consume reads each record as Avro and transcodes it to Zed using the configured
schema registry.  Any of the output formats used by the "zed" command may be
specified in the same way as in the zed query commands (i.e., zq, zed query, etc).

Once consume reaches the head of the Kafka topic, it blocks and waits for more
data and gives up and exits if a timeout is provided.  Note that if the duration
is too short, consume may exit before any available records are ready
asynchronously from the topic.
`,
	New: New,
}

func init() {
	root.Zync.Add(Consume)
}

type Command struct {
	*root.Command
	flags       cli.Flags
	timeout     string
	offset      int64
	outputFlags outputflags.Flags
}

func New(parent charm.Command, fs *flag.FlagSet) (charm.Command, error) {
	c := &Command{Command: parent.(*root.Command)}
	fs.StringVar(&c.timeout, "timeout", "", "timeout in ZSON duration syntax (5s, 1m30s, ...)")
	fs.Int64Var(&c.offset, "offset", etl.KafkaOffsetEarliest, "initial Kafka offset (-2 is earliest, -1 is latest)")
	c.flags.SetFlags(fs)
	c.outputFlags.SetFlags(fs)
	return c, nil
}

func (c *Command) Run(args []string) error {
	ctx := context.Background()
	if len(args) != 0 {
		return errors.New("extra arguments not allowed")
	}
	if c.flags.Topic == "" {
		return errors.New("no topic provided")
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
	writer, err := c.outputFlags.Open(ctx, storage.NewLocalEngine())
	if err != nil {
		return err
	}
	zctx := zed.NewContext()
	consumer, err := fifo.NewConsumer(zctx, config, registry, c.flags.Format, c.flags.Topic, c.offset, false)
	if err != nil {
		return err
	}
	timeout := nano.Duration(math.MaxInt64)
	if c.timeout != "" {
		timeout, err = nano.ParseDuration(c.timeout)
		if err != nil {
			return fmt.Errorf("parse error with -timeout option: %w", err)
		}
	}
	// Note that we do not close the Kafka consumer here as that involves
	// an extra timeout and since we are notting committing consumer offsets,
	// there is no need to shutdown in this fashion.
	err = consumer.Run(ctx, writer, time.Duration(timeout))
	if closeErr := writer.Close(); err == nil {
		err = closeErr
	}
	return err
}
