package fromkafka

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/brimdata/zed"
	"github.com/brimdata/zed/pkg/charm"
	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zync/cli"
	"github.com/brimdata/zync/cmd/zync/root"
	"github.com/brimdata/zync/etl"
	"github.com/brimdata/zync/fifo"
	"github.com/riferrei/srclient"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

func init() {
	root.Zync.Add(FromSpec)
}

var FromSpec = &charm.Spec{
	Name:  "from-kafka",
	Usage: "from-kafka [options] [config.yaml ...]",
	Short: "sync Kafka topics to Zed lake pools",
	Long: `
The "from-kafka" command syncs data from Kafka topics to Zed lake pools.
Topics and their target pools are read from the inputs section of the
config.yaml files.  (One topic/pool pair may also be specified via -topic
and -pool.)  The Kafka records are transcoded into Zed and synced to the
main branch of the target pools.

The key for each pool must be "kafka.offset" in ascending order.

See https://github.com/brimdata/zync/README.md for a description
of how this works.
`,
	New: NewFrom,
}

type From struct {
	*root.Command

	flags       cli.Flags
	lakeFlags   cli.LakeFlags
	shaperFlags cli.ShaperFlags

	exitAfter     time.Duration
	pool          string
	thresh        int
	topicMaxBytes int
	interval      time.Duration
}

func NewFrom(parent charm.Command, fs *flag.FlagSet) (charm.Command, error) {
	f := &From{Command: parent.(*root.Command)}
	f.flags.SetFlags(fs)
	f.lakeFlags.SetFlags(fs)
	f.shaperFlags.SetFlags(fs)
	fs.DurationVar(&f.exitAfter, "exitafter", 0, "if >0, exit after this duration")
	fs.StringVar(&f.pool, "pool", "", "name of Zed pool")
	fs.IntVar(&f.thresh, "thresh", 1024*1024, "maximum number of records per commit")
	fs.IntVar(&f.topicMaxBytes, "topicmaxbytes", 1024*1024, "maximum bytes buffered per topic")
	fs.DurationVar(&f.interval, "interval", 5*time.Second,
		"maximum interval between receiving and committing a record")
	return f, nil
}

func (f *From) Run(args []string) error {
	poolToTopics := map[string]map[string]struct{}{}
	if f.pool != "" || f.flags.Topic != "" {
		if f.pool == "" || f.flags.Topic == "" {
			return errors.New("both -pool and -topic must be set")
		}
		poolToTopics[f.pool] = map[string]struct{}{f.flags.Topic: {}}
	}
	for _, a := range args {
		transform, err := etl.Load(a)
		if err != nil {
			return fmt.Errorf("%s: %w", a, err)
		}
		for _, i := range transform.Inputs {
			topics, ok := poolToTopics[i.Pool]
			if !ok {
				topics = map[string]struct{}{}
				poolToTopics[i.Pool] = topics
			}
			topics[i.Topic] = struct{}{}
		}
	}
	if len(poolToTopics) == 0 {
		if len(args) > 0 {
			return errors.New("YAML config files contain no inputs")
		}
		return errors.New("provide YAML config files or set -pool and -topic")
	}

	shaper, err := f.shaperFlags.Load()
	if err != nil {
		return err
	}

	url, credentials, err := cli.SchemaRegistryEndpoint()
	if err != nil {
		return err
	}
	registry := srclient.CreateSchemaRegistryClient(url)
	registry.SetCredentials(credentials.User, credentials.Password)

	config, err := cli.LoadKafkaConfig()
	if err != nil {
		return err
	}
	config = append(config, kgo.FetchMaxPartitionBytes(int32(f.topicMaxBytes)))

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	lake, err := f.lakeFlags.Open(ctx)
	if err != nil {
		return err
	}
	group, ctx := errgroup.WithContext(ctx)
	timeoutCtx := ctx
	if f.exitAfter > 0 {
		timeoutCtx, cancel = context.WithTimeout(ctx, f.exitAfter)
		defer cancel()
	}
	for pool, topics := range poolToTopics {
		pool, topics := pool, topics
		group.Go(func() error {
			fifoLake, err := fifo.NewLake(ctx, pool, "", lake)
			if err != nil {
				return err
			}
			ch := make(chan *zed.Value)
			zctx := zed.NewContext()
			for t := range topics {
				t := t
				cOffset, err := fifoLake.NextConsumerOffset(ctx, t)
				if err != nil {
					return err
				}
				consumer, err := fifo.NewConsumer(zctx, config, registry, f.flags.Format, t, cOffset, true)
				if err != nil {
					return err
				}
				group.Go(func() error {
					return f.runRead(timeoutCtx, consumer, ch)
				})
			}
			return f.runLoad(ctx, timeoutCtx, zctx, fifoLake, shaper, ch)
		})
	}
	return group.Wait()
}

func (f *From) runRead(ctx context.Context, c *fifo.Consumer, ch chan<- *zed.Value) error {
	for {
		val, err := c.ReadValue(ctx)
		if err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				// Return nil so we don't cancel the context
				// from errgroup.WithContext.
				return nil
			}
			return err
		}
		select {
		case ch <- val:
		case <-ctx.Done():
		}
	}
}

func (f *From) runLoad(ctx, timeoutCtx context.Context, zctx *zed.Context, fifoLake *fifo.Lake, shaper string,
	ch <-chan *zed.Value) error {
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()
	// Stop ticker until data arrives.
	ticker.Stop()
	a := &zbuf.Array{}
	for {
		select {
		case val := <-ch:
			a.Append(val)
			if n := len(a.Values()); n < f.thresh {
				if n == 1 {
					// Start ticker.
					ticker.Reset(f.interval)
				}
				continue
			}
		case <-ticker.C:
			if len(a.Values()) == 0 {
				continue
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-timeoutCtx.Done():
			if len(a.Values()) == 0 {
				return nil
			}
		}
		// Stop ticker until more data arrives.
		ticker.Stop()
		if shaper != "" {
			var err error
			a, err = fifo.RunLocalQuery(ctx, zctx, a, shaper)
			if err != nil {
				return err
			}
		}
		n := len(a.Values())
		if n == 0 {
			// Shaper dropped everything.
			continue
		}
		commit, err := fifoLake.LoadBatch(ctx, zctx, a)
		if err != nil {
			return err
		}
		fmt.Printf("commit %s %d record%s\n", commit, n, plural(n))
	}
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
