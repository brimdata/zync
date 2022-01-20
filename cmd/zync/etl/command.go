package etl

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os/signal"
	"strings"
	"syscall"

	"github.com/brimdata/zed/pkg/charm"
	"github.com/brimdata/zync/cli"
	"github.com/brimdata/zync/cmd/zync/root"
	"github.com/brimdata/zync/etl"
)

var Spec = &charm.Spec{
	Name:  "etl",
	Usage: "etl [options] config.yaml",
	Short: "transform data from a Zed lake pool to another pool",
	Long: `
The "etl" command reads data from input pools, transforms it, and
writes it to output pools according to config.yaml.

All data pool keys must be "kafka.offset" sorted in ascending order.

See https://github.com/brimdata/zync/README.md for a description
of how this works.
`,
	New: New,
}

func init() {
	root.Zync.Add(Spec)
}

type Command struct {
	*root.Command
	zed       bool
	flags     cli.Flags
	lakeFlags cli.LakeFlags
}

func New(parent charm.Command, fs *flag.FlagSet) (charm.Command, error) {
	c := &Command{Command: parent.(*root.Command)}
	fs.BoolVar(&c.zed, "zed", false, "dump compiled Zed to stdout and exit)")
	c.flags.SetFlags(fs)
	c.lakeFlags.SetFlags(fs)
	return c, nil
}

func (c *Command) Run(args []string) error {
	if len(args) == 0 {
		return errors.New("no YAML config file provided")
	}
	if len(args) > 1 {
		return errors.New("too many arguments")
	}
	config, err := etl.Load(args[0])
	if err != nil {
		return err
	}
	if c.zed {
		zeds, err := etl.Build(config)
		if err != nil {
			return err
		}
		fmt.Println(strings.Join(zeds, "\n===\n"))
		return nil
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()
	lake, err := c.lakeFlags.Open(ctx)
	if err != nil {
		return err
	}
	pipeline, err := etl.NewPipeline(ctx, config, lake)
	if err != nil {
		return err
	}
	n, err := pipeline.Run(ctx)
	if err != nil {
		return err
	}
	if n != 0 {
		fmt.Printf("ETL'd %d record%s\n", n, plural(n))
	} else {
		fmt.Println("nothing new found to ETL")
	}
	return nil
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
