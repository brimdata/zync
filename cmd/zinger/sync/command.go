package sync

import (
	"errors"
	"flag"

	"github.com/brimdata/zed/pkg/charm"
	"github.com/brimdata/zinger/cmd/zinger/root"
)

var Sync = &charm.Spec{
	Name:  "sync",
	Usage: "sync [options]",
	Short: "sync from a Zed lake pool to a Kafka topic",
	Long: `
The sync command syncs data from a Zed lake to a Kafka topic acting
as a source of Zed data for Kafka.
The Zed records are transcoded into Avro and published
on the specified Kafka topic.

XXX document technique and expected format of lake records.
`,
	New: New,
}

func init() {
	root.Zinger.Add(Sync)
}

type Command struct {
	*root.Command
}

func New(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{Command: parent.(*root.Command)}
	return c, nil
}

func (c *Command) Run(args []string) error {
	return errors.New("TBD")
}
