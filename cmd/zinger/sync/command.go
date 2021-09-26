package sync

import (
	"errors"
	"flag"

	"github.com/brimdata/zed/pkg/charm"
	"github.com/brimdata/zinger/cmd/zinger/root"
)

var SyncSpec = &charm.Spec{
	Name:  "sync",
	Usage: "sync [options]",
	Short: "sync from a Zed lake pool to a Kafka topic",
	Long: `
The sync command syncs data from a Zed lake to a Kafka topic acting
as a source of Zed data for Kafka.
The Zed records are transcoded into Avro and published
on the specified Kafka topic.

See http://github.com/brimdata/zinger/README.md for a description
of the sync algorithm and the layout of the Zed records in the Zed data pool.
`,
	New: NewSync,
}

func init() {
	SyncSpec.Add(FromSpec)
	SyncSpec.Add(ToSpec)
	root.Zinger.Add(SyncSpec)
}

type Sync struct {
	*root.Command
	pool string
}

func NewSync(parent charm.Command, fs *flag.FlagSet) (charm.Command, error) {
	c := &Sync{Command: parent.(*root.Command)}
	fs.StringVar(&c.pool, "pool", "", "name of Zed lake pool")
	return c, nil
}

func (s *Sync) Run(args []string) error {
	return errors.New("TBD")
}
