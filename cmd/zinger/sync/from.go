package sync

import (
	"errors"
	"flag"

	"github.com/brimdata/zed/pkg/charm"
)

var FromSpec = &charm.Spec{
	Name:  "from",
	Usage: "from [options]",
	Short: "sync from a Zed lake pool to a Kafka topic",
	Long: `
The from command syncs data from a Zed lake to a Kafka topic acting
as a source of Zed data for Kafka.
The Zed records are transcoded into Avro and published
on the specified Kafka topic.

XXX document technique and expected format of lake records.
`,
	New: NewFrom,
}

type From struct {
	*Sync
}

func NewFrom(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &From{Sync: parent.(*Sync)}
	return c, nil
}

func (f *From) Run(args []string) error {
	return errors.New("TBD")
}
