package root

import (
	"flag"
	"log"

	"github.com/brimdata/zed/pkg/charm"
)

// These variables are populated via the Go linker.
var (
	Version   = "unknown"
	ZqVersion = "unknown"
)

var Zync = &charm.Spec{
	Name:  "zync",
	Usage: "zync command [options] [arguments...]",
	Short: "synchronize a Zed lake with Kafka",
	Long: `
Zync interconnects a Zed lake with Kafka/Avro using the Kafka Schema Registery.
Syncronization can flow in either direction with the "sync from" or "sync to"
subcommands.  Zync also includes some simple support for consuming and
producing data on Kafka topic as Avro to/from the Zed formats.  See the
"consume" and "produce" subcommands for more details.

Zync requires a schema registry and is configured using two files
in ~/.zync.

Currently, zync supports only SASL authentication but it will be
easy to add support for any other schemes supported by your Kafka
installation.

The kafka.json file should have the form
of http://github.com/brimdata/zync/kafka.json.

The schema_registry.json file should have the form
of http://github.com/brimdata/zync/schema_registry.json.
`,
	New: New,
}

type Command struct {
	charm.Command
}

func New(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{}
	log.SetPrefix("zync")
	return c, nil
}

func (c *Command) Run(args []string) error {
	if len(args) == 0 {
		return charm.NeedHelp
	}
	return charm.ErrNoRun
}
