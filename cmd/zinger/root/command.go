package root

import (
	"flag"
	"log"

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
Zinger interconnects zeek and Kafka/Acvro using the Kafka Schema Registery.

Use -k to specify the Kafka bootstrap servers as a comma-separated list
of addresses (with form address:port).  Likewise, use -r to specify
one or more registry servers.

Use -t to specify the Kafka topic. Currently, all data is published to
a single topic.

Use -n to specify a namespace and -s to specify a subject both used when
automatically creating new schemas for transcoded Zeek/ZNG data.
`,
	New: New,
}

type Command struct {
	charm.Command
}

func init() {
	Zinger.Add(charm.Help)
}

func New(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{}
	log.SetPrefix("zinger")
	return c, nil
}

func (c *Command) Run(args []string) error {
	return Zinger.Exec(c, []string{"help"})
}
