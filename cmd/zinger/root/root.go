package root

import (
	"errors"
	"flag"

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
TBD`,
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
	//f.StringVar(&c.kafkaCluster, "k", "localhost:9092", "ip:port's of one or more kafka servers")
	//f.StringVar(&c.registryCluster, "r", "localhost:8081", "ip:port's of one more kafka registry servers")
	return c, nil
}

func (c *Command) Run(args []string) error {
	return errors.New("TBD: no default zinger subcommend")
}
