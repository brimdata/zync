package listen

import (
	"flag"
	"fmt"
	"os"

	"github.com/mccanne/charm"
	"github.com/mccanne/zinger"
	"github.com/mccanne/zinger/cmd/zinger/root"
	"github.com/mccanne/zinger/registry"
)

var Listen = &charm.Spec{
	Name:  "listen",
	Usage: "listen [options]",
	Short: "listen as a daemon and transcode streaming posts onto kafka",
	Long: `
The listen command allows you to...
TBD.`,
	New: New,
}

func init() {
	root.Zinger.Add(Listen)
}

type Command struct {
	*root.Command
	listenAddr string
}

func New(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{Command: parent.(*root.Command)}
	f.StringVar(&c.listenAddr, "l", ":9890", "ip:port's of port and interface to listen on")
	return c, nil
}

func (c *Command) Run(args []string) error {
	servers := c.KafkaCluster()
	reg := registry.NewConnection(c.Subject, c.RegistryCluster())
	producer, err := zinger.NewProducer(servers, reg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return zinger.Run(c.listenAddr, producer)
}
