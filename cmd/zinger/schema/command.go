package schema

import (
	"errors"
	"flag"
	"fmt"
	"strconv"

	"github.com/mccanne/charm"
	"github.com/mccanne/zinger/cmd/zinger/root"
	"github.com/mccanne/zinger/registry"
)

var Schema = &charm.Spec{
	Name:  "schema",
	Usage: "schema [options] id",
	Short: "lookup and display schema by id",
	Long: `
The schema command allows you to...
TBD.`,
	New: New,
}

func init() {
	root.Zinger.Add(Schema)
}

type Command struct {
	*root.Command
}

func New(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{Command: parent.(*root.Command)}
	return c, nil
}

func (c *Command) Run(args []string) error {
	if len(args) != 1 {
		return errors.New("schema command must be run with schema id")
	}
	id, err := strconv.Atoi(args[0])
	if err != nil {
		return err
	}
	servers := c.RegistryCluster()
	reg := registry.NewConnection(c.Subject, servers)
	schema, err := reg.Lookup(id)
	if err != nil {
		return err
	}
	fmt.Println(string(schema))
	return nil
}
