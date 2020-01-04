package ls

import (
	"errors"
	"flag"
	"fmt"
	"strconv"

	"github.com/mccanne/charm"
	"github.com/mccanne/zinger/cmd/zinger/root"
	"github.com/mccanne/zinger/pkg/registry"
)

var Ls = &charm.Spec{
	Name:  "ls",
	Usage: "ls [options]",
	Short: "list schemas",
	Long: `
The ls command allows you to...
TBD.`,
	New: New,
}

func init() {
	root.Zinger.Add(Ls)
}

type Command struct {
	*root.Command
	lflag bool
}

func New(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{Command: parent.(*root.Command)}
	f.BoolVar(&c.lflag, "l", false, "show schema contents as ndjson")
	return c, nil
}

func parseIDs(in []string) ([]int, error) {
	var out []int
	for _, s := range in {
		v, err := strconv.Atoi(s)
		if err != nil {
			return nil, errors.New("ls arguments must be a list of schema IDs")
		}
		out = append(out, v)
	}
	return out, nil
}

func (c *Command) Run(args []string) error {
	reg := registry.NewConnection(c.Subject, c.RegistryCluster())
	var ids []int
	var err error
	if len(args) != 0 {
		ids, err = parseIDs(args)
	} else {
		ids, err = reg.List(c.Subject)
	}
	if err != nil {
		return err
	}
	for _, id := range ids {
		if c.lflag {
			schema, err := reg.Lookup(id)
			if err != nil {
				return err
			}
			fmt.Println(string(schema))
		} else {
			fmt.Println(id)
		}
	}
	return nil

}
