package ls

import (
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/mccanne/charm"
	"github.com/mccanne/zinger/cmd/zinger/root"
	"github.com/mccanne/zinger/pkg/registry"
)

var Ls = &charm.Spec{
	Name:  "ls",
	Usage: "ls [-l] [schemaID ... ]",
	Short: "list schemas",
	Long: `
The ls command allows you to list the schema IDs under the specified namespace
and subject. Specify the -l argument causes the content of each schema to be
displayed as NDJSON.
`,
	New: New,
}

func init() {
	root.Zinger.Add(Ls)
}

type Command struct {
	*root.Command
	registryCluster string
	Subject         string
	lflag           bool
}

func New(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{Command: parent.(*root.Command)}
	f.BoolVar(&c.lflag, "l", false, "show schema contents as ndjson")
	f.StringVar(&c.registryCluster, "r", "localhost:8081", "[addr]:port list of one more kafka registry servers")
	f.StringVar(&c.Subject, "s", "kavrotest-value", "subject name for kafka schema registry")
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

func servers(s string) []string {
	return strings.Split(s, ",")
}

func (c *Command) Run(args []string) error {
	reg := registry.NewConnection(c.Subject, servers(c.registryCluster))
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
