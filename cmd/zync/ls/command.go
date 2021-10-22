package ls

import (
	"flag"
	"fmt"

	"github.com/brimdata/zed/pkg/charm"
	"github.com/brimdata/zync/cli"
	"github.com/brimdata/zync/cmd/zync/root"
	"github.com/riferrei/srclient"
)

var Ls = &charm.Spec{
	Name:  "ls",
	Usage: "ls [-l] [schemaID ... ]",
	Short: "list schemas",
	Long: `
The ls command prints the schema registry subjects and schema information
for the latest schema in each subject.
The endpoint URL and credentials are obtained from $HOME/.confluent/schema_registry.json.

This runs a bit slow as each schema entry is fetched synchronously from the registry.
`,
	New: New,
}

func init() {
	root.Zync.Add(Ls)
}

type Command struct {
	*root.Command
	flags cli.Flags
}

func New(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{Command: parent.(*root.Command)}
	c.flags.SetFlags(f)
	return c, nil
}

func (c *Command) Run(args []string) error {
	url, secret, err := cli.SchemaRegistryEndpoint()
	if err != nil {
		return err
	}
	registry := srclient.CreateSchemaRegistryClient(url)
	registry.SetCredentials(secret.User, secret.Password)
	subjects, err := registry.GetSubjects()
	if err != nil {
		return err
	}
	for _, subject := range subjects {
		fmt.Printf("subject %s:\n", subject)
		schema, err := registry.GetLatestSchema(subject)
		if err != nil {
			return err
		}
		fmt.Printf("  id %d\n", schema.ID())
		fmt.Printf("  version %d\n", schema.Version())
		fmt.Printf("  schema %s\n", schema.Schema())
	}
	return nil
}
