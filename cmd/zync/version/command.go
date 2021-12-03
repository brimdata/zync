package ls

import (
	"errors"
	"flag"
	"fmt"
	"runtime/debug"

	"github.com/brimdata/zed/pkg/charm"
	"github.com/brimdata/zync/cmd/zync/root"
)

func init() {
	root.Zync.Add(Spec)
}

var Spec = &charm.Spec{
	Name:  "version",
	Usage: "version",
	Short: "print version",
	Long:  "The version command prints zync version information.",
	New: func(parent charm.Command, _ *flag.FlagSet) (charm.Command, error) {
		return &Command{Command: parent.(*root.Command)}, nil
	},
}

type Command struct {
	*root.Command
}

// version is meant to be set by the linker.  See Makefile.
var version string

func (c *Command) Run(args []string) error {
	if len(args) > 0 {
		return errors.New("too many arguments")
	}
	if version == "" {
		info, ok := debug.ReadBuildInfo()
		if !ok {
			return errors.New("binary built without version or module support")
		}
		version = info.Main.Version
	}
	fmt.Println(version)
	return nil

}
