package cli

import (
	"flag"

	"github.com/brimdata/zed/cli/lakeflags"
)

type LakeFlags struct {
	lakeflags.Flags
}

// SetFlags calls Flags.SetFlags and then, for any flag added, it
// prepends "Zed " to flag.Flag.Usage.
func (l *LakeFlags) SetFlags(fs *flag.FlagSet) {
	beforeSetFlags := map[string]struct{}{}
	fs.VisitAll(func(f *flag.Flag) {
		beforeSetFlags[f.Name] = struct{}{}
	})
	l.Flags.SetFlags(fs)
	fs.VisitAll(func(f *flag.Flag) {
		if _, ok := beforeSetFlags[f.Name]; !ok {
			f.Usage = "Zed " + f.Usage
		}
	})

}
