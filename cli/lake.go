package cli

import (
	"flag"

	zedcli "github.com/brimdata/zed/cli"
)

type LakeFlags struct {
	zedcli.LakeFlags
}

// SetFlags calls LakeFlags.LakeFlags.SetFlags and then, for any flag added, it
// prepends "Zed " to flag.Flag.Usage.
func (l *LakeFlags) SetFlags(fs *flag.FlagSet) {
	beforeSetFlags := map[string]struct{}{}
	fs.VisitAll(func(f *flag.Flag) {
		beforeSetFlags[f.Name] = struct{}{}
	})
	l.LakeFlags.SetFlags(fs)
	fs.VisitAll(func(f *flag.Flag) {
		if _, ok := beforeSetFlags[f.Name]; !ok {
			f.Usage = "Zed " + f.Usage
		}
	})

}
