package cli

import (
	"flag"
	"os"
)

type ShaperFlags struct {
	Path string
}

func (s *ShaperFlags) SetFlags(fs *flag.FlagSet) {
	fs.StringVar(&s.Path, "shaper", "", "path of optional Zed script for shaping")
}

func (s *ShaperFlags) Load() (string, error) {
	if s.Path == "" {
		return "", nil
	}
	b, err := os.ReadFile(s.Path)
	return string(b), err
}
