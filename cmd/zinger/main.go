package main

import (
	"fmt"
	"os"

	_ "github.com/brimdata/zinger/cmd/zinger/consume"
	_ "github.com/brimdata/zinger/cmd/zinger/info"
	_ "github.com/brimdata/zinger/cmd/zinger/ls"
	_ "github.com/brimdata/zinger/cmd/zinger/produce"
	"github.com/brimdata/zinger/cmd/zinger/root"
	_ "github.com/brimdata/zinger/cmd/zinger/sync"
)

// These variables are populated via the Go linker.
var (
	version   = "unknown"
	zqVersion = "unknown"
)

func main() {
	if err := root.Zinger.ExecRoot(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}
