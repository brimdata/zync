package main

import (
	"fmt"
	"os"

	_ "github.com/mccanne/zinger/cmd/zinger/listen"
	_ "github.com/mccanne/zinger/cmd/zinger/ls"
	"github.com/mccanne/zinger/cmd/zinger/root"
	_ "github.com/mccanne/zinger/cmd/zinger/schema"
)

// These variables are populated via the Go linker.
var (
	version   = "unknown"
	zqVersion = "unknown"
)

func main() {
	_, err := root.Zinger.ExecRoot(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}
