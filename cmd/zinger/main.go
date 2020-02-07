package main

import (
	"fmt"
	"os"

	_ "github.com/brimsec/zinger/cmd/zinger/listen"
	_ "github.com/brimsec/zinger/cmd/zinger/ls"
	_ "github.com/brimsec/zinger/cmd/zinger/post"
	"github.com/brimsec/zinger/cmd/zinger/root"
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
