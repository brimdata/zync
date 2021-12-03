package main

import (
	"fmt"
	"os"

	_ "github.com/brimdata/zync/cmd/zync/consume"
	_ "github.com/brimdata/zync/cmd/zync/etl"
	_ "github.com/brimdata/zync/cmd/zync/from-kafka"
	_ "github.com/brimdata/zync/cmd/zync/info"
	_ "github.com/brimdata/zync/cmd/zync/ls"
	_ "github.com/brimdata/zync/cmd/zync/produce"
	"github.com/brimdata/zync/cmd/zync/root"
	_ "github.com/brimdata/zync/cmd/zync/to-kafka"
	_ "github.com/brimdata/zync/cmd/zync/version"
)

func main() {
	if err := root.Zync.ExecRoot(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}
