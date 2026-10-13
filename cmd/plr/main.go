// Command plr is the pipeline-runner time service.
package main

import (
	"fmt"
	"os"
)

// version is stamped at build time with -ldflags "-X main.version=<sha>".
var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println(version)
		return
	}
	fmt.Fprintln(os.Stderr, "usage: plr version")
	os.Exit(2)
}
