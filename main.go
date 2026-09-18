//revive:disable:package-comments
package main

import (
	"os"

	"github.com/grpcd/gateway/internal/cli"
)

func main() { os.Exit(cli.Run()) }
