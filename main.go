package main

import (
	"fmt"
	"os"

	"github.com/mysql-pitr/mysql-pitr/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
