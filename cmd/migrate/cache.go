package main

import (
	"fmt"

	"github.com/johndauphine/dmt/internal/command"

	"github.com/urfave/cli/v2"
)

func cacheClear(c *cli.Context) error {
	out, err := command.ClearTypeCache(c.Bool("ai-only"))
	if err != nil {
		return err
	}
	fmt.Print(out)
	return nil
}
