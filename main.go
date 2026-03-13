package main

import (
	"log"

	"github.com/dps123/rp/cli"
)

func main() {
	app := cli.New()
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
