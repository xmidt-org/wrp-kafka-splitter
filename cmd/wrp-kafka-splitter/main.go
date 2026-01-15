// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	"xmidt-org/wrp-kafka-splitter/internal/app"
)

func main() {
	app, err := app.WrpKafkaRouter(os.Args[1:])
	if err == nil {
		app.Run()
		return
	}

	fmt.Fprintln(os.Stderr, err)
	os.Exit(-1)
}
