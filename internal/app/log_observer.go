// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"log/slog"
	"xmidt-org/wrp-kafka-splitter/internal/log"
	"xmidt-org/wrp-kafka-splitter/internal/observe"

	"go.uber.org/fx"
)

type logObserverIn struct {
	fx.In
	Logger *slog.Logger
}

type LogObserverOut struct {
	fx.Out
	Subject *observe.Subject[log.Event]
}

func provideLogObserver(in logObserverIn) (LogObserverOut, error) {
	subject := log.New(in.Logger)
	return LogObserverOut{
		Subject: subject,
	}, nil
}
