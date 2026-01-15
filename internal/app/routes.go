// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/xmidt-org/arrange/arrangehttp"
	"github.com/xmidt-org/arrange/arrangepprof"
	"github.com/xmidt-org/httpaux"
	"github.com/xmidt-org/touchstone/touchhttp"
	"go.uber.org/fx"
)

func provideHealthCheck() fx.Option {
	return fx.Provide(
		fx.Annotated{
			Name: "servers.health.metrics",
			Target: touchhttp.ServerBundle{}.NewInstrumenter(
				touchhttp.ServerLabel, "health",
			),
		},
		fx.Annotate(
			func(metrics touchhttp.ServerInstrumenter, path HealthPath) arrangehttp.Option[http.Server] {
				return arrangehttp.AsOption[http.Server](
					func(s *http.Server) {
						mux := chi.NewMux()
						mux.Method("GET", string(path), httpaux.ConstantHandler{
							StatusCode: http.StatusOK,
						})
						s.Handler = metrics.Then(mux)
					},
				)
			},
			fx.ParamTags(`name:"servers.health.metrics"`),
			fx.ResultTags(`group:"servers.health.options"`),
		),
	)
}

func provideMetricEndpoint() fx.Option {
	return fx.Provide(
		fx.Annotate(
			func(metrics touchhttp.Handler, path MetricsPath) arrangehttp.Option[http.Server] {
				return arrangehttp.AsOption[http.Server](
					func(s *http.Server) {
						mux := chi.NewMux()
						mux.Method("GET", string(path), metrics)
						s.Handler = mux
					},
				)
			},
			fx.ResultTags(`group:"servers.metrics.options"`),
		),
	)
}

func providePprofEndpoint() fx.Option {
	return fx.Provide(
		fx.Annotate(
			func(pathPrefix PprofPathPrefix) arrangehttp.Option[http.Server] {
				return arrangehttp.AsOption[http.Server](
					func(s *http.Server) {
						s.Handler = arrangepprof.HTTP{
							PathPrefix: string(pathPrefix),
						}.New()
					},
				)
			},
			fx.ResultTags(`group:"servers.pprof.options"`),
		),
	)
}
