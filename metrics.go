package main

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	rowanNamespace = "rowan"
)

var (
	InboundHTTPRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: rowanNamespace,
			Name:      "inbound_http_requests_total",
			Help:      "Cumulative number of inbound HTTP requests by status code.",
		},
		[]string{"code"},
	)

	OutboundHTTPRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: rowanNamespace,
			Name:      "outbound_http_requests_total",
			Help:      "Cumulative number of outbound HTTP requests by status code.",
		},
		[]string{"code"},
	)

	OutboundHTTPLatencies = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: rowanNamespace,
		Name:      "outbound_http_requests_latencies",
		Help:      "Duration in seconds from expected to actual delivery time.",
	},
		[]string{"code"},
	)
)

func init() {
	prometheus.MustRegister(InboundHTTPRequests)
	prometheus.MustRegister(OutboundHTTPRequests)
	prometheus.MustRegister(OutboundHTTPLatencies)
}

func serveMetrics(port int) {
	http.ListenAndServe(fmt.Sprintf(":%d", port), promhttp.Handler())
}
