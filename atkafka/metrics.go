package atkafka

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	handledEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "atkafka",
		Name:      "handled_events",
	}, []string{"action_name", "collection"})

	producedEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "atkafka",
		Name:      "produced_events",
	}, []string{"status"})

	plcRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "atkafka",
		Name:      "plc_requests",
	}, []string{"kind", "status", "cached"})

	cacheSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "atkafka",
		Name:      "cache_size",
	}, []string{"kind"})

	apiRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "atkafka",
		Name:      "api_requests",
	}, []string{"kind", "status", "cached"})

	acksSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "atkafka",
		Name:      "acks_sent",
	}, []string{"status"})

	tapEvtBufferSize = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "atkafka",
		Name:      "tap_event_buffer_size",
	})

	tapAckBufferSize = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "atkafka",
		Name:      "tap_ack_event_buffer_size",
	})
)
