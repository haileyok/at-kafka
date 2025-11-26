package atkafka

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var handledEvents = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "atkafka",
	Name:      "handled_events",
}, []string{"action_name", "collection"})

var producedEvents = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "atkafka",
	Name:      "produced_events",
}, []string{"status"})
