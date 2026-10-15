package natsotel

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// instruments holds the messaging metrics. An instrument that could not be
// created is replaced by a no-op one, so recording never panics.
type instruments struct {
	processDuration   metric.Float64Histogram
	operationDuration metric.Float64Histogram
	consumed          metric.Int64Counter
	sent              metric.Int64Counter
}

func newInstruments(m metric.Meter) *instruments {
	nop := noop.Meter{}
	hist := func(name, desc string) metric.Float64Histogram {
		h, err := m.Float64Histogram(name, metric.WithUnit("s"), metric.WithDescription(desc))
		if err != nil {
			h, _ = nop.Float64Histogram(name)
		}
		return h
	}
	count := func(name, desc string) metric.Int64Counter {
		c, err := m.Int64Counter(name, metric.WithUnit("{message}"), metric.WithDescription(desc))
		if err != nil {
			c, _ = nop.Int64Counter(name)
		}
		return c
	}
	return &instruments{
		processDuration:   hist("messaging.process.duration", "Duration of processing a received message."),
		operationDuration: hist("messaging.client.operation.duration", "Duration of a messaging client operation."),
		consumed:          count("messaging.client.consumed.messages", "Number of messages delivered to the application."),
		sent:              count("messaging.client.sent.messages", "Number of messages sent by the application."),
	}
}
