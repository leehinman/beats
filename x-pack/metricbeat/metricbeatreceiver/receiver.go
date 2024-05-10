package metricbeatreceiver

import (
	"context"

	"github.com/elastic/beats/v7/libbeat/beat"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"
)

type metricbeatReceiver struct {
	host         component.Host
	cancel       context.CancelFunc
	logger       *zap.Logger
	config       *Config
	nextConsumer consumer.Metrics
	beat         *beat.Beat
	beater       beat.Beater
}

func (mb *metricbeatReceiver) Start(ctx context.Context, host component.Host) error {
	mb.host = host
	go func() {
		mb.beater.Run(mb.beat)
	}()
	return nil
}

func (mb *metricbeatReceiver) Shutdown(ctx context.Context) error {
	mb.beater.Stop()
	return nil
}
