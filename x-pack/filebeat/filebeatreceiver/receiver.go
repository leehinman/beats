package filebeatreceiver

import (
	"context"

	"github.com/elastic/beats/v7/libbeat/beat"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"
)

type filebeatReceiver struct {
	host         component.Host
	cancel       context.CancelFunc
	logger       *zap.Logger
	config       *Config
	nextConsumer consumer.Logs
	beat         *beat.Beat
	beater       beat.Beater
}

func (fb *filebeatReceiver) Start(ctx context.Context, host component.Host) error {
	fb.host = host
	go func() {
		fb.beater.Run(fb.beat)
	}()
	return nil
}

func (fb *filebeatReceiver) Shutdown(ctx context.Context) error {
	fb.beater.Stop()
	return nil
}
