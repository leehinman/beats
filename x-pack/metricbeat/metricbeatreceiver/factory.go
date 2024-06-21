package metricbeatreceiver

import (
	"context"
	"fmt"

	"github.com/elastic/beats/v7/libbeat/cmd/instance"
	"github.com/elastic/beats/v7/libbeat/common/reload"
	"github.com/elastic/beats/v7/libbeat/plugin"
	"github.com/elastic/beats/v7/libbeat/publisher/pipeline"
	"github.com/elastic/beats/v7/metricbeat/beater"
	"github.com/elastic/beats/v7/metricbeat/cmd"
	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/elastic/elastic-agent-libs/monitoring"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

const (
	typeStr = "metricbeatreceiver"
)

func createDefaultConfig() component.Config {
	return &Config{
		FakeString: "metricbeatreceiver config default 'FakeString'",
	}
}

func createLogsReceiver(_ context.Context, params receiver.CreateSettings, baseCfg component.Config, consumer consumer.Logs) (receiver.Logs, error) {
	logger := params.Logger
	err := logp.ConfigureWithCore(logp.DefaultConfig(logp.DefaultEnvironment), params.Logger.Core())
	if err != nil {
		return nil, fmt.Errorf("Error configuring beats logp: %w", err)
	}

	cfg := baseCfg.(*Config)

	// TODO x-pack/filebeat/cmd/root.go has Global Processors defined, probably need to bring those over
	settings := cmd.MetricbeatSettings()
	settings.ElasticLicensed = true

	b, err := instance.NewBeat(settings.Name, settings.IndexPrefix, settings.Version, settings.ElasticLicensed, settings.Initialize)
	if err != nil {
		return nil, err
	}

	if err := plugin.Initialize(); err != nil {
		return nil, fmt.Errorf("error initializing plugins: %w", err)
	}

	if err := b.ReceiverConfigure(settings, cfg.Beatconfig); err != nil {
		return nil, fmt.Errorf("couldn't configure beat: %w", err)
	}

	mbCreator := beater.DefaultCreator()

	sub, err := b.BeatConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to get beat config: %w", err)
	}

	reg := monitoring.Default.GetRegistry("metricbeatreceiver")
	if reg == nil {
		reg = monitoring.Default.NewRegistry("metricbeatreceiver")
	}

	b.Beat.Info.LogsConsumer = consumer

	outputEnabled := b.Config.Output.IsSet() && b.Config.Output.Config().Enabled()
	if !outputEnabled {
		if b.Manager.Enabled() {
			logp.Info("Output is configured through Central Management")
		} else {
			msg := "no outputs are defined, please define one under the output section"
			logp.Info(msg)
			return nil, fmt.Errorf(msg)
		}
	}

	monitors := pipeline.Monitors{
		Metrics:   reg,
		Telemetry: monitoring.GetNamespace("metricbeatreceiverstate").GetRegistry(),
		Logger:    logp.L().Named("publisher"),
		Tracer:    b.Instrumentation.Tracer(),
	}

	outputFactory := b.MakeOutputFactory(b.Config.Output)

	pipelineSettings := pipeline.Settings{
		Processors:     b.Processors(),
		InputQueueSize: b.InputQueueSize,
	}
	publisher, err := pipeline.LoadWithSettings(b.Info, monitors, b.Config.Pipeline, outputFactory, pipelineSettings)
	if err != nil {
		return nil, fmt.Errorf("error initializing publisher: %w", err)
	}

	reload.MetricbeatRegisterV2.MustRegisterOutput(b.MakeOutputReloader(publisher.OutputReloader()))
	//reload.RegisterV2.Register(reload.OutputRegName, b.MakeOutputReloader(publisher.OutputReloader()))

	b.Publisher = publisher
	mbBeater, err := mbCreator(&b.Beat, sub)
	if err != nil {
		return nil, fmt.Errorf("error getting metricbeat creator:%w", err)
	}

	mbRcvr := &metricbeatReceiver{
		logger:       logger,
		nextConsumer: consumer,
		config:       cfg,
		beat:         &b.Beat,
		beater:       mbBeater,
	}

	return mbRcvr, nil
}

// NewFactory creates a factory for tailtracer receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, component.StabilityLevelAlpha))

}
