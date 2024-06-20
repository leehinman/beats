// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package otelconsumer

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/mapstr"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func init() {
	outputs.RegisterType("otelconsumer", makeOtelConsumer)
}

type otelConsumer struct {
	observer        outputs.Observer
	logsConsumer    consumer.Logs
	metricsConsumer consumer.Metrics
	beatInfo        beat.Info
}

func makeOtelConsumer(_ outputs.IndexManager, beat beat.Info, observer outputs.Observer, cfg *config.C) (outputs.Group, error) {

	out := &otelConsumer{
		observer:        observer,
		logsConsumer:    beat.LogsConsumer,
		metricsConsumer: beat.MetricsConsumer,
		beatInfo:        beat,
	}

	ocConfig := defaultConfig()
	if err := cfg.Unpack(&ocConfig); err != nil {
		return outputs.Fail(err)
	}
	return outputs.Success(ocConfig.Queue, -1, 0, nil, out)
}

func (out *otelConsumer) Close() error {
	return nil
}

func (out *otelConsumer) Publish(ctx context.Context, batch publisher.Batch) error {
	switch {
	case out.logsConsumer != nil:
		return out.logsPublish(ctx, batch)
	case out.metricsConsumer != nil:
		return out.metricsPublish(ctx, batch)
	default:
		panic(fmt.Errorf("an otel consumer must be specified"))
	}
}

func (out *otelConsumer) logsPublish(_ context.Context, batch publisher.Batch) error {
	defer batch.ACK()
	st := out.observer
	pLogs := plog.NewLogs()
	resourceLogs := pLogs.ResourceLogs().AppendEmpty()
	sourceLogs := resourceLogs.ScopeLogs().AppendEmpty()
	logRecords := sourceLogs.LogRecords()

	events := batch.Events()
	for _, event := range events {
		fmt.Fprintf(os.Stderr, "Got Event: %v\n", event.Content)
		logRecord := logRecords.AppendEmpty()
		meta := event.Content.Meta.Clone()
		meta["beat"] = out.beatInfo.Beat
		meta["version"] = out.beatInfo.Version
		meta["type"] = "_doc"

		beatEvent := event.Content.Fields.Clone()
		beatEvent["@timestamp"] = event.Content.Timestamp
		beatEvent["@metadata"] = meta
		fmt.Fprintf(os.Stderr, "beatEvent: %v\n", beatEvent)
		logRecord.SetTimestamp(pcommon.NewTimestampFromTime(event.Content.Timestamp))
		pcommonEvent := mapstrToPcommonMap(beatEvent)
		pcommonEvent.CopyTo(logRecord.Body().SetEmptyMap())
	}
	if err := out.logsConsumer.ConsumeLogs(context.TODO(), pLogs); err != nil {
		return fmt.Errorf("error otel log consumer: %w", err)
	}
	// Here is where we convert to otel pdata.Logs
	st.NewBatch(len(events))
	st.AckedEvents(len(events))
	return nil
}

func (out *otelConsumer) metricsPublish(_ context.Context, batch publisher.Batch) error {
	defer batch.ACK()
	st := out.observer
	pMetrics := pmetric.NewMetrics()
	resourceMetrics := pMetrics.ResourceMetrics().AppendEmpty()

	events := batch.Events()
	for _, event := range events {
		attr := mapstr.Union(event.Content.Fields.Flatten(), event.Content.Meta.Flatten())
		resourceMetrics.Resource().Attributes().FromRaw(attr)
		m := resourceMetrics.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName("test_metric")
		m.SetEmptyGauge().DataPoints().AppendEmpty()
	}
	if err := out.metricsConsumer.ConsumeMetrics(context.TODO(), pMetrics); err != nil {
		return fmt.Errorf("error otel metric consumer: %w", err)
	}
	// Here is where we convert to otel pdata.Logs
	st.NewBatch(len(events))
	st.AckedEvents(len(events))
	return nil
}

func (out *otelConsumer) String() string {
	return "otelconsumer"
}

func mapstrToPcommonMap(m mapstr.M) pcommon.Map {
	out := pcommon.NewMap()
	for k, v := range m {
		switch x := v.(type) {
		case string:
			out.PutStr(k, x)
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			out.PutInt(k, x.(int64))
		case float32, float64:
			out.PutDouble(k, x.(float64))
		case bool:
			out.PutBool(k, x)
		case mapstr.M:
			dest := out.PutEmptyMap(k)
			newMap := mapstrToPcommonMap(x)
			newMap.CopyTo(dest)
		case time.Time:
			out.PutInt(k, x.UnixMilli())
		default:
			out.PutStr(k, fmt.Sprintf("unknown type: %T", x))
		}
	}
	return out
}
