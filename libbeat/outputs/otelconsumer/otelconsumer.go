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

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/mapstr"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func init() {
	outputs.RegisterType("otelconsumer", makeOtelConsumer)
}

type otelConsumer struct {
	observer     outputs.Observer
	logsConsumer consumer.Logs
}

func makeOtelConsumer(_ outputs.IndexManager, beat beat.Info, observer outputs.Observer, cfg *config.C) (outputs.Group, error) {

	out := &otelConsumer{
		observer:     observer,
		logsConsumer: beat.LogsConsumer,
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

func (out *otelConsumer) Publish(_ context.Context, batch publisher.Batch) error {
	defer batch.ACK()
	st := out.observer
	pLogs := plog.NewLogs()
	resourceLogs := pLogs.ResourceLogs().AppendEmpty()
	sourceLogs := resourceLogs.ScopeLogs().AppendEmpty()
	logRecords := sourceLogs.LogRecords()

	events := batch.Events()
	for _, event := range events {
		logRecord := logRecords.AppendEmpty()
		if val, err := event.Content.Fields.GetValue("message"); err == nil {
			if s, ok := val.(string); ok == true {
				logRecord.Body().SetStr(s)
			}
			_ = event.Content.Fields.Delete("message")
		}
		logRecord.SetTimestamp(pcommon.NewTimestampFromTime(event.Content.Timestamp))
		attr := mapstr.Union(event.Content.Fields.Flatten(), event.Content.Meta.Flatten())
		logRecord.Attributes().FromRaw(attr)
	}
	if err := out.logsConsumer.ConsumeLogs(context.TODO(), pLogs); err != nil {
		return fmt.Errorf("error otel log consumer: %w", err)
	}
	// Here is where we convert to otel pdata.Logs
	st.NewBatch(len(events))
	st.Acked(len(events))
	return nil
}

func (out *otelConsumer) String() string {
	return "otelconsumer"
}

// Doesn't look like we need this since Attributes don't seem to be nested
func mapstrToPcommonMap(m mapstr.M) (pcommon.Map) {
	out := pcommon.NewMap()
	for k,v := range m {
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
		default:
			out.PutStr(k, fmt.Sprintf("unknown type: %T", x))
		}
	}
	return out
}
