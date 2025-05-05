// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exporterhelper // import "go.opentelemetry.io/collector/exporter/exporterhelper"

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/queuebatch"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/request"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/sizer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pipeline"
)

var (
	tracesMarshaler   = &ptrace.ProtoMarshaler{}
	tracesUnmarshaler = &ptrace.ProtoUnmarshaler{}
)

// NewTracesQueueBatchSettings returns a new QueueBatchSettings to configure to WithQueueBatch when using ptrace.Traces.
// Experimental: This API is at the early stage of development and may change without backward compatibility
// until https://github.com/open-telemetry/opentelemetry-collector/issues/8122 is resolved.
func NewTracesQueueBatchSettings() QueueBatchSettings {
	return QueueBatchSettings{
		Encoding: tracesEncoding{},
		Sizers: map[RequestSizerType]request.Sizer[Request]{
			RequestSizerTypeRequests: NewRequestsSizer(),
			RequestSizerTypeItems:    request.NewItemsSizer(),
			RequestSizerTypeBytes: request.BaseSizer{
				SizeofFunc: func(req request.Request) int64 {
					return int64(tracesMarshaler.TracesSize(req.(*tracesRequest).td))
				},
			},
		},
	}
}

type tracesRequest struct {
	td         ptrace.Traces
	links      []trace.Link
	cachedSize int
}

func newTracesRequest(td ptrace.Traces, links []trace.Link) Request {
	return &tracesRequest{
		td:         td,
		links:      links,
		cachedSize: -1,
	}
}

type tracesEncoding struct{}

type tracesWithSpanContexts struct {
	Traces      []byte              `json:"traces"`
	SpanContext []trace.SpanContext `json:"span_context"`
}

// Helper for JSON unmarshaling of SpanContextConfig

type spanContextConfigJSON struct {
	TraceID    string
	SpanID     string
	TraceFlags string
	TraceState string
	Remote     bool
}

func unmarshalSpanContextConfig(data []byte) (trace.SpanContextConfig, error) {
	var aux spanContextConfigJSON
	if err := json.Unmarshal(data, &aux); err != nil {
		return trace.SpanContextConfig{}, err
	}
	tid, err := trace.TraceIDFromHex(aux.TraceID)
	if err != nil {
		return trace.SpanContextConfig{}, err
	}
	sid, err := trace.SpanIDFromHex(aux.SpanID)
	if err != nil {
		return trace.SpanContextConfig{}, err
	}
	ts, err := trace.ParseTraceState(aux.TraceState)
	if err != nil {
		return trace.SpanContextConfig{}, err
	}
	tf, err := hex.DecodeString(aux.TraceFlags)
	if err != nil {
		return trace.SpanContextConfig{}, err
	}
	if len(tf) != 1 {
		return trace.SpanContextConfig{}, errors.New("invalid trace flags")
	}
	return trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.TraceFlags(tf[0]),
		TraceState: ts,
		Remote:     aux.Remote,
	}, nil
}

func (tracesEncoding) Unmarshal(bytes []byte) (Request, error) {
	var twl struct {
		Traces      []byte            `json:"traces"`
		SpanContext []json.RawMessage `json:"span_context"`
	}
	if err := json.Unmarshal(bytes, &twl); err != nil {
		return nil, err
	}
	traces, err := tracesUnmarshaler.UnmarshalTraces(twl.Traces)
	if err != nil {
		return nil, err
	}
	links := make([]trace.Link, len(twl.SpanContext))
	for i, raw := range twl.SpanContext {
		cfg, err := unmarshalSpanContextConfig(raw)
		if err != nil {
			return nil, err
		}
		links[i] = trace.Link{SpanContext: trace.NewSpanContext(cfg)}
	}
	return newTracesRequest(traces, links), nil
}

func (tracesEncoding) Marshal(req Request) ([]byte, error) {
	tr := req.(*tracesRequest)
	tracesBytes, err := tracesMarshaler.MarshalTraces(tr.td)
	if err != nil {
		return nil, err
	}
	spanContexts := make([]trace.SpanContext, len(tr.links))
	for i, l := range tr.links {
		spanContexts[i] = l.SpanContext
	}
	twl := tracesWithSpanContexts{
		Traces:      tracesBytes,
		SpanContext: spanContexts,
	}
	return json.Marshal(twl)
}

func (req *tracesRequest) OnError(err error) Request {
	var traceError consumererror.Traces
	if errors.As(err, &traceError) {
		return newTracesRequest(traceError.Data(), req.links)
	}
	return req
}

func (req *tracesRequest) ItemsCount() int {
	return req.td.SpanCount()
}

// Links returns the trace links associated with this request.
func (req *tracesRequest) Links() []trace.Link {
	return req.links
}

func (req *tracesRequest) size(sizer sizer.TracesSizer) int {
	if req.cachedSize == -1 {
		req.cachedSize = sizer.TracesSize(req.td)
	}
	return req.cachedSize
}

func (req *tracesRequest) setCachedSize(size int) {
	req.cachedSize = size
}

type tracesExporter struct {
	*internal.BaseExporter
	consumer.Traces
}

// NewTraces creates an exporter.Traces that records observability metrics and wraps every request with a Span.
func NewTraces(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
	pusher consumer.ConsumeTracesFunc,
	options ...Option,
) (exporter.Traces, error) {
	if cfg == nil {
		return nil, errNilConfig
	}
	if pusher == nil {
		return nil, errNilPushTraces
	}
	return NewTracesRequest(ctx, set, requestFromTraces(), requestConsumeFromTraces(pusher),
		append([]Option{internal.WithQueueBatchSettings(NewTracesQueueBatchSettings())}, options...)...)
}

// requestConsumeFromTraces returns a RequestConsumeFunc that consumes ptrace.Traces.
func requestConsumeFromTraces(pusher consumer.ConsumeTracesFunc) RequestConsumeFunc {
	return func(ctx context.Context, request Request) error {
		return pusher.ConsumeTraces(ctx, request.(*tracesRequest).td)
	}
}

// requestFromTraces returns a RequestConverterFunc that converts ptrace.Traces into a Request.
func requestFromTraces() RequestConverterFunc[ptrace.Traces] {
	return func(ctx context.Context, traces ptrace.Traces) (Request, error) {
		links := queuebatch.LinksFromContext(ctx)
		return newTracesRequest(traces, links), nil
	}
}

// NewTracesRequest creates a new traces exporter based on a custom TracesConverter and Sender.
// Experimental: This API is at the early stage of development and may change without backward compatibility
// until https://github.com/open-telemetry/opentelemetry-collector/issues/8122 is resolved.
func NewTracesRequest(
	_ context.Context,
	set exporter.Settings,
	converter RequestConverterFunc[ptrace.Traces],
	pusher RequestConsumeFunc,
	options ...Option,
) (exporter.Traces, error) {
	if set.Logger == nil {
		return nil, errNilLogger
	}

	if converter == nil {
		return nil, errNilTracesConverter
	}

	if pusher == nil {
		return nil, errNilConsumeRequest
	}

	be, err := internal.NewBaseExporter(set, pipeline.SignalTraces, pusher, options...)
	if err != nil {
		return nil, err
	}

	tc, err := consumer.NewTraces(newConsumeTraces(converter, be, set.Logger), be.ConsumerOptions...)
	if err != nil {
		return nil, err
	}

	return &tracesExporter{BaseExporter: be, Traces: tc}, nil
}

func newConsumeTraces(converter RequestConverterFunc[ptrace.Traces], be *internal.BaseExporter, logger *zap.Logger) consumer.ConsumeTracesFunc {
	return func(ctx context.Context, td ptrace.Traces) error {
		req, err := converter(ctx, td)
		if err != nil {
			logger.Error("Failed to convert traces. Dropping data.",
				zap.Int("dropped_spans", td.SpanCount()),
				zap.Error(err))
			return consumererror.NewPermanent(err)
		}
		return be.Send(ctx, req)
	}
}
