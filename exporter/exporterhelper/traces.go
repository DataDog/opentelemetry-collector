// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exporterhelper // import "go.opentelemetry.io/collector/exporter/exporterhelper"

import (
	"context"
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

// TraceStateSerializable represents a serializable version of TraceState
type TraceStateSerializable struct {
	Members []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"members"`
}

// traceStateToSerializable converts a TraceState to a serializable format
func traceStateToSerializable(ts trace.TraceState) TraceStateSerializable {
	result := TraceStateSerializable{
		Members: make([]struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}, 0, ts.Len()),
	}

	ts.Walk(func(key, value string) bool {
		result.Members = append(result.Members, struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}{
			Key:   key,
			Value: value,
		})
		return true
	})

	return result
}

// serializableToTraceState converts a serializable format back to TraceState
func serializableToTraceState(ts TraceStateSerializable) (trace.TraceState, error) {
	var result trace.TraceState
	var err error

	for _, m := range ts.Members {
		result, err = result.Insert(m.Key, m.Value)
		if err != nil {
			return trace.TraceState{}, err
		}
	}

	return result, nil
}

// Update SerializableLink to use the new TraceState serialization
type SerializableLink struct {
	TraceID    [16]byte               `json:"trace_id"`
	SpanID     [8]byte                `json:"span_id"`
	TraceFlags byte                   `json:"trace_flags"`
	TraceState TraceStateSerializable `json:"trace_state"`
}

func linkToSerializable(l trace.Link) SerializableLink {
	return SerializableLink{
		TraceID:    l.SpanContext.TraceID(),
		SpanID:     l.SpanContext.SpanID(),
		TraceFlags: byte(l.SpanContext.TraceFlags()),
		TraceState: traceStateToSerializable(l.SpanContext.TraceState()),
	}
}

func serializableToLink(sl SerializableLink) trace.Link {
	ts, err := serializableToTraceState(sl.TraceState)
	if err != nil {
		// If there's an error parsing the trace state, use an empty one
		ts = trace.TraceState{}
	}

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    sl.TraceID,
		SpanID:     sl.SpanID,
		TraceFlags: trace.TraceFlags(sl.TraceFlags),
		TraceState: ts,
	})
	return trace.Link{
		SpanContext: sc,
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

type tracesWithLinks struct {
	Traces []byte             `json:"traces"`
	Links  []SerializableLink `json:"links"`
}

func (tracesEncoding) Unmarshal(bytes []byte) (Request, error) {
	var twl tracesWithLinks
	if err := json.Unmarshal(bytes, &twl); err != nil {
		return nil, err
	}
	traces, err := tracesUnmarshaler.UnmarshalTraces(twl.Traces)
	if err != nil {
		return nil, err
	}
	links := make([]trace.Link, len(twl.Links))
	for i, sl := range twl.Links {
		links[i] = serializableToLink(sl)
	}
	return newTracesRequest(traces, links), nil
}

func (tracesEncoding) Marshal(req Request) ([]byte, error) {
	tr := req.(*tracesRequest)
	tracesBytes, err := tracesMarshaler.MarshalTraces(tr.td)
	if err != nil {
		return nil, err
	}
	serializableLinks := make([]SerializableLink, len(tr.links))
	for i, l := range tr.links {
		serializableLinks[i] = linkToSerializable(l)
	}
	twl := tracesWithLinks{
		Traces: tracesBytes,
		Links:  serializableLinks,
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
