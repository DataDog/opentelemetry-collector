// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exporterhelper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/sizer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/testdata"
)

func TestMergeTraces(t *testing.T) {
	tr1 := newTracesRequest(testdata.GenerateTraces(2), nil)
	tr2 := newTracesRequest(testdata.GenerateTraces(3), nil)
	res, err := tr1.MergeSplit(context.Background(), 0, RequestSizerTypeItems, tr2)
	require.NoError(t, err)
	assert.Equal(t, 5, res[0].ItemsCount())
}

func TestMergeSplitTraces(t *testing.T) {
	tests := []struct {
		name     string
		szt      RequestSizerType
		maxSize  int
		tr1      Request
		tr2      Request
		expected []Request
	}{
		{
			name:     "both_requests_empty",
			szt:      RequestSizerTypeItems,
			maxSize:  10,
			tr1:      newTracesRequest(ptrace.NewTraces(), nil),
			tr2:      newTracesRequest(ptrace.NewTraces(), nil),
			expected: []Request{newTracesRequest(ptrace.NewTraces(), nil)},
		},
		{
			name:     "first_request_empty",
			szt:      RequestSizerTypeItems,
			maxSize:  10,
			tr1:      newTracesRequest(ptrace.NewTraces(), nil),
			tr2:      newTracesRequest(testdata.GenerateTraces(5), nil),
			expected: []Request{newTracesRequest(testdata.GenerateTraces(5), nil)},
		},
		{
			name:     "second_request_empty",
			szt:      RequestSizerTypeItems,
			maxSize:  10,
			tr1:      newTracesRequest(testdata.GenerateTraces(5), nil),
			tr2:      newTracesRequest(ptrace.NewTraces(), nil),
			expected: []Request{newTracesRequest(testdata.GenerateTraces(5), nil)},
		},
		{
			name:     "first_empty_second_nil",
			szt:      RequestSizerTypeItems,
			maxSize:  10,
			tr1:      newTracesRequest(ptrace.NewTraces(), nil),
			tr2:      nil,
			expected: []Request{newTracesRequest(ptrace.NewTraces(), nil)},
		},
		{
			name:    "merge_only",
			szt:     RequestSizerTypeItems,
			maxSize: 10,
			tr1:     newTracesRequest(testdata.GenerateTraces(5), nil),
			tr2:     newTracesRequest(testdata.GenerateTraces(5), nil),
			expected: []Request{newTracesRequest(func() ptrace.Traces {
				td := testdata.GenerateTraces(5)
				testdata.GenerateTraces(5).ResourceSpans().MoveAndAppendTo(td.ResourceSpans())
				return td
			}(), nil)},
		},
		{
			name:    "split_only",
			szt:     RequestSizerTypeItems,
			maxSize: 4,
			tr1:     newTracesRequest(ptrace.NewTraces(), nil),
			tr2:     newTracesRequest(testdata.GenerateTraces(10), nil),
			expected: []Request{
				newTracesRequest(testdata.GenerateTraces(4), nil),
				newTracesRequest(testdata.GenerateTraces(4), nil),
				newTracesRequest(testdata.GenerateTraces(2), nil),
			},
		},
		{
			name:    "split_and_merge",
			szt:     RequestSizerTypeItems,
			maxSize: 10,
			tr1:     newTracesRequest(testdata.GenerateTraces(4), nil),
			tr2:     newTracesRequest(testdata.GenerateTraces(20), nil),
			expected: []Request{
				newTracesRequest(func() ptrace.Traces {
					td := testdata.GenerateTraces(4)
					testdata.GenerateTraces(6).ResourceSpans().MoveAndAppendTo(td.ResourceSpans())
					return td
				}(), nil),
				newTracesRequest(testdata.GenerateTraces(10), nil),
				newTracesRequest(testdata.GenerateTraces(4), nil),
			},
		},
		{
			name:    "scope_spans_split",
			szt:     RequestSizerTypeItems,
			maxSize: 10,
			tr1: newTracesRequest(func() ptrace.Traces {
				td := testdata.GenerateTraces(10)
				extraScopeTraces := testdata.GenerateTraces(5)
				extraScopeTraces.ResourceSpans().At(0).ScopeSpans().At(0).Scope().SetName("extra scope")
				extraScopeTraces.ResourceSpans().MoveAndAppendTo(td.ResourceSpans())
				return td
			}(), nil),
			tr2: nil,
			expected: []Request{
				newTracesRequest(testdata.GenerateTraces(10), nil),
				newTracesRequest(func() ptrace.Traces {
					td := testdata.GenerateTraces(5)
					td.ResourceSpans().At(0).ScopeSpans().At(0).Scope().SetName("extra scope")
					return td
				}(), nil),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := tt.tr1.MergeSplit(context.Background(), tt.maxSize, tt.szt, tt.tr2)
			require.NoError(t, err)
			assert.Len(t, res, len(tt.expected))
			for i := range res {
				assert.Equal(t, tt.expected[i].(*tracesRequest).td, res[i].(*tracesRequest).td)
			}
		})
	}
}

func TestMergeSplitTracesBasedOnByteSize(t *testing.T) {
	tests := []struct {
		name     string
		szt      RequestSizerType
		maxSize  int
		lr1      Request
		lr2      Request
		expected []Request
	}{
		{
			name:     "both_requests_empty",
			szt:      RequestSizerTypeBytes,
			maxSize:  tracesMarshaler.TracesSize(testdata.GenerateTraces(10)),
			lr1:      newTracesRequest(ptrace.NewTraces(), nil),
			lr2:      newTracesRequest(ptrace.NewTraces(), nil),
			expected: []Request{newTracesRequest(ptrace.NewTraces(), nil)},
		},
		{
			name:     "first_request_empty",
			szt:      RequestSizerTypeBytes,
			maxSize:  tracesMarshaler.TracesSize(testdata.GenerateTraces(10)),
			lr1:      newTracesRequest(ptrace.NewTraces(), nil),
			lr2:      newTracesRequest(testdata.GenerateTraces(5), nil),
			expected: []Request{newTracesRequest(testdata.GenerateTraces(5), nil)},
		},
		{
			name:     "first_empty_second_nil",
			szt:      RequestSizerTypeBytes,
			maxSize:  tracesMarshaler.TracesSize(testdata.GenerateTraces(10)),
			lr1:      newTracesRequest(ptrace.NewTraces(), nil),
			lr2:      nil,
			expected: []Request{newTracesRequest(ptrace.NewTraces(), nil)},
		},
		{
			name:    "merge_only",
			szt:     RequestSizerTypeBytes,
			maxSize: tracesMarshaler.TracesSize(testdata.GenerateTraces(10)),
			lr1:     newTracesRequest(testdata.GenerateTraces(1), nil),
			lr2:     newTracesRequest(testdata.GenerateTraces(6), nil),
			expected: []Request{newTracesRequest(func() ptrace.Traces {
				traces := testdata.GenerateTraces(1)
				testdata.GenerateTraces(6).ResourceSpans().MoveAndAppendTo(traces.ResourceSpans())
				return traces
			}(), nil)},
		},
		{
			name:    "split_only",
			szt:     RequestSizerTypeBytes,
			maxSize: tracesMarshaler.TracesSize(testdata.GenerateTraces(4)),
			lr1:     newTracesRequest(ptrace.NewTraces(), nil),
			lr2:     newTracesRequest(testdata.GenerateTraces(10), nil),
			expected: []Request{
				newTracesRequest(testdata.GenerateTraces(4), nil),
				newTracesRequest(testdata.GenerateTraces(4), nil),
				newTracesRequest(testdata.GenerateTraces(2), nil),
			},
		},
		{
			name:    "merge_and_split",
			szt:     RequestSizerTypeBytes,
			maxSize: tracesMarshaler.TracesSize(testdata.GenerateTraces(10))/2 + tracesMarshaler.TracesSize(testdata.GenerateTraces(11))/2,
			lr1:     newTracesRequest(testdata.GenerateTraces(8), nil),
			lr2:     newTracesRequest(testdata.GenerateTraces(20), nil),
			expected: []Request{
				newTracesRequest(func() ptrace.Traces {
					traces := testdata.GenerateTraces(8)
					testdata.GenerateTraces(2).ResourceSpans().MoveAndAppendTo(traces.ResourceSpans())
					return traces
				}(), nil),
				newTracesRequest(testdata.GenerateTraces(10), nil),
				newTracesRequest(testdata.GenerateTraces(8), nil),
			},
		},
		{
			name:    "scope_spans_split",
			szt:     RequestSizerTypeBytes,
			maxSize: tracesMarshaler.TracesSize(testdata.GenerateTraces(4)),
			lr1: newTracesRequest(func() ptrace.Traces {
				ld := testdata.GenerateTraces(4)
				ld.ResourceSpans().At(0).ScopeSpans().AppendEmpty().Spans().AppendEmpty().Attributes().PutStr("attr", "attrvalue")
				return ld
			}(), nil),
			lr2: newTracesRequest(testdata.GenerateTraces(2), nil),
			expected: []Request{
				newTracesRequest(testdata.GenerateTraces(4), nil),
				newTracesRequest(func() ptrace.Traces {
					ld := testdata.GenerateTraces(0)
					ld.ResourceSpans().At(0).ScopeSpans().At(0).Spans().AppendEmpty().Attributes().PutStr("attr", "attrvalue")
					testdata.GenerateTraces(2).ResourceSpans().MoveAndAppendTo(ld.ResourceSpans())
					return ld
				}(), nil),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := tt.lr1.MergeSplit(context.Background(), tt.maxSize, tt.szt, tt.lr2)
			require.NoError(t, err)
			assert.Len(t, res, len(tt.expected))
			for i := range res {
				assert.Equal(t, tt.expected[i].(*tracesRequest).td, res[i].(*tracesRequest).td)
			}
		})
	}
}

func TestMergeSplitTracesInputNotModifiedIfErrorReturned(t *testing.T) {
	r1 := newTracesRequest(testdata.GenerateTraces(18), nil)
	r2 := newLogsRequest(testdata.GenerateLogs(3))
	_, err := r1.MergeSplit(context.Background(), 10, RequestSizerTypeItems, r2)
	require.Error(t, err)
	assert.Equal(t, 18, r1.ItemsCount())
}

func TestExtractTraces(t *testing.T) {
	for i := 0; i < 10; i++ {
		td := testdata.GenerateTraces(10)
		extractedTraces, removedSize := extractTraces(td, i, &sizer.TracesCountSizer{})
		assert.Equal(t, i, extractedTraces.SpanCount())
		assert.Equal(t, 10-i, td.SpanCount())
		assert.Equal(t, i, removedSize)
	}
}

func TestMergeSplitManySmallTraces(t *testing.T) {
	merged := []Request{newTracesRequest(testdata.GenerateTraces(1), nil)}
	for j := 0; j < 1000; j++ {
		lr2 := newTracesRequest(testdata.GenerateTraces(10), nil)
		res, _ := merged[len(merged)-1].MergeSplit(context.Background(), 10000, RequestSizerTypeItems, lr2)
		merged = append(merged[0:len(merged)-1], res...)
	}
	assert.Len(t, merged, 2)
}

func TestTracesMergeSplitExactBytes(t *testing.T) {
	pb := ptrace.ProtoMarshaler{}
	// Set max size off by 1, so forces every log to be it's own batch.
	lr := newTracesRequest(testdata.GenerateTraces(4), nil)
	merged, err := lr.MergeSplit(context.Background(), pb.TracesSize(testdata.GenerateTraces(2))-1, RequestSizerTypeBytes, nil)
	require.NoError(t, err)
	assert.Len(t, merged, 4)
}

func TestTracesMergeSplitExactItems(t *testing.T) {
	// Set max size off by 1, so forces every log to be it's own batch.
	lr := newTracesRequest(testdata.GenerateTraces(4), nil)
	merged, err := lr.MergeSplit(context.Background(), 1, RequestSizerTypeItems, nil)
	require.NoError(t, err)
	assert.Len(t, merged, 4)
}

func TestTracesMergeSplitUnknownSizerType(t *testing.T) {
	req := newTracesRequest(ptrace.NewTraces(), nil)
	// Call MergeSplit with invalid sizer
	_, err := req.MergeSplit(context.Background(), 0, RequestSizerType{}, nil)
	require.EqualError(t, err, "unknown sizer type")
}

func BenchmarkSplittingBasedOnItemCountManySmallTraces(b *testing.B) {
	// All requests merge into a single batch.
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		merged := []Request{newTracesRequest(testdata.GenerateTraces(10), nil)}
		for j := 0; j < 1000; j++ {
			lr2 := newTracesRequest(testdata.GenerateTraces(10), nil)
			res, _ := merged[len(merged)-1].MergeSplit(context.Background(), 10010, RequestSizerTypeItems, lr2)
			merged = append(merged[0:len(merged)-1], res...)
		}
		assert.Len(b, merged, 1)
	}
}

func BenchmarkSplittingBasedOnItemCountManyTracesSlightlyAboveLimit(b *testing.B) {
	// Every incoming request results in a split.
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		merged := []Request{newTracesRequest(testdata.GenerateTraces(0), nil)}
		for j := 0; j < 10; j++ {
			lr2 := newTracesRequest(testdata.GenerateTraces(10001), nil)
			res, _ := merged[len(merged)-1].MergeSplit(context.Background(), 10000, RequestSizerTypeItems, lr2)
			merged = append(merged[0:len(merged)-1], res...)
		}
		assert.Len(b, merged, 11)
	}
}

func BenchmarkSplittingBasedOnItemCountHugeTraces(b *testing.B) {
	// One request splits into many batches.
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		merged := []Request{newTracesRequest(testdata.GenerateTraces(0), nil)}
		lr2 := newTracesRequest(testdata.GenerateTraces(100000), nil)
		res, _ := merged[len(merged)-1].MergeSplit(context.Background(), 10000, RequestSizerTypeItems, lr2)
		merged = append(merged[0:len(merged)-1], res...)
		assert.Len(b, merged, 10)
	}
}

func TestMergeSplitTracesWithLinks(t *testing.T) {
	// Create test links
	link1 := trace.Link{
		SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			SpanID:     [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
			TraceFlags: trace.FlagsSampled,
		}),
		Attributes: []attribute.KeyValue{
			attribute.String("key1", "value1"),
		},
	}
	link2 := trace.Link{
		SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    [16]byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
			SpanID:     [8]byte{8, 7, 6, 5, 4, 3, 2, 1},
			TraceFlags: trace.FlagsSampled,
		}),
		Attributes: []attribute.KeyValue{
			attribute.String("key2", "value2"),
		},
	}

	tests := []struct {
		name     string
		szt      RequestSizerType
		maxSize  int
		tr1      Request
		tr2      Request
		expected []Request
	}{
		{
			name:    "merge_with_links",
			szt:     RequestSizerTypeItems,
			maxSize: 10,
			tr1:     newTracesRequest(testdata.GenerateTraces(2), []trace.Link{link1}),
			tr2:     newTracesRequest(testdata.GenerateTraces(3), []trace.Link{link2}),
			expected: []Request{newTracesRequest(func() ptrace.Traces {
				td := testdata.GenerateTraces(2)
				testdata.GenerateTraces(3).ResourceSpans().MoveAndAppendTo(td.ResourceSpans())
				return td
			}(), []trace.Link{link1, link2})},
		},
		{
			name:    "split_with_links",
			szt:     RequestSizerTypeItems,
			maxSize: 2,
			tr1:     newTracesRequest(testdata.GenerateTraces(5), []trace.Link{link1, link2}),
			tr2:     nil,
			expected: []Request{
				newTracesRequest(testdata.GenerateTraces(2), []trace.Link{link1, link2}),
				newTracesRequest(testdata.GenerateTraces(2), []trace.Link{link1, link2}),
				newTracesRequest(testdata.GenerateTraces(1), []trace.Link{link1, link2}),
			},
		},
		{
			name:    "merge_and_split_with_links",
			szt:     RequestSizerTypeItems,
			maxSize: 3,
			tr1:     newTracesRequest(testdata.GenerateTraces(2), []trace.Link{link1}),
			tr2:     newTracesRequest(testdata.GenerateTraces(4), []trace.Link{link2}),
			expected: []Request{
				newTracesRequest(testdata.GenerateTraces(3), []trace.Link{link1, link2}),
				newTracesRequest(testdata.GenerateTraces(3), []trace.Link{link1, link2}),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := tt.tr1.MergeSplit(context.Background(), tt.maxSize, tt.szt, tt.tr2)
			require.NoError(t, err)
			assert.Len(t, res, len(tt.expected))
			for i := range res {
				tracesReq := res[i].(*tracesRequest)
				expectedReq := tt.expected[i].(*tracesRequest)
				assert.Len(t, tracesReq.links, len(expectedReq.links))
				assert.Equal(t, expectedReq.links, tracesReq.links)
			}
		})
	}
}

func TestTracesRequestLinksMarshaling(t *testing.T) {
	traceState, err := trace.TraceState{}.Insert("key1", "value1")
	require.NoError(t, err)

	link := trace.Link{
		SpanContext: trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			SpanID:     [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
			TraceFlags: trace.FlagsSampled,
			TraceState: traceState,
		}),
	}

	originalReq := newTracesRequest(testdata.GenerateTraces(2), []trace.Link{link})

	// Marshal the request
	encoding := tracesEncoding{}
	bytes, err := encoding.Marshal(originalReq)
	require.NoError(t, err)

	// Unmarshal back to a new request
	newReq, err := encoding.Unmarshal(bytes)
	require.NoError(t, err)

	// Verify the unmarshaled request
	tracesReq := newReq.(*tracesRequest)
	require.Len(t, tracesReq.links, 1)

	// Verify link properties
	unmarshaledLink := tracesReq.links[0]
	assert.Equal(t, link.SpanContext.TraceID(), unmarshaledLink.SpanContext.TraceID())
	assert.Equal(t, link.SpanContext.SpanID(), unmarshaledLink.SpanContext.SpanID())
	assert.Equal(t, link.SpanContext.TraceFlags(), unmarshaledLink.SpanContext.TraceFlags())
	assert.Equal(t, link.SpanContext.TraceState(), unmarshaledLink.SpanContext.TraceState())

	// Verify trace data
	assert.Equal(t, originalReq.(*tracesRequest).td, tracesReq.td)
}
