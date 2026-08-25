// Copyright (c) 2023 Cloudnatively Services Pvt Ltd
//
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const traceVisibilityTimeout = 5 * time.Minute

type testTraceListRecord struct {
	ServiceName string `json:"service.name"`
	SpanName    string `json:"span_name"`
	TraceID     string `json:"span_trace_id"`
	SpanID      string `json:"span_span_id"`
}

type testTraceListResponse struct {
	Count   uint64                `json:"count"`
	Offset  int                   `json:"offset"`
	Limit   int                   `json:"limit"`
	Records []testTraceListRecord `json:"records"`
}

type testTraceDetailRecord struct {
	ServiceName  string `json:"service.name"`
	SpanName     string `json:"span_name"`
	TraceID      string `json:"span_trace_id"`
	SpanID       string `json:"span_span_id"`
	ParentSpanID string `json:"span_parent_span_id"`
	Level        int    `json:"level"`
}

type testTraceDetailResponse struct {
	StartTime string                  `json:"startTime"`
	EndTime   string                  `json:"endTime"`
	Records   []testTraceDetailRecord `json:"records"`
}

func createTraceDataset(t *testing.T, dataset string) {
	t.Helper()
	result, err := NewGlob.PBClient.Run(context.Background(), "dataset", "add", dataset, "--type", "traces")
	require.NoErrorf(t, err, "pb traces dataset add failed (exit=%d, stdout=%q, stderr=%q)", result.ExitCode, result.Stdout, result.Stderr)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.PBClient, dataset)
	})
}

func ingestTestTraces(t *testing.T, dataset, traceID string) {
	t.Helper()
	client := NewGlob.QueryClient
	if NewGlob.IngestorUrl.String() != "" {
		client = NewGlob.IngestorClient
	}

	rootSpanID := "0123456789abcdef"
	childSpanID := "fedcba9876543210"
	start := time.Now().UTC().Add(-2 * time.Second)
	rootEnd := start.Add(1500 * time.Millisecond)
	childStart := start.Add(250 * time.Millisecond)
	childEnd := start.Add(time.Second)
	payload := map[string]any{
		"resourceSpans": []any{
			map[string]any{
				"resource": map[string]any{
					"attributes": []any{
						map[string]any{
							"key":   "service.name",
							"value": map[string]string{"stringValue": "quest-trace-service"},
						},
					},
				},
				"scopeSpans": []any{
					map[string]any{
						"scope": map[string]string{"name": "quest"},
						"spans": []any{
							map[string]any{
								"traceId":           traceID,
								"spanId":            rootSpanID,
								"parentSpanId":      "",
								"name":              "quest-root-span",
								"kind":              2,
								"startTimeUnixNano": fmt.Sprint(start.UnixNano()),
								"endTimeUnixNano":   fmt.Sprint(rootEnd.UnixNano()),
								"status":            map[string]int{"code": 1},
							},
							map[string]any{
								"traceId":           traceID,
								"spanId":            childSpanID,
								"parentSpanId":      rootSpanID,
								"name":              "quest-child-span",
								"kind":              3,
								"startTimeUnixNano": fmt.Sprint(childStart.UnixNano()),
								"endTimeUnixNano":   fmt.Sprint(childEnd.UnixNano()),
								"status":            map[string]int{"code": 1},
							},
						},
					},
				},
			},
		},
	}
	encoded, err := json.Marshal(payload)
	require.NoError(t, err)
	req, err := client.NewRequestAtPath("POST", "/v1/traces", bytes.NewReader(encoded))
	require.NoError(t, err)
	req.Header.Set("X-P-Stream", dataset)
	req.Header.Set("X-P-Log-Source", "otel-traces")
	response, err := client.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "OTLP/JSON trace ingestion failed: %s", readAsString(response.Body))
}

func traceAPIRequest(path string, payload any) (int, []byte, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, err
	}
	req, err := NewGlob.QueryClient.NewRequestAtPath("POST", path, bytes.NewReader(encoded))
	if err != nil {
		return 0, nil, err
	}
	response, err := NewGlob.QueryClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	body := []byte(readAsString(response.Body))
	return response.StatusCode, body, nil
}

func waitForTestTrace(t *testing.T, payload map[string]any) testTraceListResponse {
	t.Helper()
	deadline := time.Now().Add(traceVisibilityTimeout)
	var lastStatus int
	var lastBody []byte
	var lastErr error
	for time.Now().Before(deadline) {
		lastStatus, lastBody, lastErr = traceAPIRequest("api/prism/v1/services/traces/list", payload)
		if lastErr == nil && lastStatus == 200 {
			var result testTraceListResponse
			if err := json.Unmarshal(lastBody, &result); err == nil && len(result.Records) > 0 {
				return result
			}
		}
		time.Sleep(5 * time.Second)
	}
	require.NoErrorf(t, lastErr, "trace did not become queryable within %s", traceVisibilityTimeout)
	require.Failf(t, "trace did not become queryable", "last status=%d body=%s", lastStatus, lastBody)
	return testTraceListResponse{}
}

func TestSmokeTraceAPIs(t *testing.T) {
	// Verifies real OTLP trace ingestion, trace listing, detail, and field queries.
	t.Parallel()
	dataset := NewGlob.Stream + "traces"
	traceID := fmt.Sprintf("%032x", time.Now().UnixNano())
	startTime := time.Now().UTC().Add(-5 * time.Minute).Format(time.RFC3339Nano)
	createTraceDataset(t, dataset)
	ingestTestTraces(t, dataset, traceID)
	endTime := time.Now().UTC().Add(5 * time.Minute).Format(time.RFC3339Nano)

	listPayload := map[string]any{
		"dataset":   dataset,
		"startTime": startTime,
		"endTime":   endTime,
		"sortBy":    "mostRecent",
		"options":   "spans",
		"limit":     10,
		"offset":    0,
	}
	listed := waitForTestTrace(t, listPayload)
	require.GreaterOrEqual(t, listed.Count, uint64(1))
	require.Equal(t, 0, listed.Offset)
	require.Equal(t, 10, listed.Limit)
	trace := listed.Records[0]
	require.Equal(t, traceID, trace.TraceID)
	require.NotEmpty(t, trace.ServiceName)
	require.NotEmpty(t, trace.SpanName)
	require.NotEmpty(t, trace.TraceID)
	require.NotEmpty(t, trace.SpanID)

	t.Run("GetTraceDetail", func(t *testing.T) {
		status, body, err := traceAPIRequest("api/prism/v1/services/traces/detail", map[string]any{
			"dataset":   dataset,
			"traceId":   trace.TraceID,
			"startTime": startTime,
			"endTime":   endTime,
		})
		require.NoError(t, err)
		require.Equalf(t, 200, status, "Server returned body: %s", body)
		var detail testTraceDetailResponse
		require.NoError(t, json.Unmarshal(body, &detail))
		require.NotEmpty(t, detail.StartTime)
		require.NotEmpty(t, detail.EndTime)
		require.NotEmpty(t, detail.Records)
		for _, span := range detail.Records {
			require.Equal(t, trace.TraceID, span.TraceID)
			require.NotEmpty(t, span.SpanID)
			require.NotEmpty(t, span.SpanName)
		}
	})

	t.Run("QueryTraceFields", func(t *testing.T) {
		queryPayload := map[string]string{
			"query":     fmt.Sprintf(`SELECT span_trace_id, span_name FROM "%s" WHERE span_trace_id = '%s' LIMIT 1`, dataset, trace.TraceID),
			"startTime": startTime,
			"endTime":   endTime,
		}
		status, body, err := traceAPIRequest("api/v1/query?fields=true", queryPayload)
		require.NoError(t, err)
		require.Equalf(t, 200, status, "Server returned body: %s", body)
		var result queryWithFieldsResponse
		require.NoError(t, json.Unmarshal(body, &result))
		require.ElementsMatch(t, []string{"span_name", "span_trace_id"}, result.Fields)
		require.NotEmpty(t, result.Records)
		require.Contains(t, string(result.Records[0]), trace.TraceID)
	})
}
