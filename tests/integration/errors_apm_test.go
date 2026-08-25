// Copyright (c) 2023 Cloudnatively Services Pvt Ltd
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

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

const (
	observabilityFrontendService = "quest-frontend"
	observabilityBackendService  = "quest-backend"
	observabilityErrorType       = "QuestCheckoutError"
	observabilityErrorMessage    = "quest checkout failed"
	observabilityBackendSpan     = "POST /checkout"
)

func observabilityRequest(path string, payload any) (int, []byte, error) {
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
	return response.StatusCode, []byte(readAsString(response.Body)), nil
}

func createObservabilityDataset(t *testing.T, dataset string) {
	t.Helper()
	result, err := NewGlob.PBClient.Run(context.Background(), "dataset", "add", dataset, "--type", "traces")
	require.NoErrorf(t, err, "pb traces dataset add failed (exit=%d, stdout=%q, stderr=%q)", result.ExitCode, result.Stdout, result.Stderr)
	t.Cleanup(func() { DeleteStream(t, NewGlob.PBClient, dataset) })
}

func otlpStringAttribute(key, value string) map[string]any {
	return map[string]any{"key": key, "value": map[string]string{"stringValue": value}}
}

func otlpIntAttribute(key string, value int) map[string]any {
	return map[string]any{"key": key, "value": map[string]string{"intValue": fmt.Sprint(value)}}
}

func ingestObservabilityFixture(t *testing.T, dataset, traceID string) {
	t.Helper()
	client := NewGlob.QueryClient
	if NewGlob.IngestorUrl.String() != "" {
		client = NewGlob.IngestorClient
	}

	start := time.Now().UTC().Add(-2 * time.Second)
	rootSpanID := "1111111111111111"
	childSpanID := "2222222222222222"
	rootSpan := map[string]any{
		"traceId": traceID, "spanId": rootSpanID, "name": "GET /cart", "kind": 2,
		"startTimeUnixNano": fmt.Sprint(start.UnixNano()),
		"endTimeUnixNano":   fmt.Sprint(start.Add(1500 * time.Millisecond).UnixNano()),
		"status":            map[string]int{"code": 1},
		"attributes":        []any{otlpStringAttribute("http.route", "/cart"), otlpIntAttribute("http.response.status_code", 200)},
	}
	childSpan := map[string]any{
		"traceId": traceID, "spanId": childSpanID, "parentSpanId": rootSpanID, "name": observabilityBackendSpan, "kind": 3,
		"startTimeUnixNano": fmt.Sprint(start.Add(200 * time.Millisecond).UnixNano()),
		"endTimeUnixNano":   fmt.Sprint(start.Add(time.Second).UnixNano()),
		"status":            map[string]any{"code": 2, "message": observabilityErrorMessage},
		"attributes":        []any{otlpStringAttribute("http.route", "/checkout"), otlpIntAttribute("http.response.status_code", 500)},
		"events": []any{map[string]any{
			"timeUnixNano": fmt.Sprint(start.Add(900 * time.Millisecond).UnixNano()),
			"name":         "exception",
			"attributes": []any{
				otlpStringAttribute("exception.type", observabilityErrorType),
				otlpStringAttribute("exception.message", observabilityErrorMessage),
				otlpStringAttribute("exception.stacktrace", "quest checkout stack"),
			},
		}},
	}
	payload := map[string]any{"resourceSpans": []any{
		map[string]any{
			"resource":   map[string]any{"attributes": []any{otlpStringAttribute("service.name", observabilityFrontendService)}},
			"scopeSpans": []any{map[string]any{"scope": map[string]string{"name": "quest"}, "spans": []any{rootSpan}}},
		},
		map[string]any{
			"resource":   map[string]any{"attributes": []any{otlpStringAttribute("service.name", observabilityBackendService)}},
			"scopeSpans": []any{map[string]any{"scope": map[string]string{"name": "quest"}, "spans": []any{childSpan}}},
		},
	}}
	encoded, err := json.Marshal(payload)
	require.NoError(t, err)
	req, err := client.NewRequestAtPath("POST", "/v1/traces", bytes.NewReader(encoded))
	require.NoError(t, err)
	req.Header.Set("X-P-Stream", dataset)
	req.Header.Set("X-P-Log-Source", "otel-traces")
	response, err := client.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "OTLP observability fixture failed: %s", readAsString(response.Body))
}

func observabilityWindow() (string, string) {
	return time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano), time.Now().UTC().Add(5 * time.Minute).Format(time.RFC3339Nano)
}

type errorsListResponse struct {
	Total   int `json:"total"`
	Records []struct {
		ExceptionType    string `json:"exceptionType"`
		ExceptionMessage string `json:"exceptionMessage"`
		ServiceName      string `json:"serviceName"`
		Occurrences      int    `json:"occurrences"`
	} `json:"records"`
}

func waitForObservabilityError(t *testing.T, payload map[string]any) errorsListResponse {
	t.Helper()
	deadline := time.Now().Add(traceVisibilityTimeout)
	var lastStatus int
	var lastBody []byte
	var lastErr error
	for time.Now().Before(deadline) {
		lastStatus, lastBody, lastErr = observabilityRequest("api/prism/v1/errors/list", payload)
		if lastErr == nil && lastStatus == 200 {
			var result errorsListResponse
			if err := json.Unmarshal(lastBody, &result); err == nil && len(result.Records) > 0 {
				return result
			}
		}
		time.Sleep(5 * time.Second)
	}
	require.NoErrorf(t, lastErr, "error data did not become queryable within %s", traceVisibilityTimeout)
	require.Failf(t, "error data did not become queryable", "last status=%d body=%s", lastStatus, lastBody)
	return errorsListResponse{}
}

func TestEnterpriseErrorsAndAPM(t *testing.T) {
	// Verifies all Enterprise Errors and APM APIs.
	if NewGlob.Edition != "enterprise" {
		t.Skip("Errors and APM are only available in Enterprise")
	}
	t.Parallel()
	dataset := NewGlob.Stream + "observability"
	traceID := fmt.Sprintf("%032x", time.Now().UnixNano())
	createObservabilityDataset(t, dataset)
	ingestObservabilityFixture(t, dataset, traceID)
	startTime, endTime := observabilityWindow()

	listPayload := map[string]any{
		"dataset": dataset, "startTime": startTime, "endTime": endTime,
		"offset": 0, "limit": 10, "sortBy": "lastSeen", "sortOrder": "desc",
	}
	errors := waitForObservabilityError(t, listPayload)
	require.GreaterOrEqual(t, errors.Total, 1)
	errorRecord := errors.Records[0]
	require.Equal(t, observabilityErrorType, errorRecord.ExceptionType)
	require.Equal(t, observabilityErrorMessage, errorRecord.ExceptionMessage)
	require.Equal(t, observabilityBackendService, errorRecord.ServiceName)
	require.GreaterOrEqual(t, errorRecord.Occurrences, 1)

	errorGroupPayload := map[string]any{
		"dataset": dataset, "startTime": startTime, "endTime": endTime,
		"serviceName": errorRecord.ServiceName, "exceptionType": errorRecord.ExceptionType,
		"exceptionMessage": errorRecord.ExceptionMessage,
	}

	t.Run("ErrorsFrequency", func(t *testing.T) {
		t.Parallel()
		status, body, err := observabilityRequest("api/prism/v1/errors/frequency", errorGroupPayload)
		require.NoError(t, err)
		require.Equalf(t, 200, status, "errors frequency failed: %s", body)
		require.Contains(t, string(body), `"records"`)
	})

	t.Run("ErrorTraces", func(t *testing.T) {
		t.Parallel()
		payload := make(map[string]any, len(errorGroupPayload)+4)
		for key, value := range errorGroupPayload {
			payload[key] = value
		}
		payload["limit"], payload["offset"] = 10, 0
		payload["sortBy"], payload["sortOrder"] = "spanStartTime", "desc"
		status, body, err := observabilityRequest("api/prism/v1/errors/traces", payload)
		require.NoError(t, err)
		require.Equalf(t, 200, status, "error traces failed: %s", body)
		require.Contains(t, string(body), traceID)
	})

	servicePayload := map[string]any{"dataset": dataset, "startTime": startTime, "endTime": endTime}
	t.Run("ServicesList", func(t *testing.T) {
		t.Parallel()
		status, body, err := observabilityRequest("api/prism/v1/services/list", servicePayload)
		require.NoError(t, err)
		require.Equalf(t, 200, status, "services list failed: %s", body)
		require.Contains(t, string(body), observabilityFrontendService)
		require.Contains(t, string(body), observabilityBackendService)
	})

	serviceInsightPayload := map[string]any{
		"dataset": dataset, "serviceName": observabilityBackendService,
		"startTime": startTime, "endTime": endTime,
	}
	t.Run("ServiceInsights", func(t *testing.T) {
		t.Parallel()
		status, body, err := observabilityRequest("api/prism/v1/services/insights", serviceInsightPayload)
		require.NoError(t, err)
		require.Equalf(t, 200, status, "service insights failed: %s", body)
		require.Contains(t, string(body), `"serviceSummary"`)
		require.Contains(t, string(body), observabilityBackendSpan)
	})

	t.Run("ResourceInsights", func(t *testing.T) {
		t.Parallel()
		payload := map[string]any{
			"dataset": dataset, "serviceName": observabilityBackendService, "resource": observabilityBackendSpan,
			"startTime": startTime, "endTime": endTime,
		}
		status, body, err := observabilityRequest("api/prism/v1/services/resource/insights", payload)
		require.NoError(t, err)
		require.Equalf(t, 200, status, "resource insights failed: %s", body)
		require.Contains(t, string(body), `"overview"`)
	})

	t.Run("ServiceMap", func(t *testing.T) {
		t.Parallel()
		status, body, err := observabilityRequest("api/prism/v1/services/map", servicePayload)
		require.NoError(t, err)
		require.Equalf(t, 200, status, "service map failed: %s", body)
		require.Contains(t, string(body), observabilityFrontendService)
		require.Contains(t, string(body), observabilityBackendService)
		require.Contains(t, string(body), `"dependencies"`)
	})

	t.Run("ServiceMapInsights", func(t *testing.T) {
		t.Parallel()
		status, body, err := observabilityRequest("api/prism/v1/services/map/insights", serviceInsightPayload)
		require.NoError(t, err)
		require.Equalf(t, 200, status, "service map insights failed: %s", body)
		require.Contains(t, string(body), `"upstreamServices"`)
		require.Contains(t, string(body), `"downstreamServices"`)
		require.Contains(t, string(body), `"resources"`)
	})
}
