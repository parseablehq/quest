// Copyright (c) 2023 Cloudnatively Services Pvt Ltd
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const promQLMetricName = "gen"
const promQLMetricCount = 120
const promQLMetricRate = 2

type promQLCommandCase struct {
	name         string
	args         []string
	expectMetric bool
}

type promQLExpressionCase struct {
	name       string
	expression string
}

type promQLResponse struct {
	Status string          `json:"status"`
	Data   json.RawMessage `json:"data"`
}

type promQLData struct {
	ResultType string            `json:"resultType"`
	Result     []json.RawMessage `json:"result"`
}

func promQLExpressionCases() []promQLExpressionCase {
	return []promQLExpressionCase{
		{name: "metric selector", expression: promQLMetricName},
		{name: "sum aggregation", expression: "sum(" + promQLMetricName + ")"},
		{name: "average aggregation", expression: "avg(" + promQLMetricName + ")"},
		{name: "count aggregation", expression: "count(" + promQLMetricName + ")"},
		{name: "arithmetic", expression: promQLMetricName + " + 1"},
		{name: "comparison", expression: promQLMetricName + " > bool 0"},
		{name: "average over time", expression: "avg_over_time(" + promQLMetricName + "[5m])"},
		{name: "sum over time", expression: "sum_over_time(" + promQLMetricName + "[5m])"},
		{name: "count over time", expression: "count_over_time(" + promQLMetricName + "[5m])"},
		{name: "delta", expression: "delta(" + promQLMetricName + "[5m])"},
	}
}

func promQLCommandCases(dataset, instantTime string) []promQLCommandCase {
	return []promQLCommandCase{
		{name: "range query", args: []string{"promql", "run", promQLMetricName, "--dataset", dataset, "--from", "10m", "--step", "1m"}, expectMetric: true},
		{name: "instant query", args: []string{"promql", "run", promQLMetricName, "--dataset", dataset, "--instant", "--to", instantTime}, expectMetric: true},
		{name: "labels", args: []string{"promql", "labels", "--dataset", dataset}},
		{name: "label values", args: []string{"promql", "label-values", "__name__", "--dataset", dataset}, expectMetric: true},
		{name: "series", args: []string{"promql", "series", "--dataset", dataset, "--match", fmt.Sprintf(`{__name__=%q}`, promQLMetricName)}, expectMetric: true},
		{name: "cardinality label names", args: []string{"promql", "cardinality", "label-names", "--dataset", dataset}},
		{name: "cardinality label values", args: []string{"promql", "cardinality", "label-values", "--dataset", dataset, "--label", "__name__"}},
		{name: "cardinality active series", args: []string{"promql", "cardinality", "active-series", "--dataset", dataset}},
		{name: "active queries", args: []string{"promql", "active-queries"}},
		{name: "TSDB stats", args: []string{"promql", "tsdb", "--dataset", dataset}},
	}
}

func ingestPromQLMetric(t *testing.T, dataset string) time.Time {
	// Generates and ingests real OTLP metrics, like RunFlog does for SQL tests.
	t.Helper()
	targetURL := NewGlob.QueryUrl
	username := NewGlob.QueryUsername
	password := NewGlob.QueryPassword
	if NewGlob.IngestorUrl.String() != "" {
		targetURL = NewGlob.IngestorUrl
		username = NewGlob.IngestorUsername
		password = NewGlob.IngestorPassword
	}

	args := []string{
		"metrics",
		"--metrics", fmt.Sprint(promQLMetricCount),
		"--rate", fmt.Sprint(promQLMetricRate),
		"--otlp-http",
		"--otlp-endpoint", targetURL.Host,
		"--otlp-http-url-path", "/v1/metrics",
		"--otlp-header", fmt.Sprintf(`x-p-stream=%q`, dataset),
		"--otlp-header", `x-p-log-source="otel-metrics"`,
		"--otlp-header", fmt.Sprintf(`Authorization=%q`, basicAuthorization(username, password)),
	}
	if targetURL.Scheme == "http" {
		args = append(args, "--otlp-insecure")
	}

	t.Logf("ingesting OTLP metrics: dataset=%s metric=%s count=%d rate=%d/s endpoint=%s/v1/metrics", dataset, promQLMetricName, promQLMetricCount, promQLMetricRate, targetURL.String())
	output, err := exec.Command("telemetrygen", args...).CombinedOutput()
	require.NoErrorf(t, err, "telemetrygen metrics failed: %s", output)
	t.Logf("telemetrygen output: %s", strings.TrimSpace(string(output)))
	return time.Now().UTC().Add(time.Second)
}

func waitForPromQLMetric(t *testing.T, dataset string) {
	// Retries the real instant query until Parseable makes the ingested metric queryable.
	t.Helper()
	const visibilityTimeout = 5 * time.Minute
	deadline := time.Now().Add(visibilityTimeout)
	var lastResult string
	var lastErr error
	for time.Now().Before(deadline) {
		var response promQLResponse
		result, err := NewGlob.PBClient.RunJSON(
			context.Background(),
			&response,
			"promql", "run", promQLMetricName, "--dataset", dataset, "--from", "10m", "--step", "1m",
		)
		lastResult = fmt.Sprintf("exit=%d, stdout=%q, stderr=%q", result.ExitCode, result.Stdout, result.Stderr)
		lastErr = err
		if err == nil && response.Status == "success" && strings.Contains(string(response.Data), promQLMetricName) {
			return
		}
		time.Sleep(5 * time.Second)
	}
	require.NoErrorf(t, lastErr, "metric did not become queryable within %s (%s)", visibilityTimeout, lastResult)
	require.Failf(t, "metric did not become queryable", "metric %q was absent after %s (%s)", promQLMetricName, visibilityTimeout, lastResult)
}

func basicAuthorization(username, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
}

func createPromQLDataset(t *testing.T, dataset string) {
	t.Helper()
	result, err := NewGlob.PBClient.Run(context.Background(), "dataset", "add", dataset, "--type", "metrics")
	require.NoErrorf(t, err, "pb metrics dataset add failed (exit=%d, stdout=%q, stderr=%q)", result.ExitCode, result.Stdout, result.Stderr)
	cleanupPromQLDataset(t, dataset)
}

func cleanupPromQLDataset(t *testing.T, dataset string) {
	t.Helper()
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.PBClient, dataset)
	})
}

func TestPromQLCommands(t *testing.T) {
	// Verifies every PromQL command against Enterprise using the PB client.
	if NewGlob.Edition != "enterprise" {
		t.Skip("PromQL is only available in Enterprise")
	}
	t.Parallel()
	dataset := NewGlob.Stream + "promql"
	instantTime := ingestPromQLMetric(t, dataset).Format(time.RFC3339Nano)
	cleanupPromQLDataset(t, dataset)
	t.Logf("waiting %s for ingested metrics to flush before the first PromQL query", parseableLoadSettleWait)
	time.Sleep(parseableLoadSettleWait)
	waitForPromQLMetric(t, dataset)

	t.Run("queries", func(t *testing.T) {
		for _, tc := range promQLExpressionCases() {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				var response promQLResponse
				result, err := NewGlob.PBClient.RunJSON(
					context.Background(),
					&response,
					"promql", "run", tc.expression, "--dataset", dataset, "--from", "10m", "--step", "1m",
				)
				require.NoErrorf(t, err, "pb PromQL query %q failed (exit=%d, stdout=%q, stderr=%q)", tc.expression, result.ExitCode, result.Stdout, result.Stderr)
				require.Equal(t, "success", response.Status)

				var data promQLData
				require.NoError(t, json.Unmarshal(response.Data, &data))
				require.NotEmptyf(t, data.Result, "PromQL query %q returned no series", tc.expression)
			})
		}
	})

	for _, tc := range promQLCommandCases(dataset, instantTime) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var response promQLResponse
			result, err := NewGlob.PBClient.RunJSON(context.Background(), &response, tc.args...)
			require.NoErrorf(t, err, "pb %v failed (exit=%d, stdout=%q, stderr=%q)", tc.args, result.ExitCode, result.Stdout, result.Stderr)
			require.Equal(t, "success", response.Status)
			require.NotEmpty(t, response.Data)
			require.NotEqual(t, "null", string(response.Data))
			if tc.expectMetric {
				require.Contains(t, string(response.Data), promQLMetricName)
			}
		})
	}
}
