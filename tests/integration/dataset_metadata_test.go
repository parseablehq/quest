// Copyright (c) 2023 Cloudnatively Services Pvt Ltd
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

type prismDatasetMetadata struct {
	Tags []string `json:"tags"`
}

type prismTaggedDataset struct {
	Name      string `json:"name"`
	LogSource string `json:"logSource"`
}

func putPrismDatasetTags(t *testing.T, dataset string, tags []string) prismDatasetMetadata {
	t.Helper()
	body, err := json.Marshal(map[string][]string{"tags": tags})
	require.NoError(t, err)
	req, err := NewGlob.QueryClient.NewRequestAtPath("PUT", "api/prism/v1/datasets/"+dataset, bytes.NewReader(body))
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	responseBody := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "PUT dataset metadata returned %s: %s", response.Status, responseBody)
	var metadata prismDatasetMetadata
	require.NoError(t, json.Unmarshal([]byte(responseBody), &metadata))
	return metadata
}

func listPrismDatasetsByTag(t *testing.T, tag string) []prismTaggedDataset {
	t.Helper()
	req, err := NewGlob.QueryClient.NewRequestAtPath("GET", "api/prism/v1/datasets/tags/"+tag, nil)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "GET datasets by tag returned %s", response.Status)
	datasets, err := readJsonBody[[]prismTaggedDataset](response.Body)
	require.NoError(t, err)
	return datasets
}

func TestEnterprisePrismDatasetTags(t *testing.T) {
	if NewGlob.Edition != "enterprise" {
		t.Skip("Prism dataset metadata endpoints are only available in Enterprise")
	}
	t.Parallel()

	const tag = "agent-observability"
	datasetA := NewGlob.Stream + "agentmetadataa"
	datasetB := NewGlob.Stream + "agentmetadatab"
	traceHeaders := map[string]string{
		"X-P-Telemetry-Type": "traces",
		"X-P-Log-Source":     "otel-traces",
	}
	CreateStreamWithHeader(t, NewGlob.QueryClient, datasetA, traceHeaders)
	t.Cleanup(func() { DeleteStream(t, NewGlob.PBClient, datasetA) })
	CreateStreamWithHeader(t, NewGlob.QueryClient, datasetB, traceHeaders)
	t.Cleanup(func() { DeleteStream(t, NewGlob.PBClient, datasetB) })

	for _, dataset := range []string{datasetA, datasetB} {
		metadata := putPrismDatasetTags(t, dataset, []string{tag})
		require.Equal(t, []string{tag}, metadata.Tags)
	}

	tagged := listPrismDatasetsByTag(t, tag)
	require.Contains(t, tagged, prismTaggedDataset{Name: datasetA, LogSource: "otel-traces"})
	require.Contains(t, tagged, prismTaggedDataset{Name: datasetB, LogSource: "otel-traces"})

	metadata := putPrismDatasetTags(t, datasetA, []string{})
	require.NotNil(t, metadata.Tags)
	require.Empty(t, metadata.Tags)

	tagged = listPrismDatasetsByTag(t, tag)
	for _, dataset := range tagged {
		require.NotEqual(t, datasetA, dataset.Name)
	}
	require.Contains(t, tagged, prismTaggedDataset{Name: datasetB, LogSource: "otel-traces"})
}
