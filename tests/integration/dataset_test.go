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
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type prismDatasetResponse struct {
	Stream    string          `json:"stream"`
	Info      json.RawMessage `json:"info"`
	Schema    json.RawMessage `json:"schema"`
	Stats     json.RawMessage `json:"stats"`
	Retention json.RawMessage `json:"retention"`
	Counts    json.RawMessage `json:"counts"`
}

type prismDatasetInfoResponse struct {
	Info      json.RawMessage `json:"info"`
	Schema    json.RawMessage `json:"schema"`
	Stats     json.RawMessage `json:"stats"`
	Retention json.RawMessage `json:"retention"`
}

func requireJSONField(t *testing.T, field json.RawMessage, name string) {
	t.Helper()
	require.NotEmptyf(t, field, "%s is missing from the response", name)
	require.NotEqualf(t, "null", string(field), "%s is null in the response", name)
}

func TestSmokeListLogStream(t *testing.T) {
	// Verifies that the dataset list includes a newly created stream.
	t.Parallel()
	streamName := NewGlob.Stream + "list"
	CreateStream(t, NewGlob.PBClient, streamName)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.PBClient, streamName)
	})
	datasets := ListDatasetsWithPB(t, NewGlob.PBClient)
	require.Contains(t, datasets, PBDataset{Title: streamName})
}

func TestSmokeCreateStream(t *testing.T) {
	// Verifies that PB creates a logs dataset.
	t.Parallel()
	stream := NewGlob.Stream + "create"
	CreateStream(t, NewGlob.PBClient, stream)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.PBClient, stream)
	})
	info := DatasetInfoWithPB(t, NewGlob.PBClient, stream)
	require.Equal(t, "logs", info.DatasetType)
}

func TestSmokeDeleteStream(t *testing.T) {
	// Verifies that PB deletes an empty stream.
	t.Parallel()
	stream := NewGlob.Stream + "delete"
	CreateStream(t, NewGlob.PBClient, stream)
	DeleteStream(t, NewGlob.PBClient, stream)
	datasets := ListDatasetsWithPB(t, NewGlob.PBClient)
	require.NotContains(t, datasets, PBDataset{Title: stream})
}

func TestSmokeDetectSchema(t *testing.T) {
	// Verifies that schema detection returns the expected schema.
	t.Parallel()
	DetectSchema(t, NewGlob.QueryClient, SampleJson, SchemaBody)
}

func TestPrismDatasetUIEndpoints(t *testing.T) {
	// Verifies the combined dataset list and dataset info APIs used by Prism.
	t.Parallel()
	stream := NewGlob.Stream + "prismdataset"
	CreateStream(t, NewGlob.PBClient, stream)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.PBClient, stream)
	})

	t.Run("ListSelectedDataset", func(t *testing.T) {
		body := strings.NewReader(`{"streams":["` + stream + `"]}`)
		req, err := NewGlob.QueryClient.NewRequestAtPath("POST", "api/prism/v1/datasets", body)
		require.NoError(t, err)
		response, err := NewGlob.QueryClient.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)

		datasets, err := readJsonBody[[]prismDatasetResponse](response.Body)
		require.NoError(t, err)
		require.Len(t, datasets, 1)
		require.Equal(t, stream, datasets[0].Stream)
		requireJSONField(t, datasets[0].Info, "info")
		requireJSONField(t, datasets[0].Schema, "schema")
		requireJSONField(t, datasets[0].Stats, "stats")
		requireJSONField(t, datasets[0].Retention, "retention")
		requireJSONField(t, datasets[0].Counts, "counts")
	})

	t.Run("GetDatasetInfo", func(t *testing.T) {
		req, err := NewGlob.QueryClient.NewRequestAtPath("GET", "api/prism/v1/logstream/"+stream+"/info", nil)
		require.NoError(t, err)
		response, err := NewGlob.QueryClient.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)

		info, err := readJsonBody[prismDatasetInfoResponse](response.Body)
		require.NoError(t, err)
		requireJSONField(t, info.Info, "info")
		requireJSONField(t, info.Schema, "schema")
		requireJSONField(t, info.Stats, "stats")
		requireJSONField(t, info.Retention, "retention")
	})
}

func TestStaticSchemaIngestion(t *testing.T) {
	// Verifies that a static schema accepts matching fields and rejects new fields.
	t.Parallel()
	staticSchemaStream := NewGlob.Stream + "staticschema"
	staticSchemaFlagHeader := map[string]string{"X-P-Static-Schema-Flag": "true"}
	CreateStreamWithSchemaBody(t, NewGlob.QueryClient, staticSchemaStream, staticSchemaFlagHeader, SchemaPayload)

	client := NewGlob.QueryClient
	if NewGlob.IngestorUrl.String() != "" {
		client = NewGlob.IngestorClient
	}

	t.Run("AcceptMatchingFields", func(t *testing.T) {
		IngestOneEventForStaticSchemaStream_SameFieldsInLog(t, client, staticSchemaStream)
	})
	t.Run("RejectNewField", func(t *testing.T) {
		IngestOneEventForStaticSchemaStream_NewFieldInLog(t, client, staticSchemaStream)
	})
}

func TestCreateStream_WithCustomPartition_Success(t *testing.T) {
	// Verifies that a stream accepts one custom partition field.
	t.Parallel()
	customPartitionStream := NewGlob.Stream + "custompartitionsuccess"
	customHeader := map[string]string{"X-P-Custom-Partition": "level"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, customPartitionStream, customHeader)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.PBClient, customPartitionStream)
	})
}

func TestCreateStream_WithCustomPartition_Error(t *testing.T) {
	// Verifies that multiple custom partition fields are rejected.
	t.Parallel()
	customPartitionStream := NewGlob.Stream + "custompartitionerror"
	customHeader := map[string]string{"X-P-Custom-Partition": "level,os"}
	CreateStreamWithCustompartitionError(t, NewGlob.QueryClient, customPartitionStream, customHeader)
}
