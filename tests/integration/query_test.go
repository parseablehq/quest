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
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type queryWithFieldsResponse struct {
	Fields  []string          `json:"fields"`
	Records []json.RawMessage `json:"records"`
}

type countsResponse struct {
	Fields  []string       `json:"fields"`
	Records []countsRecord `json:"records"`
}

type countsRecord struct {
	Count uint64 `json:"count"`
}

type datasetFieldStats struct {
	FieldCount     float64            `json:"field_count"`
	DistinctCount  float64            `json:"distinct_count"`
	DistinctValues map[string]float64 `json:"distinct_values"`
}

func postQueryFeature(path string, payload any) (int, []byte, error) {
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

func TestSmokeIngestAndQuery(t *testing.T) {
	// Verifies ingestion and SQL queries across two streams.
	t.Parallel()
	stream1 := NewGlob.Stream + "ingestquery1"
	stream2 := NewGlob.Stream + "ingestquery2"
	CreateStream(t, NewGlob.PBClient, stream1)
	CreateStream(t, NewGlob.PBClient, stream2)

	if NewGlob.IngestorUrl.String() == "" {
		RunFlog(t, NewGlob.QueryClient, stream1)
		RunFlog(t, NewGlob.QueryClient, stream2)
	} else {
		RunFlog(t, NewGlob.IngestorClient, stream1)
		RunFlog(t, NewGlob.IngestorClient, stream2)
	}

	// Parseable persists ingested events in a two-minute batch. Both streams are
	// populated before this wait so all ingestion and query assertions can share
	// the same batch window.
	time.Sleep(120 * time.Second)

	t.Run("IngestEventsToStream", func(t *testing.T) {
		t.Parallel()
		QueryLogStreamCount(t, NewGlob.PBClient, stream1, 50)
		AssertStreamSchema(t, NewGlob.QueryClient, stream1, FlogJsonSchema)
	})

	t.Run("RunQueries", func(t *testing.T) {
		t.Parallel()
		QueryLogStreamCount(t, NewGlob.PBClient, stream1, 50)
		AssertQueryOK(t, NewGlob.PBClient, "SELECT * FROM %s", stream1)
		AssertQueryOK(t, NewGlob.PBClient, "SELECT * FROM %s OFFSET 25 LIMIT 25", stream1)

		for _, item := range flogStreamFields() {
			AssertQueryOK(t, NewGlob.PBClient, "SELECT %s FROM %s", item, stream1)
		}

		AssertQueryOK(t, NewGlob.PBClient, "SELECT * FROM %s WHERE method = 'POST'", stream1)
		AssertQueryOK(t, NewGlob.PBClient, "SELECT method, COUNT(*) FROM %s GROUP BY method", stream1)
		AssertQueryOK(t, NewGlob.PBClient, `SELECT DATE_TRUNC('minute', p_timestamp) as minute, COUNT(*) FROM %s GROUP BY minute`, stream1)
	})

	t.Run("QueryTwoStreams", func(t *testing.T) {
		t.Parallel()
		QueryTwoLogStreamCount(t, NewGlob.PBClient, stream1, stream2, 100)
	})

	t.Run("QueryWithFields", func(t *testing.T) {
		// Verifies the UI query response includes field names and records.
		t.Parallel()
		endTime := time.Now().Add(time.Second).Format(time.RFC3339Nano)
		startTime := time.Now().Add(-30 * time.Minute).Format(time.RFC3339Nano)
		payload, err := json.Marshal(map[string]string{
			"query":     "SELECT method, status FROM " + stream1 + " LIMIT 1",
			"startTime": startTime,
			"endTime":   endTime,
		})
		require.NoError(t, err)

		req, err := NewGlob.QueryClient.NewRequest("POST", "query", bytes.NewReader(payload))
		require.NoError(t, err)
		params := req.URL.Query()
		params.Set("fields", "true")
		req.URL.RawQuery = params.Encode()

		response, err := NewGlob.QueryClient.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)

		result, err := readJsonBody[queryWithFieldsResponse](response.Body)
		require.NoError(t, err)
		require.Equal(t, []string{"method", "status"}, result.Fields)
		require.Len(t, result.Records, 1)
	})

	t.Run("QueryCounts", func(t *testing.T) {
		// Verifies that the UI counts API returns graph buckets for ingested events.
		t.Parallel()
		endTime := time.Now().Add(time.Second).Format(time.RFC3339Nano)
		startTime := time.Now().Add(-30 * time.Minute).Format(time.RFC3339Nano)
		payload, err := json.Marshal(map[string]any{
			"stream":    stream1,
			"startTime": startTime,
			"endTime":   endTime,
			"numBins":   10,
		})
		require.NoError(t, err)

		req, err := NewGlob.QueryClient.NewRequest("POST", "counts", bytes.NewReader(payload))
		require.NoError(t, err)
		response, err := NewGlob.QueryClient.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)

		result, err := readJsonBody[countsResponse](response.Body)
		require.NoError(t, err)
		require.Equal(t, []string{"start_time", "end_time", "count"}, result.Fields)
		require.NotEmpty(t, result.Records)
		var total uint64
		for _, record := range result.Records {
			total += record.Count
		}
		require.Equal(t, uint64(50), total)
	})

	t.Run("DatasetFieldStats", func(t *testing.T) {
		// Verifie that dataset field statistics are returned for a stream.
		t.Parallel()
		payload := map[string]any{
			"datasetName": stream1,
			"startTime":   time.Now().Add(-30 * time.Minute).Format(time.RFC3339Nano),
			"endTime":     time.Now().Add(time.Minute).Format(time.RFC3339Nano),
			"fields":      []string{"method", "status"},
			"offset":      0,
			"limit":       5,
		}
		var stats map[string]datasetFieldStats
		var status int
		var body []byte
		var err error
		deadline := time.Now().Add(time.Minute)
		for {
			status, body, err = postQueryFeature("api/prism/v1/dataset_stats", payload)
			if err == nil && status == 200 && json.Unmarshal(body, &stats) == nil &&
				stats["method"].FieldCount > 0 && stats["status"].FieldCount > 0 {
				break
			}
			if time.Now().After(deadline) {
				require.FailNowf(t, "dataset stats did not become available", "status=%d body=%s error=%v", status, body, err)
			}
			time.Sleep(10 * time.Second)
		}
		require.Greater(t, stats["method"].DistinctCount, float64(0))
		require.NotEmpty(t, stats["method"].DistinctValues)
		require.Greater(t, stats["status"].DistinctCount, float64(0))
	})

}

func TestQueryContextEndpoint(t *testing.T) {
	// Verifies the shared log-context route is registered.
	t.Parallel()
	status, body, err := postQueryFeature("api/v1/query/context", map[string]any{})
	require.NoError(t, err)
	require.NotEqualf(t, 404, status, "log-context route was not found: %s", body)
}

func TestForecastCountsEndpoint(t *testing.T) {
	// Verifies the Enterprise forecast-counts route is registered.
	if NewGlob.Edition != "enterprise" {
		t.Skip("forecast counts are only available in Enterprise")
	}
	t.Parallel()
	status, body, err := postQueryFeature("api/v1/counts?forecast=true", map[string]any{})
	require.NoError(t, err)
	require.Equalf(t, 400, status, "expected request validation from the forecast-counts route: %s", body)
}

func TestPanoramaForecastEndpoint(t *testing.T) {
	// Verifies the Enterprise Panorama forecast route is registered.
	if NewGlob.Edition != "enterprise" {
		t.Skip("Panorama forecast is only available in Enterprise")
	}
	t.Parallel()
	status, body, err := postQueryFeature("api/prism/v1/panorama/forecast", map[string]any{})
	require.NoError(t, err)
	require.Equalf(t, 400, status, "expected request validation from the Panorama route: %s", body)
}
