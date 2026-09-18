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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testSavedFilterQuery struct {
	FilterType  string `json:"filter_type"`
	FilterQuery string `json:"filter_query"`
}

type testSavedFilterResponse struct {
	Version           string               `json:"version"`
	UserID            string               `json:"user_id"`
	StreamName        string               `json:"stream_name"`
	FilterName        string               `json:"filter_name"`
	FilterID          string               `json:"filter_id"`
	FilterDescription string               `json:"filter_description"`
	Query             testSavedFilterQuery `json:"query"`
}

func savedFilterRequest(t *testing.T, method, path string, payload any, expectedStatus int) *testSavedFilterResponse {
	t.Helper()
	var body *bytes.Reader
	if payload == nil {
		body = bytes.NewReader(nil)
	} else {
		encoded, err := json.Marshal(payload)
		require.NoError(t, err)
		body = bytes.NewReader(encoded)
	}

	req, err := NewGlob.QueryClient.NewRequest(method, path, body)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, expectedStatus, response.StatusCode, "Server returned http code: %s", response.Status)
	if expectedStatus != 200 || method == "DELETE" {
		return nil
	}

	filter, err := readJsonBody[testSavedFilterResponse](response.Body)
	require.NoError(t, err)
	return &filter
}

func listTestSavedFilters(t *testing.T) []testSavedFilterResponse {
	t.Helper()
	req, err := NewGlob.QueryClient.NewRequest("GET", "/filters", nil)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	filters, err := readJsonBody[[]testSavedFilterResponse](response.Body)
	require.NoError(t, err)
	return filters
}

func requireTestSavedFilterListed(t *testing.T, filters []testSavedFilterResponse, filterID string) testSavedFilterResponse {
	t.Helper()
	for _, filter := range filters {
		if filter.FilterID == filterID {
			return filter
		}
	}
	t.Fatalf("saved filter %q was not returned", filterID)
	return testSavedFilterResponse{}
}

func TestSmokeSavedFilterLifecycle(t *testing.T) {
	// Verifies create, list, get, update, and delete for a saved filter.
	t.Parallel()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	stream := NewGlob.Stream + "savedfilter"
	filterName := NewGlob.Stream + " saved filter " + suffix
	updatedName := filterName + " updated"
	query := "SELECT * FROM \"" + stream + "\" LIMIT 10"
	updatedQuery := "SELECT * FROM \"" + stream + "\" LIMIT 5"

	created := savedFilterRequest(t, "POST", "/filters", map[string]any{
		"stream_name":        stream,
		"filter_name":        filterName,
		"filter_description": "Quest saved filter",
		"query": map[string]string{
			"filter_type":  "sql",
			"filter_query": query,
		},
		"time_filter": nil,
	}, 200)
	require.NotEmpty(t, created.FilterID)
	require.Equal(t, "v2", created.Version)
	require.NotEmpty(t, created.UserID)
	require.Equal(t, stream, created.StreamName)
	require.Equal(t, filterName, created.FilterName)
	require.Equal(t, query, created.Query.FilterQuery)

	filterDeleted := false
	t.Cleanup(func() {
		if !filterDeleted {
			savedFilterRequest(t, "DELETE", "/filters/"+created.FilterID, nil, 200)
		}
	})

	t.Run("ListFilters", func(t *testing.T) {
		listed := requireTestSavedFilterListed(t, listTestSavedFilters(t), created.FilterID)
		require.Equal(t, filterName, listed.FilterName)
		require.Equal(t, query, listed.Query.FilterQuery)
	})

	t.Run("GetFilter", func(t *testing.T) {
		filter := savedFilterRequest(t, "GET", "/filters/"+created.FilterID, nil, 200)
		require.Equal(t, created.FilterID, filter.FilterID)
		require.Equal(t, filterName, filter.FilterName)
		require.Equal(t, query, filter.Query.FilterQuery)
	})

	t.Run("UpdateFilter", func(t *testing.T) {
		filter := savedFilterRequest(t, "PUT", "/filters/"+created.FilterID, map[string]any{
			"filter_id":          created.FilterID,
			"stream_name":        stream,
			"filter_name":        updatedName,
			"filter_description": "Updated by Quest",
			"query": map[string]string{
				"filter_type":  "sql",
				"filter_query": updatedQuery,
			},
			"time_filter": nil,
		}, 200)
		require.Equal(t, created.FilterID, filter.FilterID)
		require.Equal(t, updatedName, filter.FilterName)
		require.Equal(t, "Updated by Quest", filter.FilterDescription)
		require.Equal(t, updatedQuery, filter.Query.FilterQuery)
	})

	t.Run("DeleteFilter", func(t *testing.T) {
		savedFilterRequest(t, "DELETE", "/filters/"+created.FilterID, nil, 200)
		filterDeleted = true
		savedFilterRequest(t, "GET", "/filters/"+created.FilterID, nil, 400)
		for _, filter := range listTestSavedFilters(t) {
			require.NotEqual(t, created.FilterID, filter.FilterID)
		}
	})
}
