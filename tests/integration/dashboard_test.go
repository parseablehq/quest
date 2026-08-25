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
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testDashboardTile struct {
	TileID string `json:"tile_id"`
	Title  string `json:"title"`
}

type testDashboardResponse struct {
	Version     string              `json:"version"`
	Title       string              `json:"title"`
	Author      string              `json:"author"`
	DashboardID string              `json:"dashboardId"`
	Created     string              `json:"created"`
	Modified    string              `json:"modified"`
	Tags        []string            `json:"tags"`
	IsFavorite  bool                `json:"isFavorite"`
	Tiles       []testDashboardTile `json:"tiles"`
}

func dashboardRequest(t *testing.T, method, path string, payload any, expectedStatus int) *testDashboardResponse {
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

	dashboard, err := readJsonBody[testDashboardResponse](response.Body)
	require.NoError(t, err)
	return &dashboard
}

func listTestDashboards(t *testing.T, path string) []testDashboardResponse {
	t.Helper()
	req, err := NewGlob.QueryClient.NewRequest("GET", path, nil)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	dashboards, err := readJsonBody[[]testDashboardResponse](response.Body)
	require.NoError(t, err)
	return dashboards
}

func requireTestDashboardListed(t *testing.T, dashboards []testDashboardResponse, dashboardID string) testDashboardResponse {
	t.Helper()
	for _, dashboard := range dashboards {
		if dashboard.DashboardID == dashboardID {
			return dashboard
		}
	}
	t.Fatalf("dashboard %q was not returned", dashboardID)
	return testDashboardResponse{}
}

func TestSmokeDashboardLifecycle(t *testing.T) {
	// Verifies all shared dashboard CRUD, filter, update, and tile APIs.
	t.Parallel()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	title := NewGlob.Stream + " dashboard " + suffix
	updatedTitle := title + " updated"
	finalTitle := title + " final"
	tag := NewGlob.Stream + "-dashboard-" + suffix
	updatedTag := tag + "-updated"
	tileID := "01J00000000000000000000001"

	created := dashboardRequest(t, "POST", "/dashboards", map[string]any{
		"title":      title,
		"tags":       []string{tag},
		"tiles":      []any{},
		"sections":   []any{},
		"variables":  []any{},
		"isFavorite": false,
	}, 200)
	require.NotEmpty(t, created.DashboardID)
	require.Equal(t, title, created.Title)
	require.Equal(t, []string{tag}, created.Tags)
	require.False(t, created.IsFavorite)
	require.NotEmpty(t, created.Author)
	require.NotEmpty(t, created.Created)
	require.NotEmpty(t, created.Modified)

	dashboardDeleted := false
	t.Cleanup(func() {
		if !dashboardDeleted {
			dashboardRequest(t, "DELETE", "/dashboards/"+created.DashboardID, nil, 200)
		}
	})

	t.Run("ListDashboards", func(t *testing.T) {
		listed := requireTestDashboardListed(t, listTestDashboards(t, "/dashboards"), created.DashboardID)
		require.Equal(t, title, listed.Title)
		require.Equal(t, []string{tag}, listed.Tags)
	})

	t.Run("ListTopFive", func(t *testing.T) {
		dashboards := listTestDashboards(t, "/dashboards?limit=5")
		require.LessOrEqual(t, len(dashboards), 5)
		requireTestDashboardListed(t, dashboards, created.DashboardID)
	})

	t.Run("FilterByTag", func(t *testing.T) {
		path := "/dashboards?tags=" + url.QueryEscape(tag)
		requireTestDashboardListed(t, listTestDashboards(t, path), created.DashboardID)
	})

	t.Run("ListTags", func(t *testing.T) {
		req, err := NewGlob.QueryClient.NewRequest("GET", "/dashboards/list_tags", nil)
		require.NoError(t, err)
		response, err := NewGlob.QueryClient.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
		tags, err := readJsonBody[[]string](response.Body)
		require.NoError(t, err)
		require.Contains(t, tags, tag)
	})

	t.Run("GetDashboard", func(t *testing.T) {
		dashboard := dashboardRequest(t, "GET", "/dashboards/"+created.DashboardID, nil, 200)
		require.Equal(t, created.DashboardID, dashboard.DashboardID)
		require.Equal(t, title, dashboard.Title)
		require.Equal(t, []string{tag}, dashboard.Tags)
	})

	t.Run("UpdateDashboard", func(t *testing.T) {
		dashboard := dashboardRequest(t, "PUT", "/dashboards/"+created.DashboardID, map[string]any{
			"title":      updatedTitle,
			"tags":       []string{tag},
			"tiles":      []any{},
			"sections":   []any{},
			"variables":  []any{},
			"isFavorite": false,
		}, 200)
		require.Equal(t, updatedTitle, dashboard.Title)
		require.Equal(t, []string{tag}, dashboard.Tags)
	})

	t.Run("RenameFavoriteAndRetag", func(t *testing.T) {
		query := url.Values{}
		query.Set("isFavorite", "true")
		query.Set("renameTo", finalTitle)
		query.Set("tags", tag+","+updatedTag)
		dashboard := dashboardRequest(t, "PUT", "/dashboards/"+created.DashboardID+"?"+query.Encode(), nil, 200)
		require.Equal(t, finalTitle, dashboard.Title)
		require.True(t, dashboard.IsFavorite)
		require.ElementsMatch(t, []string{tag, updatedTag}, dashboard.Tags)
	})

	t.Run("AddTile", func(t *testing.T) {
		dashboard := dashboardRequest(t, "PUT", "/dashboards/"+created.DashboardID+"/add_tile", map[string]any{
			"tile_id":    tileID,
			"title":      "Quest dashboard tile",
			"chartQuery": "SELECT 1",
			"dbName":     NewGlob.Stream,
			"chartType":  "table",
			"tileType":   "code",
			"config":     map[string]any{},
			"layout": map[string]int{
				"x": 0,
				"y": 0,
				"w": 4,
				"h": 4,
			},
		}, 200)
		require.Len(t, dashboard.Tiles, 1)
		require.Equal(t, tileID, dashboard.Tiles[0].TileID)
		require.Equal(t, "Quest dashboard tile", dashboard.Tiles[0].Title)
	})

	t.Run("DeleteDashboard", func(t *testing.T) {
		dashboardRequest(t, "DELETE", "/dashboards/"+created.DashboardID, nil, 200)
		dashboardDeleted = true
		dashboardRequest(t, "GET", "/dashboards/"+created.DashboardID, nil, 400)
	})
}
