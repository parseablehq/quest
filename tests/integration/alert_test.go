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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testTargetResponse struct {
	Target struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"target"`
}

type testAlertResponse struct {
	Severity  string   `json:"severity"`
	Title     string   `json:"title"`
	ID        string   `json:"id"`
	State     string   `json:"state"`
	AlertType string   `json:"alertType"`
	Tags      []string `json:"tags"`
	Created   string   `json:"created"`
	Datasets  []string `json:"datasets"`
}

func ingestAlertFixture(t *testing.T, stream string) {
	t.Helper()
	client := NewGlob.QueryClient
	if NewGlob.IngestorUrl.String() != "" {
		client = NewGlob.IngestorClient
	}
	req, _ := client.NewRequest("POST", "ingest", strings.NewReader(`[{"level":"info"}]`))
	req.Header.Add("X-P-Stream", stream)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func createTestTarget(t *testing.T, name string) string {
	t.Helper()
	body := fmt.Sprintf(`{
		"name": %q,
		"type": "webhook",
		"endpoint": "https://webhook.site/ec627445-d52b-44e9-948d-56671df3581e",
		"headers": {},
		"skipTlsCheck": false
	}`, name)
	req, _ := NewGlob.QueryClient.NewRequest("POST", "/targets", strings.NewReader(body))
	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

	req, _ = NewGlob.QueryClient.NewRequest("GET", "/targets", nil)
	response, err = NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	targets, err := readJsonBody[[]testTargetResponse](response.Body)
	require.NoError(t, err)
	for _, target := range targets {
		if target.Target.Name == name {
			return target.Target.ID
		}
	}
	t.Fatalf("target %q was not returned by GET /targets", name)
	return ""
}

func createTestAlert(t *testing.T, stream, targetID, title string) string {
	t.Helper()
	body := strings.Replace(getAlertBody(stream, targetID), `"title": "AlertTitle"`, fmt.Sprintf(`"title": %q`, title), 1)
	req, _ := NewGlob.QueryClient.NewRequest("POST", "/alerts", strings.NewReader(body))
	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

	alert := getTestAlert(t, title)
	return alert.ID
}

func getTestAlert(t *testing.T, title string) testAlertResponse {
	t.Helper()
	req, _ := NewGlob.QueryClient.NewRequest("GET", "/alerts", nil)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equal(t, 200, response.StatusCode)
	alerts, err := readJsonBody[[]testAlertResponse](response.Body)
	require.NoError(t, err)
	for _, alert := range alerts {
		if alert.Title == title {
			return alert
		}
	}
	t.Fatalf("alert %q was not returned by GET /alerts", title)
	return testAlertResponse{}
}

func TestSmokeSetTarget(t *testing.T) {
	// Verifies that a webhook target can be created.
	t.Parallel()
	targetID := createTestTarget(t, NewGlob.Stream+"settarget")
	t.Cleanup(func() {
		DeleteTarget(t, NewGlob.QueryClient, targetID)
	})
}

func TestSmokeAlertLifecycle(t *testing.T) {
	// Verifies that an alert can be created and returns the expected details.
	t.Parallel()
	stream := NewGlob.Stream + "alert"
	title := NewGlob.Stream + "alerttitle"
	CreateStream(t, NewGlob.PBClient, stream)
	ingestAlertFixture(t, stream)
	time.Sleep(120 * time.Second)
	targetID := createTestTarget(t, NewGlob.Stream+"alerttarget")
	t.Cleanup(func() {
		DeleteTarget(t, NewGlob.QueryClient, targetID)
	})
	alertID := createTestAlert(t, stream, targetID, title)
	t.Cleanup(func() {
		DeleteAlert(t, NewGlob.QueryClient, alertID)
	})

	alert := getTestAlert(t, title)
	require.Equal(t, alertID, alert.ID)
	require.Equal(t, title, alert.Title)
	require.Equal(t, "threshold", alert.AlertType)
	require.Equal(t, "Medium", alert.Severity)
	require.Equal(t, []string{stream}, alert.Datasets)
	require.NotEmpty(t, alert.State)
	require.NotEmpty(t, alert.Created)
}
