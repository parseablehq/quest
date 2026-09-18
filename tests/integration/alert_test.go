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
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testTargetResponse struct {
	Target  testTarget `json:"target"`
	Enabled bool       `json:"enabled"`
}

type testTarget struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Type         string            `json:"type"`
	Endpoint     string            `json:"endpoint"`
	Headers      map[string]string `json:"headers"`
	SkipTLSCheck bool              `json:"skipTlsCheck"`
}

type testAlertTargetPolicy struct {
	AllowPrivate    bool     `json:"allowPrivate"`
	AllowedDomains  []string `json:"allowedDomains"`
	AllowedCIDRs    []string `json:"allowedCidrs"`
	DeniedDomains   []string `json:"deniedDomains"`
	DeniedCIDRs     []string `json:"deniedCidrs"`
	AllowInvalidTLS bool     `json:"allowInvalidTls"`
}

type testAlertResponse struct {
	Severity        string   `json:"severity"`
	Title           string   `json:"title"`
	ID              string   `json:"id"`
	State           string   `json:"state"`
	AlertType       string   `json:"alertType"`
	Tags            []string `json:"tags"`
	Created         string   `json:"created"`
	Datasets        []string `json:"datasets"`
	LastTriggeredAt string   `json:"lastTriggeredAt"`
	MetricName      string   `json:"metric_name"`
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

func getTestTargetByID(t *testing.T, targetID string) testTargetResponse {
	t.Helper()
	req, err := NewGlob.QueryClient.NewRequest("GET", "/targets/"+targetID, nil)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	target, err := readJsonBody[testTargetResponse](response.Body)
	require.NoError(t, err)
	return target
}

func updateTestTarget(t *testing.T, targetID, name string) testTarget {
	t.Helper()
	body := fmt.Sprintf(`{
		"name": %q,
		"type": "webhook",
		"endpoint": "https://webhook.site/ec627445-d52b-44e9-948d-56671df3581e",
		"headers": {"X-Quest-Test": "updated"},
		"skipTlsCheck": false
	}`, name)
	req, err := NewGlob.QueryClient.NewRequest("PUT", "/targets/"+targetID, strings.NewReader(body))
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	target, err := readJsonBody[testTarget](response.Body)
	require.NoError(t, err)
	return target
}

func getTestAlertTargetPolicy(t *testing.T) testAlertTargetPolicy {
	t.Helper()
	req, err := NewGlob.QueryClient.NewRequest("GET", "/alert-target-policy", nil)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	policy, err := readJsonBody[testAlertTargetPolicy](response.Body)
	require.NoError(t, err)
	return policy
}

func updateTestAlertTargetPolicy(t *testing.T, policy testAlertTargetPolicy) testAlertTargetPolicy {
	t.Helper()
	body, err := json.Marshal(policy)
	require.NoError(t, err)
	req, err := NewGlob.QueryClient.NewRequest("PUT", "/alert-target-policy", bytes.NewReader(body))
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	updated, err := readJsonBody[testAlertTargetPolicy](response.Body)
	require.NoError(t, err)
	return updated
}

func testAlertRequestBody(stream, targetID, title, metricName string) string {
	body := strings.Replace(getAlertBody(stream, targetID, metricName), `"title": "AlertTitle"`, fmt.Sprintf(`"title": %q`, title), 1)
	body = strings.Replace(body, `"value": 100`, `"value": 1`, 1)
	return strings.Replace(body, `"evalFrequency": 1`, `"evalFrequency": 10`, 1)
}

func createTestAlert(t *testing.T, stream, targetID, title, metricName string) string {
	t.Helper()
	body := testAlertRequestBody(stream, targetID, title, metricName)
	req, _ := NewGlob.QueryClient.NewRequest("POST", "/alerts", strings.NewReader(body))
	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

	alert := getTestAlert(t, title)
	return alert.ID
}

func listTestAlerts(t *testing.T, path string) []testAlertResponse {
	t.Helper()
	req, err := NewGlob.QueryClient.NewRequest("GET", path, nil)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	alerts, err := readJsonBody[[]testAlertResponse](response.Body)
	require.NoError(t, err)
	return alerts
}

func requireTestAlertListed(t *testing.T, alerts []testAlertResponse, alertID string) {
	t.Helper()
	for _, alert := range alerts {
		if alert.ID == alertID {
			return
		}
	}
	t.Fatalf("alert %q was not returned", alertID)
}

func updateTestAlert(t *testing.T, alertID, stream, targetID, title, metricName string) testAlertResponse {
	t.Helper()
	body := testAlertRequestBody(stream, targetID, title, metricName)
	req, err := NewGlob.QueryClient.NewRequest("PUT", "/alerts/"+alertID, strings.NewReader(body))
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	alert, err := readJsonBody[testAlertResponse](response.Body)
	require.NoError(t, err)
	return alert
}

func disableTestAlert(t *testing.T, alertID string) testAlertResponse {
	t.Helper()
	req, err := NewGlob.QueryClient.NewRequest("PATCH", "/alerts/"+alertID+"/disable", nil)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	alert, err := readJsonBody[testAlertResponse](response.Body)
	require.NoError(t, err)
	return alert
}

func enableTestAlert(t *testing.T, alertID string) testAlertResponse {
	t.Helper()
	req, err := NewGlob.QueryClient.NewRequest("PATCH", "/alerts/"+alertID+"/enable", nil)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	alert, err := readJsonBody[testAlertResponse](response.Body)
	require.NoError(t, err)
	return alert
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

func getTestAlertByID(t *testing.T, alertID string) testAlertResponse {
	t.Helper()
	req, err := NewGlob.QueryClient.NewRequest("GET", "/alerts/"+alertID, nil)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	alert, err := readJsonBody[testAlertResponse](response.Body)
	require.NoError(t, err)
	return alert
}

func muteTestAlertNotifications(t *testing.T, alertID string) {
	t.Helper()
	req, err := NewGlob.QueryClient.NewRequest(
		"PATCH",
		"/alerts/"+alertID+"/update_notification_state",
		strings.NewReader(`{"state":"indefinite"}`),
	)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
}

func evaluateTestAlert(t *testing.T, alertID string) {
	t.Helper()
	req, err := NewGlob.QueryClient.NewRequest("PUT", "/alerts/"+alertID+"/evaluate_alert", nil)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
}

func waitForTestAlertState(t *testing.T, alertID, expectedState string) testAlertResponse {
	t.Helper()
	var alert testAlertResponse
	require.Eventuallyf(t, func() bool {
		req, err := NewGlob.QueryClient.NewRequest("GET", "/alerts/"+alertID, nil)
		if err != nil {
			return false
		}
		response, err := NewGlob.QueryClient.Do(req)
		if err != nil {
			return false
		}
		defer response.Body.Close()
		if response.StatusCode != 200 {
			return false
		}
		alert, err = readJsonBody[testAlertResponse](response.Body)
		return err == nil && alert.State == expectedState
	}, 30*time.Second, 500*time.Millisecond, "alert %s did not become %s", alertID, expectedState)
	return alert
}

func TestSmokeSetTarget(t *testing.T) {
	// Verifies that a webhook target can be created, read, and updated.
	t.Parallel()
	targetName := NewGlob.Stream + "settarget"
	targetID := createTestTarget(t, targetName)
	t.Cleanup(func() {
		DeleteTarget(t, NewGlob.QueryClient, targetID)
	})

	target := getTestTargetByID(t, targetID)
	require.True(t, target.Enabled)
	require.Equal(t, targetID, target.Target.ID)
	require.Equal(t, targetName, target.Target.Name)
	require.Equal(t, "webhook", target.Target.Type)

	updated := updateTestTarget(t, targetID, targetName)
	require.Equal(t, targetID, updated.ID)
	require.Equal(t, targetName, updated.Name)
	require.Equal(t, "********", updated.Headers["X-Quest-Test"])

	target = getTestTargetByID(t, targetID)
	require.Equal(t, "********", target.Target.Headers["X-Quest-Test"])
}

func TestSmokeAlertTargetPolicy(t *testing.T) {
	// Verifies that the alert target network policy can be read and saved safely.
	t.Parallel()
	policy := getTestAlertTargetPolicy(t)
	updated := updateTestAlertTargetPolicy(t, policy)
	require.Equal(t, policy, updated)
	require.Equal(t, policy, getTestAlertTargetPolicy(t))
}

func TestSmokeAlertLifecycle(t *testing.T) {
	// Verifies an alert changes from not-triggered to triggered after ingestion.
	t.Parallel()
	stream := NewGlob.Stream + "alert"
	title := NewGlob.Stream + "alerttitle"
	metricName := NewGlob.Stream + "metric"
	CreateStreamWithSchemaBody(t, NewGlob.QueryClient, stream, map[string]string{"X-P-Static-Schema-Flag": "true"}, SchemaPayload)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.PBClient, stream)
	})
	targetID := createTestTarget(t, NewGlob.Stream+"alerttarget")
	t.Cleanup(func() {
		DeleteTarget(t, NewGlob.QueryClient, targetID)
	})
	alertID := createTestAlert(t, stream, targetID, title, metricName)
	t.Cleanup(func() {
		DeleteAlert(t, NewGlob.QueryClient, alertID)
	})

	alert := getTestAlertByID(t, alertID)
	require.Equal(t, alertID, alert.ID)
	require.Equal(t, title, alert.Title)
	require.Equal(t, "threshold", alert.AlertType)
	require.Equal(t, "medium", alert.Severity)
	require.Equal(t, []string{stream}, alert.Datasets)
	require.Equal(t, "not-triggered", alert.State)
	require.NotEmpty(t, alert.Created)
	require.Equal(t, metricName, alert.MetricName)

	t.Run("FilterByMetric", func(t *testing.T) {
		alerts := listTestAlerts(t, "/alerts?metric_name="+url.QueryEscape(metricName))
		requireTestAlertListed(t, alerts, alertID)
	})

	t.Run("FilterByTag", func(t *testing.T) {
		alerts := listTestAlerts(t, "/alerts?tags="+url.QueryEscape("quest-test"))
		requireTestAlertListed(t, alerts, alertID)
	})

	t.Run("ListTags", func(t *testing.T) {
		req, err := NewGlob.QueryClient.NewRequest("GET", "/alerts/list_tags", nil)
		require.NoError(t, err)
		response, err := NewGlob.QueryClient.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
		tags, err := readJsonBody[[]string](response.Body)
		require.NoError(t, err)
		require.Contains(t, tags, "quest-test")
	})

	updatedTitle := title + "updated"
	t.Run("UpdateAlert", func(t *testing.T) {
		updated := updateTestAlert(t, alertID, stream, targetID, updatedTitle, metricName)
		require.Equal(t, alertID, updated.ID)
		require.Equal(t, updatedTitle, updated.Title)
		require.Equal(t, metricName, updated.MetricName)
	})

	muteTestAlertNotifications(t, alertID)
	ingestAlertFixture(t, stream)
	time.Sleep(120 * time.Second)
	evaluateTestAlert(t, alertID)
	alert = waitForTestAlertState(t, alertID, "triggered")
	require.NotEmpty(t, alert.LastTriggeredAt)

	t.Run("DisableAlert", func(t *testing.T) {
		disabled := disableTestAlert(t, alertID)
		require.Equal(t, "disabled", disabled.State)
	})

	t.Run("EnableAlert", func(t *testing.T) {
		enabled := enableTestAlert(t, alertID)
		require.Equal(t, "not-triggered", enabled.State)
	})
}
