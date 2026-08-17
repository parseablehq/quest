// Copyright (c) 2023 Cloudnatively Services Pvt Ltd
//
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	vus          = "10"
	duration     = "2m"
	schema_count = "10"
	events_count = "5"
)

// The default role is server-wide state. RBAC tests are still scheduled as
// parallel tests, but their short user/role mutations must not overlap.
var rbacMu sync.Mutex

var k6Mu sync.RWMutex

func TestSmokeListLogStream(t *testing.T) {
	t.Parallel()
	streamName := NewGlob.Stream + "list"
	CreateStream(t, NewGlob.QueryClient, streamName)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.QueryClient, streamName)
	})
	req, err := NewGlob.QueryClient.NewRequest("GET", "logstream", nil)
	require.NoErrorf(t, err, "Request failed: %s", err)

	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)

	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status)
	type streamInfo struct {
		Name string `json:"name"`
	}
	res, err := readJsonBody[[]streamInfo](bytes.NewBufferString(body))
	require.NoError(t, err)
	require.Contains(t, res, streamInfo{Name: streamName})
}

func TestSmokeCreateStream(t *testing.T) {
	t.Parallel()
	stream := NewGlob.Stream + "create"
	CreateStream(t, NewGlob.QueryClient, stream)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.QueryClient, stream)
	})
}

func TestSmokeDetectSchema(t *testing.T) {
	t.Parallel()
	DetectSchema(t, NewGlob.QueryClient, SampleJson, SchemaBody)
}

// func TestTimePartition_TimeStampMismatch(t *testing.T) {
// 	historicalStream := NewGlob.Stream + "historical"
// 	timeHeader := map[string]string{"X-P-Time-Partition": "source_time"}
// 	CreateStreamWithHeader(t, NewGlob.QueryClient, historicalStream, timeHeader)
// 	if NewGlob.IngestorUrl.String() == "" {
// 		IngestOneEventWithTimePartition_TimeStampMismatch(t, NewGlob.QueryClient, historicalStream)
// 	} else {
// 		IngestOneEventWithTimePartition_TimeStampMismatch(t, NewGlob.IngestorClient, historicalStream)
// 	}
// 	DeleteStream(t, NewGlob.QueryClient, historicalStream)
// }

// func TestTimePartition_NoTimePartitionInLog(t *testing.T) {
// 	historicalStream := NewGlob.Stream + "historical"
// 	timeHeader := map[string]string{"X-P-Time-Partition": "source_time"}
// 	CreateStreamWithHeader(t, NewGlob.QueryClient, historicalStream, timeHeader)
// 	if NewGlob.IngestorUrl.String() == "" {
// 		IngestOneEventWithTimePartition_NoTimePartitionInLog(t, NewGlob.QueryClient, historicalStream)
// 	} else {
// 		IngestOneEventWithTimePartition_NoTimePartitionInLog(t, NewGlob.IngestorClient, historicalStream)
// 	}
// 	DeleteStream(t, NewGlob.QueryClient, historicalStream)
// }

// func TestTimePartition_IncorrectDateTimeFormatTimePartitionInLog(t *testing.T) {
// 	historicalStream := NewGlob.Stream + "historical"
// 	timeHeader := map[string]string{"X-P-Time-Partition": "source_time"}
// 	CreateStreamWithHeader(t, NewGlob.QueryClient, historicalStream, timeHeader)
// 	if NewGlob.IngestorUrl.String() == "" {
// 		IngestOneEventWithTimePartition_IncorrectDateTimeFormatTimePartitionInLog(t, NewGlob.QueryClient, historicalStream)
// 	} else {
// 		IngestOneEventWithTimePartition_IncorrectDateTimeFormatTimePartitionInLog(t, NewGlob.IngestorClient, historicalStream)
// 	}
// 	DeleteStream(t, NewGlob.QueryClient, historicalStream)
// }

func TestLoadStream_StaticSchema_EventWithSameFields(t *testing.T) {
	t.Parallel()
	staticSchemaStream := NewGlob.Stream + "staticschemasame"
	staticSchemaFlagHeader := map[string]string{"X-P-Static-Schema-Flag": "true"}
	CreateStreamWithSchemaBody(t, NewGlob.QueryClient, staticSchemaStream, staticSchemaFlagHeader, SchemaPayload)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.QueryClient, staticSchemaStream)
	})
	if NewGlob.IngestorUrl.String() == "" {
		IngestOneEventForStaticSchemaStream_SameFieldsInLog(t, NewGlob.QueryClient, staticSchemaStream)
	} else {
		IngestOneEventForStaticSchemaStream_SameFieldsInLog(t, NewGlob.IngestorClient, staticSchemaStream)
	}
}

func TestLoadStreamBatchWithK6_StaticSchema(t *testing.T) {
	if NewGlob.Mode == "load" {
		t.Parallel()

		staticSchemaStream := NewGlob.Stream + "loadbatchstaticschema"
		staticSchemaFlagHeader := map[string]string{"X-P-Static-Schema-Flag": "true"}
		CreateStreamWithSchemaBody(t, NewGlob.QueryClient, staticSchemaStream, staticSchemaFlagHeader, SchemaPayload)
		t.Cleanup(func() {
			DeleteStream(t, NewGlob.QueryClient, staticSchemaStream)
		})
		if NewGlob.IngestorUrl.String() == "" {
			cmd := exec.Command("k6",
				"run",
				"--address", "",
				"--vus", vus,
				"--duration", duration,
				"-e", fmt.Sprintf("P_URL=%s", &NewGlob.QueryUrl),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", staticSchemaStream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
				"./scripts/load_batch_events.js")

			runK6Load(t, cmd)
		} else {
			cmd := exec.Command("k6",
				"run",
				"--address", "",
				"--vus", vus,
				"--duration", duration,
				"-e", fmt.Sprintf("P_URL=%s", &NewGlob.IngestorUrl),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", staticSchemaStream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
				"./scripts/load_batch_events.js")

			runK6Load(t, cmd)
		}
	}
}

func TestLoadStream_StaticSchema_EventWithNewField(t *testing.T) {
	t.Parallel()
	staticSchemaStream := NewGlob.Stream + "staticschemanew"
	staticSchemaFlagHeader := map[string]string{"X-P-Static-Schema-Flag": "true"}
	CreateStreamWithSchemaBody(t, NewGlob.QueryClient, staticSchemaStream, staticSchemaFlagHeader, SchemaPayload)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.QueryClient, staticSchemaStream)
	})
	if NewGlob.IngestorUrl.String() == "" {
		IngestOneEventForStaticSchemaStream_NewFieldInLog(t, NewGlob.QueryClient, staticSchemaStream)
	} else {
		IngestOneEventForStaticSchemaStream_NewFieldInLog(t, NewGlob.IngestorClient, staticSchemaStream)
	}
}

func TestCreateStream_WithCustomPartition_Success(t *testing.T) {
	t.Parallel()
	customPartitionStream := NewGlob.Stream + "custompartitionsuccess"
	customHeader := map[string]string{"X-P-Custom-Partition": "level"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, customPartitionStream, customHeader)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.QueryClient, customPartitionStream)
	})
}

func TestCreateStream_WithCustomPartition_Error(t *testing.T) {
	t.Parallel()
	customPartitionStream := NewGlob.Stream + "custompartitionerror"
	customHeader := map[string]string{"X-P-Custom-Partition": "level,os"}
	CreateStreamWithCustompartitionError(t, NewGlob.QueryClient, customPartitionStream, customHeader)
}

func TestSmokeIngestAndQuery(t *testing.T) {
	t.Parallel()
	stream1 := NewGlob.Stream + "ingestquery1"
	stream2 := NewGlob.Stream + "ingestquery2"
	CreateStream(t, NewGlob.QueryClient, stream1)
	CreateStream(t, NewGlob.QueryClient, stream2)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.QueryClient, stream1)
		DeleteStream(t, NewGlob.QueryClient, stream2)
	})

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
		QueryLogStreamCount(t, NewGlob.QueryClient, stream1, 50)
		AssertStreamSchema(t, NewGlob.QueryClient, stream1, FlogJsonSchema)
	})

	t.Run("RunQueries", func(t *testing.T) {
		QueryLogStreamCount(t, NewGlob.QueryClient, stream1, 50)
		AssertQueryOK(t, NewGlob.QueryClient, "SELECT * FROM %s", stream1)
		AssertQueryOK(t, NewGlob.QueryClient, "SELECT * FROM %s OFFSET 25 LIMIT 25", stream1)

		for _, item := range flogStreamFields() {
			AssertQueryOK(t, NewGlob.QueryClient, "SELECT %s FROM %s", item, stream1)
		}

		AssertQueryOK(t, NewGlob.QueryClient, "SELECT * FROM %s WHERE method = 'POST'", stream1)
		AssertQueryOK(t, NewGlob.QueryClient, "SELECT method, COUNT(*) FROM %s GROUP BY method", stream1)
		AssertQueryOK(t, NewGlob.QueryClient, `SELECT DATE_TRUNC('minute', p_timestamp) as minute, COUNT(*) FROM %s GROUP BY minute`, stream1)
	})

	t.Run("QueryTwoStreams", func(t *testing.T) {
		QueryTwoLogStreamCount(t, NewGlob.QueryClient, stream1, stream2, 100)
	})

}

func TestSmokeLoadWithK6Streams(t *testing.T) {
	t.Parallel()
	stream := NewGlob.Stream + "smokeload"
	CreateStream(t, NewGlob.QueryClient, stream)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.QueryClient, stream)
	})
	runK6Smoke(t, stream)

	customPartitionStream := NewGlob.Stream + "smokeloadcustompartition"
	customHeader := map[string]string{"X-P-Custom-Partition": "level"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, customPartitionStream, customHeader)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.QueryClient, customPartitionStream)
	})
	runK6Smoke(t, customPartitionStream)

	time.Sleep(150 * time.Second)

	t.Run("LoadWithK6Stream", func(t *testing.T) {
		QueryLogStreamCount(t, NewGlob.QueryClient, stream, 20000)
		AssertStreamSchema(t, NewGlob.QueryClient, stream, SchemaBody)
	})

	t.Run("Load_CustomPartition_WithK6Stream", func(t *testing.T) {
		QueryLogStreamCount(t, NewGlob.QueryClient, customPartitionStream, 20000)
	})

}

func runK6Smoke(t *testing.T, stream string) {
	t.Helper()
	k6Mu.Lock()
	defer k6Mu.Unlock()
	url := NewGlob.QueryUrl.String()
	username := NewGlob.QueryUsername
	password := NewGlob.QueryPassword
	if NewGlob.IngestorUrl.String() != "" {
		url = NewGlob.IngestorUrl.String()
		username = NewGlob.IngestorUsername
		password = NewGlob.IngestorPassword
	}

	cmd := exec.Command("k6",
		"run",
		"--address", "",
		"-e", fmt.Sprintf("P_URL=%s", url),
		"-e", fmt.Sprintf("P_USERNAME=%s", username),
		"-e", fmt.Sprintf("P_PASSWORD=%s", password),
		"-e", fmt.Sprintf("P_STREAM=%s", stream),
		"./scripts/smoke.js")

	op, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "k6 failed: %s", string(op))
	t.Log(string(op))
}

func runK6Load(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	k6Mu.RLock()
	defer k6Mu.RUnlock()
	op, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "k6 failed: %s", string(op))
	t.Log(string(op))
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

// func TestSmokeLoad_TimePartition_WithK6Stream(t *testing.T) {
// 	time_partition_stream := NewGlob.Stream + "timepartition"
// 	timeHeader := map[string]string{"X-P-Time-Partition": "source_time", "X-P-Time-Partition-Limit": "365d"}
// 	CreateStreamWithHeader(t, NewGlob.QueryClient, time_partition_stream, timeHeader)
// 	if NewGlob.IngestorUrl.String() == "" {
// 		cmd := exec.Command("k6",
// 			"run",
// 			"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
// 			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
// 			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
// 			"-e", fmt.Sprintf("P_STREAM=%s", time_partition_stream),
// 			"./scripts/smoke.js")

// 		cmd.Run()
// 		cmd.Output()
// 	} else {
// 		cmd := exec.Command("k6",
// 			"run",
// 			"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
// 			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
// 			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
// 			"-e", fmt.Sprintf("P_STREAM=%s", time_partition_stream),
// 			"./scripts/smoke.js")

// 		cmd.Run()
// 		cmd.Output()
// 	}
// 	time.Sleep(120 * time.Second)
// 	QueryLogStreamCount_Historical(t, NewGlob.QueryClient, time_partition_stream, 20000)
// 	DeleteStream(t, NewGlob.QueryClient, time_partition_stream)
// }

// func TestSmokeLoad_TimeAndCustomPartition_WithK6Stream(t *testing.T) {
// 	custom_partition_stream := NewGlob.Stream + "timecustompartition"
// 	customHeader := map[string]string{"X-P-Custom-Partition": "level", "X-P-Time-Partition": "source_time", "X-P-Time-Partition-Limit": "365d"}
// 	CreateStreamWithHeader(t, NewGlob.QueryClient, custom_partition_stream, customHeader)
// 	if NewGlob.IngestorUrl.String() == "" {
// 		cmd := exec.Command("k6",
// 			"run",
// 			"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
// 			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
// 			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
// 			"-e", fmt.Sprintf("P_STREAM=%s", custom_partition_stream),
// 			"./scripts/smoke.js")

// 		cmd.Run()
// 		cmd.Output()
// 	} else {
// 		cmd := exec.Command("k6",
// 			"run",
// 			"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
// 			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
// 			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
// 			"-e", fmt.Sprintf("P_STREAM=%s", custom_partition_stream),
// 			"./scripts/smoke.js")

// 		cmd.Run()
// 		cmd.Output()
// 	}
// 	time.Sleep(180 * time.Second)
// 	QueryLogStreamCount_Historical(t, NewGlob.QueryClient, custom_partition_stream, 20000)
// 	DeleteStream(t, NewGlob.QueryClient, custom_partition_stream)
// }

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
	t.Parallel()
	targetID := createTestTarget(t, NewGlob.Stream+"settarget")
	t.Cleanup(func() {
		DeleteTarget(t, NewGlob.QueryClient, targetID)
	})
}

func TestSmokeSetAlert(t *testing.T) {
	t.Parallel()
	stream := NewGlob.Stream + "setalert"
	CreateStream(t, NewGlob.QueryClient, stream)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.QueryClient, stream)
	})
	runK6Smoke(t, stream)
	time.Sleep(120 * time.Second)
	targetID := createTestTarget(t, NewGlob.Stream+"setalerttarget")
	t.Cleanup(func() {
		DeleteTarget(t, NewGlob.QueryClient, targetID)
	})
	alertID := createTestAlert(t, stream, targetID, NewGlob.Stream+"setalerttitle")
	t.Cleanup(func() {
		DeleteAlert(t, NewGlob.QueryClient, alertID)
	})
}

func TestSmokeGetAlert(t *testing.T) {
	t.Parallel()
	stream := NewGlob.Stream + "getalert"
	title := NewGlob.Stream + "getalerttitle"
	CreateStream(t, NewGlob.QueryClient, stream)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.QueryClient, stream)
	})
	ingestAlertFixture(t, stream)
	time.Sleep(120 * time.Second)
	targetID := createTestTarget(t, NewGlob.Stream+"getalerttarget")
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

func TestSmokeSetRetention(t *testing.T) {
	t.Parallel()
	stream := NewGlob.Stream + "setretention"
	CreateStream(t, NewGlob.QueryClient, stream)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.QueryClient, stream)
	})
	req, _ := NewGlob.QueryClient.NewRequest("PUT", "logstream/"+stream+"/retention", strings.NewReader(RetentionBody))
	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func TestSmokeGetRetention(t *testing.T) {
	t.Parallel()
	stream := NewGlob.Stream + "getretention"
	CreateStream(t, NewGlob.QueryClient, stream)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.QueryClient, stream)
	})

	req, _ := NewGlob.QueryClient.NewRequest("PUT", "logstream/"+stream+"/retention", strings.NewReader(RetentionBody))
	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

	req, _ = NewGlob.QueryClient.NewRequest("GET", "logstream/"+stream+"/retention", nil)
	response, err = NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	require.JSONEq(t, RetentionBody, body, "Get retention response doesn't match with retention config returned")
}

// This test calls all the User API endpoints
// in a sequence to check if they work as expected.
func TestSmoke_AllUsersAPI(t *testing.T) {
	t.Parallel()
	rbacMu.Lock()
	defer rbacMu.Unlock()

	role := NewGlob.Stream + "allusersrole"
	user := NewGlob.Stream + "allusers"
	userWithRole := NewGlob.Stream + "alluserswithrole"
	CreateRole(t, NewGlob.QueryClient, role, dummyRole)
	AssertRole(t, NewGlob.QueryClient, role, dummyRole)

	CreateUser(t, NewGlob.QueryClient, user)
	CreateUserWithRole(t, NewGlob.QueryClient, userWithRole, []string{role})
	AssertUserRole(t, NewGlob.QueryClient, userWithRole, role, dummyRole)
	RegenPassword(t, NewGlob.QueryClient, user)
	DeleteUser(t, NewGlob.QueryClient, user)
	DeleteUser(t, NewGlob.QueryClient, userWithRole)
	DeleteRole(t, NewGlob.QueryClient, role)
}

// This test checks that a new user doesn't get any role by default
// even if a default role is set.
func TestSmoke_NewUserNoRole(t *testing.T) {
	t.Parallel()
	rbacMu.Lock()
	defer rbacMu.Unlock()

	stream := NewGlob.Stream + "newusernorole"
	role := NewGlob.Stream + "defaultrole"
	user := NewGlob.Stream + "newuser"
	CreateStream(t, NewGlob.QueryClient, stream)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.QueryClient, stream)
	})

	CreateRole(t, NewGlob.QueryClient, role, dummyRole)
	SetDefaultRole(t, NewGlob.QueryClient, role)
	AssertDefaultRole(t, NewGlob.QueryClient, fmt.Sprintf("%q", role))

	CreateUser(t, NewGlob.QueryClient, user)
}

func TestSmokeRbacBasic(t *testing.T) {
	t.Parallel()
	rbacMu.Lock()
	defer rbacMu.Unlock()

	stream := NewGlob.Stream + "rbacbasic"
	role := NewGlob.Stream + "rbacbasicrole"
	user := NewGlob.Stream + "rbacbasicuser"
	CreateStream(t, NewGlob.QueryClient, stream)
	CreateRole(t, NewGlob.QueryClient, role, dummyRole)
	AssertRole(t, NewGlob.QueryClient, role, dummyRole)
	CreateUserWithRole(t, NewGlob.QueryClient, user, []string{role})
	userClient := NewGlob.QueryClient
	userClient.Username = user
	userClient.Password = RegenPassword(t, NewGlob.QueryClient, user)
	checkAPIAccess(t, userClient, NewGlob.QueryClient, stream, "editor")
	DeleteUser(t, NewGlob.QueryClient, user)
	DeleteRole(t, NewGlob.QueryClient, role)
}

func TestSmokeRoles(t *testing.T) {
	t.Parallel()
	rbacMu.Lock()
	defer rbacMu.Unlock()

	stream := NewGlob.Stream + "roles"
	CreateStream(t, NewGlob.QueryClient, stream)
	cases := []struct {
		roleName string
		body     string
	}{
		{
			roleName: NewGlob.Stream + "ingestor",
			body:     Roleingestor(stream),
		},
		{
			roleName: NewGlob.Stream + "reader",
			body:     RoleReader(stream),
		},
		{
			roleName: NewGlob.Stream + "writer",
			body:     RoleWriter(stream),
		},
		{
			roleName: NewGlob.Stream + "editor",
			body:     RoleEditor,
		},
	}

	for _, tc := range cases {
		t.Run(tc.roleName, func(t *testing.T) {
			CreateRole(t, NewGlob.QueryClient, tc.roleName, tc.body)
			AssertRole(t, NewGlob.QueryClient, tc.roleName, tc.body)
			username := tc.roleName + "_user"
			password := CreateUserWithRole(t, NewGlob.QueryClient, username, []string{tc.roleName})
			var ingestClient HTTPClient
			queryClient := NewGlob.QueryClient
			queryClient.Username = username
			queryClient.Password = password
			if NewGlob.IngestorUrl.String() != "" {
				ingestClient = NewGlob.IngestorClient
				ingestClient.Username = username
				ingestClient.Password = password
			} else {
				ingestClient = NewGlob.QueryClient
				ingestClient.Username = username
				ingestClient.Password = password
			}

			roleKind := strings.TrimPrefix(tc.roleName, NewGlob.Stream)
			checkAPIAccess(t, queryClient, ingestClient, stream, roleKind)
			DeleteUser(t, NewGlob.QueryClient, username)
			DeleteRole(t, NewGlob.QueryClient, tc.roleName)
		})
	}
}

func TestLoadStreamBatchWithK6(t *testing.T) {
	if NewGlob.Mode == "load" {
		t.Parallel()

		stream := NewGlob.Stream + "loadbatch"
		CreateStream(t, NewGlob.QueryClient, stream)
		t.Cleanup(func() {
			DeleteStream(t, NewGlob.QueryClient, stream)
		})
		if NewGlob.IngestorUrl.String() == "" {
			cmd := exec.Command("k6",
				"run",
				"--address", "",
				"--vus", vus,
				"--duration", duration,
				"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", stream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
				"./scripts/load_batch_events.js")

			runK6Load(t, cmd)
		} else {
			cmd := exec.Command("k6",
				"run",
				"--address", "",
				"--vus", vus,
				"--duration", duration,
				"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", stream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
				"./scripts/load_batch_events.js")

			runK6Load(t, cmd)
		}
	}
}

// func TestLoadHistoricalStreamBatchWithK6(t *testing.T) {
// 	if NewGlob.Mode == "load" {
// 		historicalStream := NewGlob.Stream + "historical"
// 		timeHeader := map[string]string{"X-P-Time-Partition": "source_time"}
// 		CreateStreamWithHeader(t, NewGlob.QueryClient, historicalStream, timeHeader)
// 		if NewGlob.IngestorUrl.String() == "" {
// 			cmd := exec.Command("k6",
// 				"run",
// 				"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
// 				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
// 				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
// 				"-e", fmt.Sprintf("P_STREAM=%s", historicalStream),
// 				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
// 				"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
// 				"./scripts/load_historical_batch_events.js",
// 				"--vus=", vus,
// 				"--duration=", duration)

// 			cmd.Run()
// 			op, err := cmd.Output()
// 			if err != nil {
// 				t.Log(err)
// 			}
// 			t.Log(string(op))
// 		} else {
// 			cmd := exec.Command("k6",
// 				"run",
// 				"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
// 				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
// 				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
// 				"-e", fmt.Sprintf("P_STREAM=%s", historicalStream),
// 				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
// 				"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
// 				"./scripts/load_historical_batch_events.js",
// 				"--vus=", vus,
// 				"--duration=", duration)

// 			cmd.Run()
// 			op, err := cmd.Output()
// 			if err != nil {
// 				t.Log(err)
// 			}
// 			t.Log(string(op))
// 		}

// 		DeleteStream(t, NewGlob.QueryClient, historicalStream)
// 	}
// }

func TestLoadStreamBatchWithCustomPartitionWithK6(t *testing.T) {
	if NewGlob.Mode != "load" {
		return
	}
	t.Parallel()

	customPartitionStream := NewGlob.Stream + "loadbatchcustompartition"
	customHeader := map[string]string{"X-P-Custom-Partition": "level"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, customPartitionStream, customHeader)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.QueryClient, customPartitionStream)
	})
	if NewGlob.IngestorUrl.String() == "" {
		cmd := exec.Command("k6",
			"run",
			"--address", "",
			"--vus", vus,
			"--duration", duration,
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", customPartitionStream),
			"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
			"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
			"./scripts/load_batch_events.js")

		runK6Load(t, cmd)
	} else {
		cmd := exec.Command("k6",
			"run",
			"--address", "",
			"--vus", vus,
			"--duration", duration,
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", customPartitionStream),
			"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
			"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
			"./scripts/load_batch_events.js")

		runK6Load(t, cmd)
	}
}

func TestLoadStreamNoBatchWithK6(t *testing.T) {
	if NewGlob.Mode == "load" {
		t.Parallel()

		stream := NewGlob.Stream + "loadsingle"
		CreateStream(t, NewGlob.QueryClient, stream)
		t.Cleanup(func() {
			DeleteStream(t, NewGlob.QueryClient, stream)
		})
		if NewGlob.IngestorUrl.String() == "" {
			cmd := exec.Command("k6",
				"run",
				"--address", "",
				"--vus", vus,
				"--duration", duration,
				"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", stream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"./scripts/load_single_event.js")

			runK6Load(t, cmd)
		} else {
			cmd := exec.Command("k6",
				"run",
				"--address", "",
				"--vus", vus,
				"--duration", duration,
				"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", stream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"./scripts/load_single_event.js")

			runK6Load(t, cmd)
		}

	}
}

// func TestLoadHistoricalStreamNoBatchWithK6(t *testing.T) {
// 	if NewGlob.Mode == "load" {
// 		historicalStream := NewGlob.Stream + "historical"
// 		timeHeader := map[string]string{"X-P-Time-Partition": "source_time"}
// 		CreateStreamWithHeader(t, NewGlob.QueryClient, historicalStream, timeHeader)
// 		if NewGlob.IngestorUrl.String() == "" {
// 			cmd := exec.Command("k6",
// 				"run",
// 				"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
// 				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
// 				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
// 				"-e", fmt.Sprintf("P_STREAM=%s", historicalStream),
// 				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
// 				"./scripts/load_single_events.js",
// 				"--vus=", vus,
// 				"--duration=", duration)

// 			cmd.Run()
// 			op, err := cmd.Output()
// 			if err != nil {
// 				t.Log(err)
// 			}
// 			t.Log(string(op))
// 		} else {
// 			cmd := exec.Command("k6",
// 				"run",
// 				"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
// 				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
// 				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
// 				"-e", fmt.Sprintf("P_STREAM=%s", historicalStream),
// 				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
// 				"./scripts/load_single_events.js",
// 				"--vus=", vus,
// 				"--duration=", duration)

// 			cmd.Run()
// 			op, err := cmd.Output()
// 			if err != nil {
// 				t.Log(err)
// 			}
// 			t.Log(string(op))
// 		}

// 		DeleteStream(t, NewGlob.QueryClient, historicalStream)
// 	}
// }

func TestLoadStreamNoBatchWithCustomPartitionWithK6(t *testing.T) {
	if NewGlob.Mode != "load" {
		return
	}
	t.Parallel()

	customPartitionStream := NewGlob.Stream + "loadsinglecustompartition"
	customHeader := map[string]string{"X-P-Custom-Partition": "level"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, customPartitionStream, customHeader)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.QueryClient, customPartitionStream)
	})
	if NewGlob.IngestorUrl.String() == "" {
		cmd := exec.Command("k6",
			"run",
			"--address", "",
			"--vus", vus,
			"--duration", duration,
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", customPartitionStream),
			"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
			"./scripts/load_single_event.js")

		runK6Load(t, cmd)
	} else {
		cmd := exec.Command("k6",
			"run",
			"--address", "",
			"--vus", vus,
			"--duration", duration,
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", customPartitionStream),
			"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
			"./scripts/load_single_event.js")

		runK6Load(t, cmd)
	}
}
