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
	"io"
	"os/exec"
	"strings"
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

func TestSmokeListLogStream(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	stream := uniqueStream(t)
	CreateStream(t, client, stream)
	req, err := client.NewRequest("GET", "logstream", nil)
	require.NoErrorf(t, err, "Request failed: %s", err)

	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)

	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status)
	res, err := readJsonBody[[]string](bytes.NewBufferString(body))
	if err != nil {
		for _, s := range res {
			if s == stream {
				DeleteStream(t, client, stream)
			}
		}
	}
	DeleteStream(t, client, stream)
}

func TestSmokeCreateStream(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	stream := uniqueStream(t)
	CreateStream(t, client, stream)
	DeleteStream(t, client, stream)
}

func TestSmokeDetectSchema(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	DetectSchema(t, client, SampleJson, SchemaBody)
}

func TestSmokeIngestEventsToStream(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	ingestClient := testIngestClient(t)
	stream := uniqueStream(t)
	CreateStream(t, client, stream)
	RunFlog(t, ingestClient, stream)
	time.Sleep(120 * time.Second)

	QueryLogStreamCount(t, client, stream, 50)
	AssertStreamSchema(t, client, stream, FlogJsonSchema)
	DeleteStream(t, client, stream)
}

func TestLoadStream_StaticSchema_EventWithSameFields(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	ingestClient := testIngestClient(t)
	stream := uniqueStream(t)
	staticSchemaFlagHeader := map[string]string{"X-P-Static-Schema-Flag": "true"}
	CreateStreamWithSchemaBody(t, client, stream, staticSchemaFlagHeader, SchemaPayload)
	IngestOneEventForStaticSchemaStream_SameFieldsInLog(t, ingestClient, stream)
	DeleteStream(t, client, stream)
}

func TestLoadStreamBatchWithK6_StaticSchema(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	if NewGlob.Mode == "load" || NewGlob.Mode == "load-parallel" {
		stream := uniqueStream(t)
		staticSchemaFlagHeader := map[string]string{"X-P-Static-Schema-Flag": "true"}
		CreateStreamWithSchemaBody(t, client, stream, staticSchemaFlagHeader, SchemaPayload)
		if NewGlob.IngestorUrl.String() == "" {
			cmd := exec.Command("k6",
				"run",
				"-e", fmt.Sprintf("P_URL=%s", &NewGlob.QueryUrl),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", stream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
				"./scripts/load_batch_events.js",
				"--vus=", vus,
				"--duration=", duration)

			cmd.Run()
			op, err := cmd.Output()
			if err != nil {
				t.Log(err)
			}
			t.Log(string(op))
		} else {
			cmd := exec.Command("k6",
				"run",
				"-e", fmt.Sprintf("P_URL=%s", &NewGlob.IngestorUrl),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", stream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
				"./scripts/load_batch_events.js",
				"--vus=", vus,
				"--duration=", duration)

			cmd.Run()
			op, err := cmd.Output()
			if err != nil {
				t.Log(err)
			}
			t.Log(string(op))
		}

		DeleteStream(t, client, stream)
	}
}

func TestLoadStream_StaticSchema_EventWithNewField(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	ingestClient := testIngestClient(t)
	stream := uniqueStream(t)
	staticSchemaFlagHeader := map[string]string{"X-P-Static-Schema-Flag": "true"}
	CreateStreamWithSchemaBody(t, client, stream, staticSchemaFlagHeader, SchemaPayload)
	IngestOneEventForStaticSchemaStream_NewFieldInLog(t, ingestClient, stream)
	DeleteStream(t, client, stream)
}

func TestCreateStream_WithCustomPartition_Success(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	stream := uniqueStream(t)
	customHeader := map[string]string{"X-P-Custom-Partition": "level"}
	CreateStreamWithHeader(t, client, stream, customHeader)
	DeleteStream(t, client, stream)
}

func TestCreateStream_WithCustomPartition_Error(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	stream := uniqueStream(t)
	customHeader := map[string]string{"X-P-Custom-Partition": "level,os"}
	CreateStreamWithCustompartitionError(t, client, stream, customHeader)
}

func TestSmokeQueryTwoStreams(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	ingestClient := testIngestClient(t)
	stream := uniqueStream(t)
	stream1 := stream + "1"
	stream2 := stream + "2"
	CreateStream(t, client, stream1)
	CreateStream(t, client, stream2)
	RunFlog(t, ingestClient, stream1)
	RunFlog(t, ingestClient, stream2)
	time.Sleep(120 * time.Second)
	QueryTwoLogStreamCount(t, client, stream1, stream2, 100)
	DeleteStream(t, client, stream1)
	DeleteStream(t, client, stream2)
}

func TestSmokeRunQueries(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	ingestClient := testIngestClient(t)
	stream := uniqueStream(t)
	CreateStream(t, client, stream)
	RunFlog(t, ingestClient, stream)
	time.Sleep(120 * time.Second)
	// test count
	QueryLogStreamCount(t, client, stream, 50)
	// test yeild all values
	AssertQueryOK(t, client, "SELECT * FROM %s", stream)
	AssertQueryOK(t, client, "SELECT * FROM %s OFFSET 25 LIMIT 25", stream)
	// test fetch single column
	for _, item := range flogStreamFields() {
		AssertQueryOK(t, client, "SELECT %s FROM %s", item, stream)
	}
	// test basic filter
	AssertQueryOK(t, client, "SELECT * FROM %s WHERE method = 'POST'", stream)
	// test group by
	AssertQueryOK(t, client, "SELECT method, COUNT(*) FROM %s GROUP BY method", stream)
	AssertQueryOK(t, client, `SELECT DATE_TRUNC('minute', p_timestamp) as minute, COUNT(*) FROM %s GROUP BY minute`, stream)

	DeleteStream(t, client, stream)
}

func TestSmokeLoadWithK6Stream(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	stream := uniqueStream(t)
	CreateStream(t, client, stream)
	if NewGlob.IngestorUrl.String() == "" {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", stream),
			"./scripts/smoke.js")

		cmd.Run()
		cmd.Output()
	} else {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", stream),
			"./scripts/smoke.js")

		cmd.Run()
		cmd.Output()
	}
	time.Sleep(150 * time.Second)
	QueryLogStreamCount(t, client, stream, 20000)
	AssertStreamSchema(t, client, stream, SchemaBody)
	DeleteStream(t, client, stream)
}

func TestSmokeLoad_CustomPartition_WithK6Stream(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	stream := uniqueStream(t)
	customHeader := map[string]string{"X-P-Custom-Partition": "level"}
	CreateStreamWithHeader(t, client, stream, customHeader)
	if NewGlob.IngestorUrl.String() == "" {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", stream),
			"./scripts/smoke.js")

		cmd.Run()
		cmd.Output()
	} else {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", stream),
			"./scripts/smoke.js")

		cmd.Run()
		cmd.Output()
	}
	time.Sleep(120 * time.Second)
	QueryLogStreamCount(t, client, stream, 20000)
	DeleteStream(t, client, stream)
}

func TestSmokeSetTarget(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	targetName := uniqueName(t, "tgt")
	body := getTargetBody(targetName)
	req, _ := client.NewRequest("POST", "/targets", strings.NewReader(body))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

	// Cleanup: get target ID and delete
	req, _ = client.NewRequest("GET", "/targets", nil)
	response, err = client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	bodyTargets, _ := io.ReadAll(response.Body)
	targetId := getIdFromTargetResponse(bytes.NewReader(bodyTargets))
	DeleteTarget(t, client, targetId)
}

// TestSmokeAlertLifecycle creates a target, sets an alert, verifies it, then cleans up.
func TestSmokeAlertLifecycle(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	stream := uniqueStream(t)
	CreateStream(t, client, stream)

	// Create target
	targetName := uniqueName(t, "tgt")
	alertTitle := uniqueName(t, "alert")
	targetBody := getTargetBody(targetName)
	req, _ := client.NewRequest("POST", "/targets", strings.NewReader(targetBody))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

	// Ingest some data
	if NewGlob.IngestorUrl.String() == "" {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", stream),
			"./scripts/smoke.js")
		cmd.Run()
		cmd.Output()
	} else {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", stream),
			"./scripts/smoke.js")
		cmd.Run()
		cmd.Output()
	}
	time.Sleep(120 * time.Second)

	// Get target ID
	req, _ = client.NewRequest("GET", "/targets", nil)
	response, err = client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	bodyTargets, _ := io.ReadAll(response.Body)
	targetId := getIdFromTargetResponse(bytes.NewReader(bodyTargets))

	// Set alert
	alertBody := getAlertBody(stream, targetId, alertTitle)
	req, _ = client.NewRequest("POST", "/alerts", strings.NewReader(alertBody))
	response, err = client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

	// Get and verify alert
	req, _ = client.NewRequest("GET", "/alerts", nil)
	response, err = client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body, _ := io.ReadAll(response.Body)
	reader1 := bytes.NewReader(body)
	reader2 := bytes.NewReader(body)
	expected := readAsString(reader1)
	id, state, created, datasets := getMetadataFromAlertResponse(reader2)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	res := createAlertResponse(alertTitle, id, state, created, datasets)
	require.JSONEq(t, expected, res, "Get alert response doesn't match with Alert config returned")

	// Cleanup
	DeleteAlert(t, client, id)
	DeleteTarget(t, client, targetId)
	DeleteStream(t, client, stream)
}

func TestSmokeSetRetention(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	stream := uniqueStream(t)
	CreateStream(t, client, stream)
	req, _ := client.NewRequest("PUT", "logstream/"+stream+"/retention", strings.NewReader(RetentionBody))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
	DeleteStream(t, client, stream)
}

func TestSmokeGetRetention(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	stream := uniqueStream(t)
	CreateStream(t, client, stream)
	req, _ := client.NewRequest("PUT", "logstream/"+stream+"/retention", strings.NewReader(RetentionBody))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

	req, _ = client.NewRequest("GET", "logstream/"+stream+"/retention", nil)
	response, err = client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	require.JSONEq(t, RetentionBody, body, "Get retention response doesn't match with retention config returned")
	DeleteStream(t, client, stream)
}

// This test calls all the User API endpoints
// in a sequence to check if they work as expected.
func TestSmoke_AllUsersAPI(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	stream := uniqueStream(t)
	roleName := uniqueName(t, "role")
	user1 := uniqueName(t, "user")
	user2 := uniqueName(t, "user2")
	roleBody := dummyRoleBody(stream)

	CreateStream(t, client, stream)
	CreateRole(t, client, roleName, roleBody)
	actualRoleBody := GetRole(t, client, roleName)

	CreateUser(t, client, user1)
	CreateUserWithRole(t, client, user2, []string{roleName})
	AssertUserRole(t, client, user2, roleName, actualRoleBody)
	RegenPassword(t, client, user1)
	DeleteUser(t, client, user1)
	DeleteUser(t, client, user2)
	DeleteRole(t, client, roleName)
	DeleteStream(t, client, stream)
}

// This test checks that a new user doesn't get any role by default
// even if a default role is set.
func TestSmoke_NewUserNoRole(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	stream := uniqueStream(t)
	roleName := uniqueName(t, "role")
	userName := uniqueName(t, "user")
	roleBody := dummyRoleBody(stream)

	CreateStream(t, client, stream)
	CreateRole(t, client, roleName, roleBody)
	SetDefaultRole(t, client, roleName)
	AssertDefaultRole(t, client, fmt.Sprintf("%q", roleName))

	CreateUser(t, client, userName)
	DeleteUser(t, client, userName)
	DeleteRole(t, client, roleName)
	DeleteStream(t, client, stream)
}

func TestSmokeRbacBasic(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	stream := uniqueStream(t)
	roleName := uniqueName(t, "role")
	userName := uniqueName(t, "user")
	roleBody := dummyRoleBody(stream)

	CreateStream(t, client, stream)
	CreateRole(t, client, roleName, roleBody)
	GetRole(t, client, roleName) // verify role was created
	CreateUserWithRole(t, client, userName, []string{roleName})
	userClient := NewClient(client)
	userClient.Username = userName
	userClient.Password = RegenPassword(t, client, userName)
	checkAPIAccess(t, userClient, client, stream, "editor")
	// Note: checkAPIAccess("editor") already deleted the stream as part of verifying editor access.
	DeleteUser(t, client, userName)
	DeleteRole(t, client, roleName)
}

func TestSmokeRoles(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	stream := uniqueStream(t)
	prefix := uniqueName(t, "")
	CreateStream(t, client, stream)
	cases := []struct {
		roleType string
		body     string
	}{
		{
			roleType: "ingestor",
			body:     Roleingestor(stream),
		},
		{
			roleType: "reader",
			body:     RoleReader(stream),
		},
		{
			roleType: "writer",
			body:     RoleWriter(stream),
		},
		{
			roleType: "editor",
			body:     RoleEditor,
		},
	}

	for _, tc := range cases {
		t.Run(tc.roleType, func(t *testing.T) {
			roleName := prefix + tc.roleType
			username := prefix + tc.roleType + "user"
			CreateRole(t, client, roleName, tc.body)
			GetRole(t, client, roleName) // verify role was created
			password := CreateUserWithRole(t, client, username, []string{roleName})
			queryClient := NewClient(client)
			queryClient.Username = username
			queryClient.Password = password
			var ingestClient HTTPClient
			if NewGlob.IngestorUrl.String() != "" {
				ingestClient = testIngestClient(t)
				ingestClient.Username = username
				ingestClient.Password = password
			} else {
				ingestClient = NewClient(client)
				ingestClient.Username = username
				ingestClient.Password = password
			}

			checkAPIAccess(t, queryClient, ingestClient, stream, tc.roleType)
			DeleteUser(t, client, username)
			DeleteRole(t, client, roleName)
		})
	}
	// Note: checkAPIAccess("editor") already deleted the stream as part of verifying editor access.
}

func TestLoadStreamBatchWithK6(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	if NewGlob.Mode == "load" || NewGlob.Mode == "load-parallel" {
		stream := uniqueStream(t)
		CreateStream(t, client, stream)
		if NewGlob.IngestorUrl.String() == "" {
			cmd := exec.Command("k6",
				"run",
				"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", stream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
				"./scripts/load_batch_events.js",
				"--vus=", vus,
				"--duration=", duration)

			cmd.Run()
			op, err := cmd.Output()
			if err != nil {
				t.Log(err)
			}
			t.Log(string(op))
		} else {
			cmd := exec.Command("k6",
				"run",
				"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", stream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
				"./scripts/load_batch_events.js",
				"--vus=", vus,
				"--duration=", duration)

			cmd.Run()
			op, err := cmd.Output()
			if err != nil {
				t.Log(err)
			}
			t.Log(string(op))
		}
		DeleteStream(t, client, stream)

	}
}

func TestLoadStreamBatchWithCustomPartitionWithK6(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	stream := uniqueStream(t)
	customHeader := map[string]string{"X-P-Custom-Partition": "level"}
	CreateStreamWithHeader(t, client, stream, customHeader)
	if NewGlob.IngestorUrl.String() == "" {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", stream),
			"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
			"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
			"./scripts/load_batch_events.js",
			"--vus=", vus,
			"--duration=", duration)

		cmd.Run()
		op, err := cmd.Output()
		if err != nil {
			t.Log(err)
		}
		t.Log(string(op))
	} else {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", stream),
			"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
			"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
			"./scripts/load_batch_events.js",
			"--vus=", vus,
			"--duration=", duration)

		cmd.Run()
		op, err := cmd.Output()
		if err != nil {
			t.Log(err)
		}
		t.Log(string(op))
	}

	DeleteStream(t, client, stream)
}

func TestLoadStreamNoBatchWithK6(t *testing.T) {
	t.Parallel()
	if NewGlob.Mode == "load" || NewGlob.Mode == "load-parallel" {
		client := testClient(t)
		stream := uniqueStream(t)
		CreateStream(t, client, stream)
		if NewGlob.IngestorUrl.String() == "" {
			cmd := exec.Command("k6",
				"run",
				"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", stream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"./scripts/load_single_events.js",
				"--vus=", vus,
				"--duration=", duration)

			cmd.Run()
			op, err := cmd.Output()
			if err != nil {
				t.Log(err)
			}
			t.Log(string(op))
		} else {
			cmd := exec.Command("k6",
				"run",
				"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", stream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"./scripts/load_single_events.js",
				"--vus=", vus,
				"--duration=", duration)

			cmd.Run()
			op, err := cmd.Output()
			if err != nil {
				t.Log(err)
			}
			t.Log(string(op))
		}

	}
}

func TestLoadStreamNoBatchWithCustomPartitionWithK6(t *testing.T) {
	t.Parallel()
	client := testClient(t)
	stream := uniqueStream(t)
	customHeader := map[string]string{"X-P-Custom-Partition": "level"}
	CreateStreamWithHeader(t, client, stream, customHeader)
	if NewGlob.IngestorUrl.String() == "" {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", stream),
			"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
			"./scripts/load_single_events.js",
			"--vus=", vus,
			"--duration=", duration)

		cmd.Run()
		op, err := cmd.Output()
		if err != nil {
			t.Log(err)
		}
		t.Log(string(op))
	} else {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", stream),
			"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
			"./scripts/load_single_events.js",
			"--vus=", vus,
			"--duration=", duration)

		cmd.Run()
		op, err := cmd.Output()
		if err != nil {
			t.Log(err)
		}
		t.Log(string(op))
	}

	DeleteStream(t, client, stream)
}
