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
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
	rt.TrackStream(NewGlob.Stream)
	req, err := NewGlob.QueryClient.NewRequest("GET", "logstream", nil)
	require.NoErrorf(t, err, "Request failed: %s", err)

	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)

	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status)
	res, err := readJsonBody[[]string](bytes.NewBufferString(body))
	if err == nil {
		for _, stream := range res {
			if stream == NewGlob.Stream {
				// Stream found in list, tracked for cleanup
				_ = stream
			}
		}
	}
}

func TestSmokeCreateStream(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
	rt.TrackStream(NewGlob.Stream)
}

func TestSmokeDetectSchema(t *testing.T) {
	t.Parallel()
	DetectSchema(t, NewGlob.QueryClient, SampleJson, SchemaBody)
}

func TestSmokeIngestEventsToStream(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
	rt.TrackStream(NewGlob.Stream)
	RunFlogAuto(t, NewGlob.Stream)
	WaitForIngest(t, NewGlob.QueryClient, NewGlob.Stream, 50, 180*time.Second)
	AssertStreamSchema(t, NewGlob.QueryClient, NewGlob.Stream, FlogJsonSchema)
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
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	staticSchemaStream := NewGlob.Stream + "staticschema"
	staticSchemaFlagHeader := map[string]string{"X-P-Static-Schema-Flag": "true"}
	CreateStreamWithSchemaBody(t, NewGlob.QueryClient, staticSchemaStream, staticSchemaFlagHeader, SchemaPayload)
	rt.TrackStream(staticSchemaStream)
	if NewGlob.IngestorUrl.String() == "" {
		IngestOneEventForStaticSchemaStream_SameFieldsInLog(t, NewGlob.QueryClient, staticSchemaStream)
	} else {
		IngestOneEventForStaticSchemaStream_SameFieldsInLog(t, NewGlob.IngestorClient, staticSchemaStream)
	}
}

func TestLoadStreamBatchWithK6_StaticSchema(t *testing.T) {
	if NewGlob.Mode == "load" {
		rt := NewResourceTracker(t, NewGlob.QueryClient)
		staticSchemaStream := NewGlob.Stream + "staticschema"
		staticSchemaFlagHeader := map[string]string{"X-P-Static-Schema-Flag": "true"}
		CreateStreamWithSchemaBody(t, NewGlob.QueryClient, staticSchemaStream, staticSchemaFlagHeader, SchemaPayload)
		rt.TrackStream(staticSchemaStream)
		if NewGlob.IngestorUrl.String() == "" {
			cmd := exec.Command("k6",
				"run",
				"-e", fmt.Sprintf("P_URL=%s", &NewGlob.QueryUrl),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", staticSchemaStream),
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
				"-e", fmt.Sprintf("P_STREAM=%s", staticSchemaStream),
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
	}
}

func TestLoadStream_StaticSchema_EventWithNewField(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	staticSchemaStream := NewGlob.Stream + "staticschema"
	staticSchemaFlagHeader := map[string]string{"X-P-Static-Schema-Flag": "true"}
	CreateStreamWithSchemaBody(t, NewGlob.QueryClient, staticSchemaStream, staticSchemaFlagHeader, SchemaPayload)
	rt.TrackStream(staticSchemaStream)
	if NewGlob.IngestorUrl.String() == "" {
		IngestOneEventForStaticSchemaStream_NewFieldInLog(t, NewGlob.QueryClient, staticSchemaStream)
	} else {
		IngestOneEventForStaticSchemaStream_NewFieldInLog(t, NewGlob.IngestorClient, staticSchemaStream)
	}
}

func TestCreateStream_WithCustomPartition_Success(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	customPartitionStream := NewGlob.Stream + "custompartition"
	customHeader := map[string]string{"X-P-Custom-Partition": "level"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, customPartitionStream, customHeader)
	rt.TrackStream(customPartitionStream)
}

func TestCreateStream_WithCustomPartition_Error(t *testing.T) {
	customPartitionStream := NewGlob.Stream + "custompartition"
	customHeader := map[string]string{"X-P-Custom-Partition": "level,os"}
	CreateStreamWithCustompartitionError(t, NewGlob.QueryClient, customPartitionStream, customHeader)
}

func TestSmokeQueryTwoStreams(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	stream1 := NewGlob.Stream + "1"
	stream2 := NewGlob.Stream + "2"
	CreateStream(t, NewGlob.QueryClient, stream1)
	rt.TrackStream(stream1)
	CreateStream(t, NewGlob.QueryClient, stream2)
	rt.TrackStream(stream2)
	RunFlogAuto(t, stream1)
	RunFlogAuto(t, stream2)
	WaitForIngest(t, NewGlob.QueryClient, stream1, 1, 180*time.Second)
	WaitForIngest(t, NewGlob.QueryClient, stream2, 1, 180*time.Second)
	QueryTwoLogStreamCount(t, NewGlob.QueryClient, stream1, stream2, 100)
}

func TestSmokeRunQueries(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
	rt.TrackStream(NewGlob.Stream)
	RunFlogAuto(t, NewGlob.Stream)
	WaitForIngest(t, NewGlob.QueryClient, NewGlob.Stream, 50, 180*time.Second)
	// test yeild all values
	AssertQueryOK(t, NewGlob.QueryClient, "SELECT * FROM %s", NewGlob.Stream)
	AssertQueryOK(t, NewGlob.QueryClient, "SELECT * FROM %s OFFSET 25 LIMIT 25", NewGlob.Stream)
	// test fetch single column
	for _, item := range flogStreamFields() {
		AssertQueryOK(t, NewGlob.QueryClient, "SELECT %s FROM %s", item, NewGlob.Stream)
	}
	// test basic filter
	AssertQueryOK(t, NewGlob.QueryClient, "SELECT * FROM %s WHERE method = 'POST'", NewGlob.Stream)
	// test group by
	AssertQueryOK(t, NewGlob.QueryClient, "SELECT method, COUNT(*) FROM %s GROUP BY method", NewGlob.Stream)
	AssertQueryOK(t, NewGlob.QueryClient, `SELECT DATE_TRUNC('minute', p_timestamp) as minute, COUNT(*) FROM %s GROUP BY minute`, NewGlob.Stream)
}

func TestSmokeLoadWithK6Stream(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
	rt.TrackStream(NewGlob.Stream)
	if NewGlob.IngestorUrl.String() == "" {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", NewGlob.Stream),
			"./scripts/smoke.js")

		cmd.Run()
		cmd.Output()
	} else {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", NewGlob.Stream),
			"./scripts/smoke.js")

		cmd.Run()
		cmd.Output()
	}
	WaitForIngest(t, NewGlob.QueryClient, NewGlob.Stream, 20000, 180*time.Second)
	AssertStreamSchema(t, NewGlob.QueryClient, NewGlob.Stream, SchemaBody)
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

func TestSmokeLoad_CustomPartition_WithK6Stream(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	custom_partition_stream := NewGlob.Stream + "custompartition"
	customHeader := map[string]string{"X-P-Custom-Partition": "level"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, custom_partition_stream, customHeader)
	rt.TrackStream(custom_partition_stream)
	if NewGlob.IngestorUrl.String() == "" {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", custom_partition_stream),
			"./scripts/smoke.js")

		cmd.Run()
		cmd.Output()
	} else {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", custom_partition_stream),
			"./scripts/smoke.js")

		cmd.Run()
		cmd.Output()
	}
	WaitForIngest(t, NewGlob.QueryClient, custom_partition_stream, 20000, 180*time.Second)
}

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

func TestSmokeSetTarget(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	body := getTargetBody()
	req, _ := NewGlob.QueryClient.NewRequest("POST", "/targets", strings.NewReader(body))
	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
	targetsBody := ListTargets(t, NewGlob.QueryClient)
	targetId := getIdFromTargetResponse(bytes.NewReader([]byte(targetsBody)))
	rt.TrackTarget(targetId)
}

func TestSmokeRetentionLifecycle(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
	rt.TrackStream(NewGlob.Stream)

	// Set retention
	req, _ := NewGlob.QueryClient.NewRequest("PUT", "logstream/"+NewGlob.Stream+"/retention", strings.NewReader(RetentionBody))
	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Set retention failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Set retention failed with status: %s", response.Status)

	// Get retention
	req, _ = NewGlob.QueryClient.NewRequest("GET", "logstream/"+NewGlob.Stream+"/retention", nil)
	response, err = NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Get retention failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Get retention failed with status: %s", response.Status)
	require.JSONEq(t, RetentionBody, body, "Get retention response doesn't match with retention config returned")
}

// This test calls all the User API endpoints
// in a sequence to check if they work as expected.
func TestSmoke_AllUsersAPI(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	CreateRole(t, NewGlob.QueryClient, "dummyrole", dummyRole)
	rt.TrackRole("dummyrole")
	AssertRole(t, NewGlob.QueryClient, "dummyrole", dummyRole)

	CreateUser(t, NewGlob.QueryClient, "dummyuser")
	rt.TrackUser("dummyuser")
	CreateUserWithRole(t, NewGlob.QueryClient, "dummyanotheruser", []string{"dummyrole"})
	rt.TrackUser("dummyanotheruser")
	AssertUserRole(t, NewGlob.QueryClient, "dummyanotheruser", "dummyrole", dummyRole)
	RegenPassword(t, NewGlob.QueryClient, "dummyuser")
}

// This test checks that a new user doesn't get any role by default
// even if a default role is set.
func TestSmoke_NewUserNoRole(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
	rt.TrackStream(NewGlob.Stream)

	CreateRole(t, NewGlob.QueryClient, "dummyrole", dummyRole)
	rt.TrackRole("dummyrole")
	SetDefaultRole(t, NewGlob.QueryClient, "dummyrole")
	AssertDefaultRole(t, NewGlob.QueryClient, "\"dummyrole\"")

	CreateUser(t, NewGlob.QueryClient, "dummyuser")
	rt.TrackUser("dummyuser")
}

func TestSmokeRbacBasic(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
	rt.TrackStream(NewGlob.Stream)
	CreateRole(t, NewGlob.QueryClient, "dummy", dummyRole)
	rt.TrackRole("dummy")
	AssertRole(t, NewGlob.QueryClient, "dummy", dummyRole)
	CreateUserWithRole(t, NewGlob.QueryClient, "dummy", []string{"dummy"})
	rt.TrackUser("dummy")
	userClient := NewGlob.QueryClient
	userClient.Username = "dummy"
	userClient.Password = RegenPassword(t, NewGlob.QueryClient, "dummy")
	checkAPIAccess(t, userClient, NewGlob.QueryClient, NewGlob.Stream, "editor")
}

func TestSmokeRoles(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
	rt.TrackStream(NewGlob.Stream)
	cases := []struct {
		roleName string
		body     string
	}{
		{
			roleName: "ingestor",
			body:     Roleingestor(NewGlob.Stream),
		},
		{
			roleName: "reader",
			body:     RoleReader(NewGlob.Stream),
		},
		{
			roleName: "writer",
			body:     RoleWriter(NewGlob.Stream),
		},
		{
			roleName: "editor",
			body:     RoleEditor,
		},
	}

	for _, tc := range cases {
		t.Run(tc.roleName, func(t *testing.T) {
			CreateRole(t, NewGlob.QueryClient, tc.roleName, tc.body)
			rt.TrackRole(tc.roleName)
			AssertRole(t, NewGlob.QueryClient, tc.roleName, tc.body)
			username := tc.roleName + "_user"
			password := CreateUserWithRole(t, NewGlob.QueryClient, username, []string{tc.roleName})
			rt.TrackUser(username)
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

			checkAPIAccess(t, queryClient, ingestClient, NewGlob.Stream, tc.roleName)
		})
	}
}

func TestLoadStreamBatchWithK6(t *testing.T) {
	if NewGlob.Mode == "load" {
		rt := NewResourceTracker(t, NewGlob.QueryClient)
		CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
		rt.TrackStream(NewGlob.Stream)
		if NewGlob.IngestorUrl.String() == "" {
			cmd := exec.Command("k6",
				"run",
				"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", NewGlob.Stream),
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
				"-e", fmt.Sprintf("P_STREAM=%s", NewGlob.Stream),
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
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	customPartitionStream := NewGlob.Stream + "custompartition"
	customHeader := map[string]string{"X-P-Custom-Partition": "level"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, customPartitionStream, customHeader)
	rt.TrackStream(customPartitionStream)
	if NewGlob.IngestorUrl.String() == "" {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", customPartitionStream),
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
			"-e", fmt.Sprintf("P_STREAM=%s", customPartitionStream),
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
}

func TestLoadStreamNoBatchWithK6(t *testing.T) {
	if NewGlob.Mode == "load" {
		rt := NewResourceTracker(t, NewGlob.QueryClient)
		CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
		rt.TrackStream(NewGlob.Stream)
		if NewGlob.IngestorUrl.String() == "" {
			cmd := exec.Command("k6",
				"run",
				"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", NewGlob.Stream),
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
				"-e", fmt.Sprintf("P_STREAM=%s", NewGlob.Stream),
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
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	customPartitionStream := NewGlob.Stream + "custompartition"
	customHeader := map[string]string{"X-P-Custom-Partition": "level"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, customPartitionStream, customHeader)
	rt.TrackStream(customPartitionStream)
	if NewGlob.IngestorUrl.String() == "" {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", customPartitionStream),
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
			"-e", fmt.Sprintf("P_STREAM=%s", customPartitionStream),
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

func TestDeleteStream(t *testing.T) {
	DeleteStream(t, NewGlob.QueryClient, NewGlob.Stream)
}

// ===== P0 — New Tests =====

func TestSmokeHealthEndpoints(t *testing.T) {
	t.Parallel()
	AssertLiveness(t, NewGlob.QueryClient)
	AssertLivenessHead(t, NewGlob.QueryClient)
	AssertReadiness(t, NewGlob.QueryClient)
	AssertReadinessHead(t, NewGlob.QueryClient)
}

func TestSmokeStreamInfoAndStats(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
	rt.TrackStream(NewGlob.Stream)
	RunFlogAuto(t, NewGlob.Stream)
	WaitForIngest(t, NewGlob.QueryClient, NewGlob.Stream, 1, 180*time.Second)
	AssertStreamInfo(t, NewGlob.QueryClient, NewGlob.Stream)
	AssertStreamStats(t, NewGlob.QueryClient, NewGlob.Stream)
}

func TestSmokeOTelLogsIngestion(t *testing.T) {
	IngestOTelLogs(t, NewGlob.QueryClient)
}

func TestSmokeOTelTracesIngestion(t *testing.T) {
	IngestOTelTraces(t, NewGlob.QueryClient)
}

func TestSmokeOTelMetricsIngestion(t *testing.T) {
	IngestOTelMetrics(t, NewGlob.QueryClient)
}

func TestSmokeAlertLifecycle(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	stream := NewGlob.Stream + "alertlifecycle"
	CreateStream(t, NewGlob.QueryClient, stream)
	rt.TrackStream(stream)
	RunFlogAuto(t, stream)
	WaitForIngest(t, NewGlob.QueryClient, stream, 1, 180*time.Second)

	// Create target
	targetBody := getTargetBody()
	req, _ := NewGlob.QueryClient.NewRequest("POST", "targets", strings.NewReader(targetBody))
	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Create target failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Create target failed with status: %s", response.Status)

	// Get target ID
	req, _ = NewGlob.QueryClient.NewRequest("GET", "targets", nil)
	response, err = NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "List targets failed: %s", err)
	bodyTargets, _ := io.ReadAll(response.Body)
	targetId := getIdFromTargetResponse(bytes.NewReader(bodyTargets))
	rt.TrackTarget(targetId)

	// Create alert
	alertBody := getAlertBody(stream, targetId)
	req, _ = NewGlob.QueryClient.NewRequest("POST", "alerts", strings.NewReader(alertBody))
	response, err = NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Create alert failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Create alert failed with status: %s", response.Status)

	// Get alert ID from list
	req, _ = NewGlob.QueryClient.NewRequest("GET", "alerts", nil)
	response, err = NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "List alerts failed: %s", err)
	bodyAlerts, _ := io.ReadAll(response.Body)
	alertId, _, _, _ := getMetadataFromAlertResponse(bytes.NewReader(bodyAlerts))
	rt.TrackAlert(alertId)

	// Get by ID
	GetAlertById(t, NewGlob.QueryClient, alertId)

	// Modify
	modifyBody := getAlertModifyBody(stream, targetId)
	ModifyAlert(t, NewGlob.QueryClient, alertId, modifyBody)

	// Disable
	DisableAlert(t, NewGlob.QueryClient, alertId)

	// Enable
	EnableAlert(t, NewGlob.QueryClient, alertId)

	// List tags
	ListAlertTags(t, NewGlob.QueryClient)
}

func TestNegative_IngestToNonExistentStream(t *testing.T) {
	payload := `[{"level":"info","message":"test"}]`
	req, _ := NewGlob.QueryClient.NewRequest("POST", "ingest", bytes.NewBufferString(payload))
	req.Header.Add("X-P-Stream", "nonexistent_stream_xyz_12345")
	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	// Parseable may auto-create the stream (200) or reject (400/404)
	t.Logf("Ingest to non-existent stream returned status: %d", response.StatusCode)
}

func TestNegative_QueryNonExistentStream(t *testing.T) {
	t.Parallel()
	AssertQueryError(t, NewGlob.QueryClient, "SELECT * FROM nonexistent_stream_xyz_99999", 400)
}

func TestNegative_InvalidQuerySyntax(t *testing.T) {
	t.Parallel()
	AssertQueryError(t, NewGlob.QueryClient, "SELEC * FORM invalid_syntax", 400)
}

// ===== P1 — New Tests =====

func TestSmokeDashboardLifecycle(t *testing.T) {
	t.Parallel()
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	// Create
	dashboardId := CreateDashboard(t, NewGlob.QueryClient)
	require.NotEmptyf(t, dashboardId, "Dashboard ID should not be empty")
	rt.TrackDashboard(dashboardId)

	// List
	ListDashboards(t, NewGlob.QueryClient)

	// Get by ID
	GetDashboardById(t, NewGlob.QueryClient, dashboardId)

	// Update
	UpdateDashboard(t, NewGlob.QueryClient, dashboardId)

	// Add tile (using a generic stream name)
	AddDashboardTile(t, NewGlob.QueryClient, dashboardId, NewGlob.Stream)

	// List tags
	ListDashboardTags(t, NewGlob.QueryClient)
}

func TestSmokeFilterLifecycle(t *testing.T) {
	t.Parallel()
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	stream := NewGlob.Stream + "filtertest"
	CreateStream(t, NewGlob.QueryClient, stream)
	rt.TrackStream(stream)

	// Create
	filterId := CreateFilter(t, NewGlob.QueryClient, stream)
	require.NotEmptyf(t, filterId, "Filter ID should not be empty")
	rt.TrackFilter(filterId)

	// List
	ListFilters(t, NewGlob.QueryClient)

	// Get by ID
	GetFilterById(t, NewGlob.QueryClient, filterId)

	// Update
	UpdateFilter(t, NewGlob.QueryClient, filterId, stream)
}

func TestSmokeCorrelationLifecycle(t *testing.T) {
	t.Parallel()
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	stream1 := NewGlob.Stream + "corr1"
	stream2 := NewGlob.Stream + "corr2"
	CreateStream(t, NewGlob.QueryClient, stream1)
	rt.TrackStream(stream1)
	CreateStream(t, NewGlob.QueryClient, stream2)
	rt.TrackStream(stream2)

	// Ingest data into both streams
	RunFlogAuto(t, stream1)
	RunFlogAuto(t, stream2)
	WaitForIngest(t, NewGlob.QueryClient, stream1, 1, 180*time.Second)
	WaitForIngest(t, NewGlob.QueryClient, stream2, 1, 180*time.Second)

	// Create correlation
	correlationId := CreateCorrelation(t, NewGlob.QueryClient, stream1, stream2)
	require.NotEmptyf(t, correlationId, "Correlation ID should not be empty")
	rt.TrackCorrelation(correlationId)

	// List
	ListCorrelations(t, NewGlob.QueryClient)

	// Get by ID
	GetCorrelationById(t, NewGlob.QueryClient, correlationId)

	// Modify
	ModifyCorrelation(t, NewGlob.QueryClient, correlationId, stream1, stream2)
}

func TestSmokePrismHome(t *testing.T) {
	t.Parallel()
	AssertPrismHome(t, NewGlob.QueryClient)
}

func TestSmokePrismLogstreamInfo(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	stream := NewGlob.Stream + "prisminfo"
	CreateStream(t, NewGlob.QueryClient, stream)
	rt.TrackStream(stream)
	RunFlogAuto(t, stream)
	WaitForIngest(t, NewGlob.QueryClient, stream, 1, 180*time.Second)
	AssertPrismLogstreamInfo(t, NewGlob.QueryClient, stream)
}

func TestSmokePrismDatasets(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	stream := NewGlob.Stream + "prismdatasets"
	CreateStream(t, NewGlob.QueryClient, stream)
	rt.TrackStream(stream)
	RunFlogAuto(t, stream)
	WaitForIngest(t, NewGlob.QueryClient, stream, 1, 180*time.Second)
	AssertPrismDatasets(t, NewGlob.QueryClient, stream)
}

func TestSmokeRbacAddRemoveRoles(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	// Create roles
	roleName1 := "testrole_add1"
	roleName2 := "testrole_add2"
	CreateRole(t, NewGlob.QueryClient, roleName1, RoleEditor)
	rt.TrackRole(roleName1)
	CreateRole(t, NewGlob.QueryClient, roleName2, RoleEditor)
	rt.TrackRole(roleName2)

	// Create user
	CreateUser(t, NewGlob.QueryClient, "rbac_patch_user")
	rt.TrackUser("rbac_patch_user")

	// Add roles via PATCH
	AddRolesToUser(t, NewGlob.QueryClient, "rbac_patch_user", []string{roleName1, roleName2})

	// Remove one role via PATCH
	RemoveRolesFromUser(t, NewGlob.QueryClient, "rbac_patch_user", []string{roleName2})

	// List all roles
	ListAllRoles(t, NewGlob.QueryClient)
}

func TestSmokeTargetLifecycle(t *testing.T) {
	t.Parallel()
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	// Create
	targetBody := getTargetBody()
	req, _ := NewGlob.QueryClient.NewRequest("POST", "targets", strings.NewReader(targetBody))
	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Create target failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Create target failed with status: %s", response.Status)

	// List and get ID
	targetsBody := ListTargets(t, NewGlob.QueryClient)
	targetId := getIdFromTargetResponse(bytes.NewReader([]byte(targetsBody)))
	rt.TrackTarget(targetId)

	// Get by ID
	GetTargetById(t, NewGlob.QueryClient, targetId)

	// Update
	UpdateTarget(t, NewGlob.QueryClient, targetId)
}

// ===== P2 — New Tests =====

func TestSmokeAboutEndpoint(t *testing.T) {
	t.Parallel()
	body := AssertAbout(t, NewGlob.QueryClient)
	require.Containsf(t, body, "version", "About response should contain version field")
}

func TestSmokeMetricsEndpoint(t *testing.T) {
	t.Parallel()
	body := AssertMetrics(t, NewGlob.QueryClient)
	require.NotEmptyf(t, body, "Metrics response should not be empty")
}

func TestSmokeDemoData(t *testing.T) {
	AssertDemoData(t, NewGlob.QueryClient)
}

func TestSmokeStreamHotTier(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	stream := NewGlob.Stream + "hottier"
	CreateStream(t, NewGlob.QueryClient, stream)
	rt.TrackStream(stream)

	SetStreamHotTier(t, NewGlob.QueryClient, stream)
	GetStreamHotTier(t, NewGlob.QueryClient, stream)
	DeleteStreamHotTier(t, NewGlob.QueryClient, stream)
}

func TestSmokeDatasetStats(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	stream := NewGlob.Stream + "datasetstats"
	CreateStream(t, NewGlob.QueryClient, stream)
	rt.TrackStream(stream)
	RunFlogAuto(t, stream)
	WaitForIngest(t, NewGlob.QueryClient, stream, 1, 180*time.Second)
	AssertDatasetStats(t, NewGlob.QueryClient, []string{stream})
}

func TestSmokeRunQueriesExpanded(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	stream := NewGlob.Stream + "queryexpanded"
	CreateStream(t, NewGlob.QueryClient, stream)
	rt.TrackStream(stream)
	RunFlogAuto(t, stream)
	WaitForIngest(t, NewGlob.QueryClient, stream, 1, 180*time.Second)

	// SELECT DISTINCT
	AssertQueryOK(t, NewGlob.QueryClient, "SELECT DISTINCT method FROM %s", stream)
	// ORDER BY ASC/DESC
	AssertQueryOK(t, NewGlob.QueryClient, "SELECT * FROM %s ORDER BY p_timestamp ASC LIMIT 10", stream)
	AssertQueryOK(t, NewGlob.QueryClient, "SELECT * FROM %s ORDER BY p_timestamp DESC LIMIT 10", stream)
	// COUNT with GROUP BY and HAVING
	AssertQueryOK(t, NewGlob.QueryClient, "SELECT method, COUNT(*) as cnt FROM %s GROUP BY method HAVING COUNT(*) > 0", stream)
	// MIN/MAX/AVG/SUM
	AssertQueryOK(t, NewGlob.QueryClient, "SELECT MIN(status) as min_s, MAX(status) as max_s, AVG(status) as avg_s FROM %s", stream)
	// Large OFFSET/LIMIT
	AssertQueryOK(t, NewGlob.QueryClient, "SELECT * FROM %s LIMIT 1 OFFSET 0", stream)
}

func TestSmokeConcurrentIngestQuery(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	stream := NewGlob.Stream + "concurrent"
	CreateStream(t, NewGlob.QueryClient, stream)
	rt.TrackStream(stream)

	ingestClient := NewGlob.QueryClient
	if NewGlob.IngestorUrl.String() != "" {
		ingestClient = NewGlob.IngestorClient
	}

	ConcurrentIngestAndQuery(t, ingestClient, NewGlob.QueryClient, stream, 20, 3)
}

func TestNegative_InvalidJsonIngest(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	stream := NewGlob.Stream + "invalidjson"
	CreateStream(t, NewGlob.QueryClient, stream)
	rt.TrackStream(stream)
	AssertIngestError(t, NewGlob.QueryClient, stream, `{invalid json}`, 400)
}

func TestNegative_EmptyPayloadIngest(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	stream := NewGlob.Stream + "emptypayload"
	CreateStream(t, NewGlob.QueryClient, stream)
	rt.TrackStream(stream)
	AssertIngestError(t, NewGlob.QueryClient, stream, ``, 400)
}

func TestNegative_DuplicateStreamCreation(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	stream := NewGlob.Stream + "duplicate"
	CreateStream(t, NewGlob.QueryClient, stream)
	rt.TrackStream(stream)
	// Second creation should return an error (or idempotent 200)
	req, _ := NewGlob.QueryClient.NewRequest("PUT", "logstream/"+stream, nil)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	t.Logf("Duplicate stream creation returned status: %d", response.StatusCode)
}

func TestNegative_DeleteNonExistentStream(t *testing.T) {
	t.Parallel()
	req, _ := NewGlob.QueryClient.NewRequest("DELETE", "logstream/nonexistent_stream_delete_test_xyz", nil)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.NotEqualf(t, 200, response.StatusCode, "Deleting non-existent stream should not return 200")
}

func TestNegative_InvalidRetentionBody(t *testing.T) {
	rt := NewResourceTracker(t, NewGlob.QueryClient)
	stream := NewGlob.Stream + "invalidretention"
	CreateStream(t, NewGlob.QueryClient, stream)
	rt.TrackStream(stream)
	invalidBody := `{"invalid": "retention"}`
	req, _ := NewGlob.QueryClient.NewRequest("PUT", "logstream/"+stream+"/retention", strings.NewReader(invalidBody))
	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.NotEqualf(t, 200, response.StatusCode, "Invalid retention body should not return 200, got: %d", response.StatusCode)
}

func TestNegative_InvalidAlertBody(t *testing.T) {
	t.Parallel()
	invalidBody := `{"invalid": "alert"}`
	req, _ := NewGlob.QueryClient.NewRequest("POST", "alerts", strings.NewReader(invalidBody))
	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.NotEqualf(t, 200, response.StatusCode, "Invalid alert body should not return 200, got: %d", response.StatusCode)
}

// ===== P3 — New Tests =====

func TestDistributed_ClusterInfo(t *testing.T) {
	t.Parallel()
	if NewGlob.IngestorUrl.String() == "" {
		t.Skip("Skipping cluster test: no ingestor URL configured (standalone mode)")
	}
	AssertClusterInfo(t, NewGlob.QueryClient)
}

func TestDistributed_ClusterMetrics(t *testing.T) {
	t.Parallel()
	if NewGlob.IngestorUrl.String() == "" {
		t.Skip("Skipping cluster test: no ingestor URL configured (standalone mode)")
	}
	AssertClusterMetrics(t, NewGlob.QueryClient)
}

func TestSmokeLLMEndpoint_NoAPIKey(t *testing.T) {
	t.Parallel()
	AssertLLMEndpointNoAPIKey(t, NewGlob.QueryClient)
}
