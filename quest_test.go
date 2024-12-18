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
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
	req, err := NewGlob.QueryClient.NewRequest("GET", "logstream", nil)
	require.NoErrorf(t, err, "Request failed: %s", err)

	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)

	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status)
	res, err := readJsonBody[[]string](bytes.NewBufferString(body))
	if err != nil {
		for _, stream := range res {
			if stream == NewGlob.Stream {
				DeleteStream(t, NewGlob.QueryClient, NewGlob.Stream)
			}
		}
	}
	DeleteStream(t, NewGlob.QueryClient, NewGlob.Stream)
}

func TestSmokeCreateStream(t *testing.T) {
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
	DeleteStream(t, NewGlob.QueryClient, NewGlob.Stream)
}

func TestSmokeDetectSchema(t *testing.T) {
	DetectSchema(t, NewGlob.QueryClient, SampleJson, SchemaBody)
}

func TestSmokeIngestEventsToStream(t *testing.T) {
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
	if NewGlob.IngestorUrl.String() == "" {
		RunFlog(t, NewGlob.QueryClient, NewGlob.Stream)
	} else {
		RunFlog(t, NewGlob.IngestorClient, NewGlob.Stream)
		// Calling Sleep method
		time.Sleep(60 * time.Second)
	}

	QueryLogStreamCount(t, NewGlob.QueryClient, NewGlob.Stream, 50)
	AssertStreamSchema(t, NewGlob.QueryClient, NewGlob.Stream, FlogJsonSchema)
	DeleteStream(t, NewGlob.QueryClient, NewGlob.Stream)
}

func TestTimePartition_TimeStampMismatch(t *testing.T) {
	historicalStream := NewGlob.Stream + "historical"
	timeHeader := map[string]string{"X-P-Time-Partition": "source_time"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, historicalStream, timeHeader)
	if NewGlob.IngestorUrl.String() == "" {
		IngestOneEventWithTimePartition_TimeStampMismatch(t, NewGlob.QueryClient, historicalStream)
	} else {
		IngestOneEventWithTimePartition_TimeStampMismatch(t, NewGlob.IngestorClient, historicalStream)

	}
	DeleteStream(t, NewGlob.QueryClient, historicalStream)
}

func TestTimePartition_NoTimePartitionInLog(t *testing.T) {
	historicalStream := NewGlob.Stream + "historical"
	timeHeader := map[string]string{"X-P-Time-Partition": "source_time"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, historicalStream, timeHeader)
	if NewGlob.IngestorUrl.String() == "" {
		IngestOneEventWithTimePartition_NoTimePartitionInLog(t, NewGlob.QueryClient, historicalStream)
	} else {
		IngestOneEventWithTimePartition_NoTimePartitionInLog(t, NewGlob.IngestorClient, historicalStream)
	}
	DeleteStream(t, NewGlob.QueryClient, historicalStream)
}

func TestTimePartition_IncorrectDateTimeFormatTimePartitionInLog(t *testing.T) {
	historicalStream := NewGlob.Stream + "historical"
	timeHeader := map[string]string{"X-P-Time-Partition": "source_time"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, historicalStream, timeHeader)
	if NewGlob.IngestorUrl.String() == "" {
		IngestOneEventWithTimePartition_IncorrectDateTimeFormatTimePartitionInLog(t, NewGlob.QueryClient, historicalStream)
	} else {
		IngestOneEventWithTimePartition_IncorrectDateTimeFormatTimePartitionInLog(t, NewGlob.IngestorClient, historicalStream)
	}
	DeleteStream(t, NewGlob.QueryClient, historicalStream)
}

func TestLoadStream_StaticSchema_EventWithSameFields(t *testing.T) {
	staticSchemaStream := NewGlob.Stream + "staticschema"
	staticSchemaFlagHeader := map[string]string{"X-P-Static-Schema-Flag": "true"}
	CreateStreamWithSchemaBody(t, NewGlob.QueryClient, staticSchemaStream, staticSchemaFlagHeader, SchemaPayload)
	if NewGlob.IngestorUrl.String() == "" {
		IngestOneEventForStaticSchemaStream_SameFieldsInLog(t, NewGlob.QueryClient, staticSchemaStream)
	} else {
		IngestOneEventForStaticSchemaStream_SameFieldsInLog(t, NewGlob.IngestorClient, staticSchemaStream)
	}
	DeleteStream(t, NewGlob.QueryClient, staticSchemaStream)
}

func TestLoadStreamBatchWithK6_StaticSchema(t *testing.T) {
	if NewGlob.Mode == "load" {
		staticSchemaStream := NewGlob.Stream + "staticschema"
		staticSchemaFlagHeader := map[string]string{"X-P-Static-Schema-Flag": "true"}
		CreateStreamWithSchemaBody(t, NewGlob.QueryClient, staticSchemaStream, staticSchemaFlagHeader, SchemaPayload)
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

		DeleteStream(t, NewGlob.QueryClient, staticSchemaStream)
	}
}

func TestLoadStream_StaticSchema_EventWithNewField(t *testing.T) {
	staticSchemaStream := NewGlob.Stream + "staticschema"
	staticSchemaFlagHeader := map[string]string{"X-P-Static-Schema-Flag": "true"}
	CreateStreamWithSchemaBody(t, NewGlob.QueryClient, staticSchemaStream, staticSchemaFlagHeader, SchemaPayload)
	if NewGlob.IngestorUrl.String() == "" {
		IngestOneEventForStaticSchemaStream_NewFieldInLog(t, NewGlob.QueryClient, staticSchemaStream)
	} else {
		IngestOneEventForStaticSchemaStream_NewFieldInLog(t, NewGlob.IngestorClient, staticSchemaStream)
	}
	DeleteStream(t, NewGlob.QueryClient, staticSchemaStream)
}
func TestSmokeQueryTwoStreams(t *testing.T) {
	stream1 := NewGlob.Stream + "1"
	stream2 := NewGlob.Stream + "2"
	CreateStream(t, NewGlob.QueryClient, stream1)
	CreateStream(t, NewGlob.QueryClient, stream2)
	if NewGlob.IngestorUrl.String() == "" {
		RunFlog(t, NewGlob.QueryClient, stream1)
		RunFlog(t, NewGlob.QueryClient, stream2)
	} else {
		RunFlog(t, NewGlob.IngestorClient, stream1)
		RunFlog(t, NewGlob.IngestorClient, stream2)

	}
	time.Sleep(60 * time.Second)
	QueryTwoLogStreamCount(t, NewGlob.QueryClient, stream1, stream2, 100)
	DeleteStream(t, NewGlob.QueryClient, stream1)
	DeleteStream(t, NewGlob.QueryClient, stream2)
}

func TestSmokeRunQueries(t *testing.T) {
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
	if NewGlob.IngestorUrl.String() == "" {
		RunFlog(t, NewGlob.QueryClient, NewGlob.Stream)
	} else {
		RunFlog(t, NewGlob.IngestorClient, NewGlob.Stream)

	}
	time.Sleep(60 * time.Second)
	// test count
	QueryLogStreamCount(t, NewGlob.QueryClient, NewGlob.Stream, 50)
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

	DeleteStream(t, NewGlob.QueryClient, NewGlob.Stream)
}

func TestSmokeLoadWithK6Stream(t *testing.T) {
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
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
	time.Sleep(120 * time.Second)
	QueryLogStreamCount(t, NewGlob.QueryClient, NewGlob.Stream, 20000)
	AssertStreamSchema(t, NewGlob.QueryClient, NewGlob.Stream, SchemaBody)
	DeleteStream(t, NewGlob.QueryClient, NewGlob.Stream)
}

func TestSmokeLoad_TimePartition_WithK6Stream(t *testing.T) {
	time_partition_stream := NewGlob.Stream + "timepartition"
	timeHeader := map[string]string{"X-P-Time-Partition": "source_time", "X-P-Time-Partition-Limit": "365d"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, time_partition_stream, timeHeader)
	if NewGlob.IngestorUrl.String() == "" {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", time_partition_stream),
			"./scripts/smoke.js")

		cmd.Run()
		cmd.Output()
	} else {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", time_partition_stream),
			"./scripts/smoke.js")

		cmd.Run()
		cmd.Output()
	}
	time.Sleep(120 * time.Second)
	QueryLogStreamCount_Historical(t, NewGlob.QueryClient, time_partition_stream, 20000)
	DeleteStream(t, NewGlob.QueryClient, time_partition_stream)
}

func TestSmokeLoad_CustomPartition_WithK6Stream(t *testing.T) {
	custom_partition_stream := NewGlob.Stream + "custompartition"
	customHeader := map[string]string{"X-P-Custom-Partition": "level"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, custom_partition_stream, customHeader)
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
	time.Sleep(120 * time.Second)
	QueryLogStreamCount(t, NewGlob.QueryClient, custom_partition_stream, 20000)
	DeleteStream(t, NewGlob.QueryClient, custom_partition_stream)
}

func TestSmokeLoad_TimeAndCustomPartition_WithK6Stream(t *testing.T) {
	custom_partition_stream := NewGlob.Stream + "timecustompartition"
	customHeader := map[string]string{"X-P-Custom-Partition": "level", "X-P-Time-Partition": "source_time", "X-P-Time-Partition-Limit": "365d"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, custom_partition_stream, customHeader)
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
	time.Sleep(120 * time.Second)
	QueryLogStreamCount_Historical(t, NewGlob.QueryClient, custom_partition_stream, 20000)
	DeleteStream(t, NewGlob.QueryClient, custom_partition_stream)
}

func TestSmokeSetAlert(t *testing.T) {
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
	if NewGlob.IngestorUrl.String() == "" {
		RunFlog(t, NewGlob.QueryClient, NewGlob.Stream)
		req, _ := NewGlob.QueryClient.NewRequest("PUT", "logstream/"+NewGlob.Stream+"/alert", strings.NewReader(AlertBody))
		response, err := NewGlob.QueryClient.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

	}

}

func TestSmokeGetAlert(t *testing.T) {
	if NewGlob.IngestorUrl.String() == "" {
		req, _ := NewGlob.QueryClient.NewRequest("GET", "logstream/"+NewGlob.Stream+"/alert", nil)
		response, err := NewGlob.QueryClient.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		body := readAsString(response.Body)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
		require.JSONEq(t, AlertBody, body, "Get alert response doesn't match with Alert config returned")
	}
	DeleteStream(t, NewGlob.QueryClient, NewGlob.Stream)

}

func TestSmokeSetRetention(t *testing.T) {
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
	req, _ := NewGlob.QueryClient.NewRequest("PUT", "logstream/"+NewGlob.Stream+"/retention", strings.NewReader(RetentionBody))
	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

}

func TestSmokeGetRetention(t *testing.T) {
	req, _ := NewGlob.QueryClient.NewRequest("GET", "logstream/"+NewGlob.Stream+"/retention", nil)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	require.JSONEq(t, RetentionBody, body, "Get retention response doesn't match with retention config returned")
	DeleteStream(t, NewGlob.QueryClient, NewGlob.Stream)
}

// This test calls all the User API endpoints
// in a sequence to check if they work as expected.
func TestSmoke_AllUsersAPI(t *testing.T) {
	CreateRole(t, NewGlob.QueryClient, "dummyrole", dummyRole)
	AssertRole(t, NewGlob.QueryClient, "dummyrole", dummyRole)

	CreateUser(t, NewGlob.QueryClient, "dummyuser")
	CreateUserWithRole(t, NewGlob.QueryClient, "dummyuser", []string{"dummyrole"})
	AssertUserRole(t, NewGlob.QueryClient, "dummyuser", "dummyrole", dummyRole)
	RegenPassword(t, NewGlob.QueryClient, "dummyuser")
	DeleteUser(t, NewGlob.QueryClient, "dummyuser")

	DeleteRole(t, NewGlob.QueryClient, "dummyrole")
}

// This test checks that a new user doesn't get any role by default
// even if a default role is set.
func TestSmoke_NewUserNoRole(t *testing.T) {
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)

	CreateRole(t, NewGlob.QueryClient, "dummyrole", dummyRole)
	SetDefaultRole(t, NewGlob.QueryClient, "dummyrole")
	AssertDefaultRole(t, NewGlob.QueryClient, "\"dummyrole\"")

	CreateUser(t, NewGlob.QueryClient, "dummyuser")
	DeleteStream(t, NewGlob.QueryClient, NewGlob.Stream)

}

func TestSmokeRbacBasic(t *testing.T) {
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
	CreateRole(t, NewGlob.QueryClient, "dummy", dummyRole)
	AssertRole(t, NewGlob.QueryClient, "dummy", dummyRole)
	CreateUserWithRole(t, NewGlob.QueryClient, "dummy", []string{"dummy"})
	userClient := NewGlob.QueryClient
	userClient.Username = "dummy"
	userClient.Password = RegenPassword(t, NewGlob.QueryClient, "dummy")
	checkAPIAccess(t, userClient, NewGlob.QueryClient, NewGlob.Stream, "editor")
	DeleteUser(t, NewGlob.QueryClient, "dummy")
	DeleteRole(t, NewGlob.QueryClient, "dummy")
}

func TestSmokeRoles(t *testing.T) {
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
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

			checkAPIAccess(t, queryClient, ingestClient, NewGlob.Stream, tc.roleName)
			DeleteUser(t, NewGlob.QueryClient, username)
			DeleteRole(t, NewGlob.QueryClient, tc.roleName)
		})
	}

}

func TestLoadStreamBatchWithK6(t *testing.T) {
	if NewGlob.Mode == "load" {
		CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
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
		DeleteStream(t, NewGlob.QueryClient, NewGlob.Stream)

	}
}

func TestLoadHistoricalStreamBatchWithK6(t *testing.T) {
	if NewGlob.Mode == "load" {
		historicalStream := NewGlob.Stream + "historical"
		timeHeader := map[string]string{"X-P-Time-Partition": "source_time"}
		CreateStreamWithHeader(t, NewGlob.QueryClient, historicalStream, timeHeader)
		if NewGlob.IngestorUrl.String() == "" {
			cmd := exec.Command("k6",
				"run",
				"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", historicalStream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
				"./scripts/load_historical_batch_events.js",
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
				"-e", fmt.Sprintf("P_STREAM=%s", historicalStream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
				"./scripts/load_historical_batch_events.js",
				"--vus=", vus,
				"--duration=", duration)

			cmd.Run()
			op, err := cmd.Output()
			if err != nil {
				t.Log(err)
			}
			t.Log(string(op))
		}

		DeleteStream(t, NewGlob.QueryClient, historicalStream)
	}
}

func TestLoadStreamBatchWithCustomPartitionWithK6(t *testing.T) {
	customPartitionStream := NewGlob.Stream + "custompartition"
	customHeader := map[string]string{"X-P-Custom-Partition": "level,os"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, customPartitionStream, customHeader)
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

	DeleteStream(t, NewGlob.QueryClient, customPartitionStream)
}

func TestLoadStreamBatchWithTimeAndCustomPartitionWithK6(t *testing.T) {
	if NewGlob.Mode == "load" {
		customPartitionStream := NewGlob.Stream + "timeandcustompartition"
		customHeader := map[string]string{"X-P-Custom-Partition": "level,os", "X-P-Time-Partition": "source_time"}
		CreateStreamWithHeader(t, NewGlob.QueryClient, customPartitionStream, customHeader)
		if NewGlob.IngestorUrl.String() == "" {
			cmd := exec.Command("k6",
				"run",
				"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", customPartitionStream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
				"./scripts/load_historical_batch_events.js",
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
				"./scripts/load_historical_batch_events.js",
				"--vus=", vus,
				"--duration=", duration)

			cmd.Run()
			op, err := cmd.Output()
			if err != nil {
				t.Log(err)
			}
			t.Log(string(op))
		}

		DeleteStream(t, NewGlob.QueryClient, customPartitionStream)
	}
}

func TestLoadStreamNoBatchWithK6(t *testing.T) {
	if NewGlob.Mode == "load" {
		CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
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

func TestLoadHistoricalStreamNoBatchWithK6(t *testing.T) {
	if NewGlob.Mode == "load" {
		historicalStream := NewGlob.Stream + "historical"
		timeHeader := map[string]string{"X-P-Time-Partition": "source_time"}
		CreateStreamWithHeader(t, NewGlob.QueryClient, historicalStream, timeHeader)
		if NewGlob.IngestorUrl.String() == "" {
			cmd := exec.Command("k6",
				"run",
				"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", historicalStream),
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
				"-e", fmt.Sprintf("P_STREAM=%s", historicalStream),
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

		DeleteStream(t, NewGlob.QueryClient, historicalStream)
	}
}

func TestLoadStreamNoBatchWithCustomPartitionWithK6(t *testing.T) {
	customPartitionStream := NewGlob.Stream + "custompartition"
	customHeader := map[string]string{"X-P-Custom-Partition": "level,os"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, customPartitionStream, customHeader)
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

	DeleteStream(t, NewGlob.QueryClient, customPartitionStream)
}

func TestLoadStreamNoBatchWithTimeAndCustomPartitionWithK6(t *testing.T) {
	if NewGlob.Mode == "load" {
		customPartitionStream := NewGlob.Stream + "timeandcustompartition"
		customHeader := map[string]string{"X-P-Custom-Partition": "level,os", "X-P-Time-Partition": "source_time"}
		CreateStreamWithHeader(t, NewGlob.QueryClient, customPartitionStream, customHeader)
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

		DeleteStream(t, NewGlob.QueryClient, customPartitionStream)
	}
}

func TestDeleteStream(t *testing.T) {
	DeleteStream(t, NewGlob.QueryClient, NewGlob.Stream)
}
