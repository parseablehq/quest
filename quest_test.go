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

	"github.com/stretchr/testify/require"
)

const (
	vus          = "10"
	duration     = "5m"
	schema_count = "20"
	events_count = "10"
)

func TestSmokeListLogStream(t *testing.T) {
	req, err := NewGlob.Client.NewRequest("GET", "logstream", nil)
	require.NoErrorf(t, err, "Request failed: %s", err)

	response, err := NewGlob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)

	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status)
	res, err := readJsonBody[[]string](bytes.NewBufferString(body))
	if err != nil {
		for _, stream := range res {
			if stream == NewGlob.Stream {
				DeleteStream(t, NewGlob.Client, NewGlob.Stream)
			}
		}
	}
}

func TestSmokeCreateStream(t *testing.T) {
	CreateStream(t, NewGlob.Client, NewGlob.Stream)
}

func TestSmokeIngestEventsToStream(t *testing.T) {
	RunFlog(t, NewGlob.Stream)
	QueryLogStreamCount(t, NewGlob.Client, NewGlob.Stream, 50)
	AssertStreamSchema(t, NewGlob.Client, NewGlob.Stream, FlogJsonSchema)
	DeleteStream(t, NewGlob.Client, NewGlob.Stream)
}

func TestSmokeQueryTwoStreams(t *testing.T) {
	stream1 := NewGlob.Stream + "1"
	stream2 := NewGlob.Stream + "2"
	RunFlog(t, stream1)
	RunFlog(t, stream2)
	QueryTwoLogStreamCount(t, NewGlob.Client, stream1, stream2, 100)
	DeleteStream(t, NewGlob.Client, stream1)
	DeleteStream(t, NewGlob.Client, stream2)
}

func TestSmokeRunQueries(t *testing.T) {
	RunFlog(t, NewGlob.Stream)
	// test count
	QueryLogStreamCount(t, NewGlob.Client, NewGlob.Stream, 50)
	// test yeild all values
	AssertQueryOK(t, NewGlob.Client, "SELECT * FROM %s", NewGlob.Stream)
	AssertQueryOK(t, NewGlob.Client, "SELECT * FROM %s OFFSET 25 LIMIT 25", NewGlob.Stream)
	// test fetch single column
	for _, item := range flogStreamFields() {
		AssertQueryOK(t, NewGlob.Client, "SELECT %s FROM %s", item, NewGlob.Stream)
	}
	// test basic filter
	AssertQueryOK(t, NewGlob.Client, "SELECT * FROM %s WHERE method = 'POST'", NewGlob.Stream)
	// test group by
	AssertQueryOK(t, NewGlob.Client, "SELECT method, COUNT(*) FROM %s GROUP BY method", NewGlob.Stream)
	AssertQueryOK(t, NewGlob.Client, `SELECT DATE_TRUNC('minute', p_timestamp) as minute, COUNT(*) FROM %s GROUP BY minute`, NewGlob.Stream)

	DeleteStream(t, NewGlob.Client, NewGlob.Stream)
}

func TestSmokeLoadWithK6Stream(t *testing.T) {
	CreateStream(t, NewGlob.Client, NewGlob.Stream)
	cmd := exec.Command("k6",
		"run",
		"-e", fmt.Sprintf("P_URL=%s", NewGlob.Url.String()),
		"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.Username),
		"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.Password),
		"-e", fmt.Sprintf("P_STREAM=%s", NewGlob.Stream),
		"./scripts/smoke.js")

	cmd.Run()
	cmd.Output()
	QueryLogStreamCount(t, NewGlob.Client, NewGlob.Stream, 60000)
	AssertStreamSchema(t, NewGlob.Client, NewGlob.Stream, SchemaBody)
}

func TestSmokeSetAlert(t *testing.T) {
	req, _ := NewGlob.Client.NewRequest("PUT", "logstream/"+NewGlob.Stream+"/alert", strings.NewReader(AlertBody))
	response, err := NewGlob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func TestSmokeGetAlert(t *testing.T) {
	req, _ := NewGlob.Client.NewRequest("GET", "logstream/"+NewGlob.Stream+"/alert", nil)
	response, err := NewGlob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	require.JSONEq(t, AlertBody, body, "Get alert response doesn't match with Alert config returned")
}

func TestSmokeSetRetention(t *testing.T) {
	req, _ := NewGlob.Client.NewRequest("PUT", "logstream/"+NewGlob.Stream+"/retention", strings.NewReader(RetentionBody))
	response, err := NewGlob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func TestSmokeGetRetention(t *testing.T) {
	req, _ := NewGlob.Client.NewRequest("GET", "logstream/"+NewGlob.Stream+"/retention", nil)
	response, err := NewGlob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	require.JSONEq(t, RetentionBody, body, "Get retention response doesn't match with retention config returned")
}

// This test calls all the User API endpoints
// in a sequence to check if they work as expected.
func TestSmoke_AllUsersAPI(t *testing.T) {
	CreateRole(t, NewGlob.Client, "dummyrole", dummyRole)
	AssertRole(t, NewGlob.Client, "dummyrole", dummyRole)

	CreateUser(t, NewGlob.Client, "dummyuser")
	AssignRolesToUser(t, NewGlob.Client, "dummyuser", []string{"dummyrole"})
	AssertUserRole(t, NewGlob.Client, "dummyuser", "dummyrole", dummyRole)
	RegenPassword(t, NewGlob.Client, "dummyuser")
	DeleteUser(t, NewGlob.Client, "dummyuser")

	CreateUserWithRole(t, NewGlob.Client, "dummyuser", []string{"dummyrole"})
	AssertUserRole(t, NewGlob.Client, "dummyuser", "dummyrole", dummyRole)
	RegenPassword(t, NewGlob.Client, "dummyuser")
	DeleteUser(t, NewGlob.Client, "dummyuser")

	DeleteRole(t, NewGlob.Client, "dummyrole")
}

// This test checks that a new user doesn't get any role by default
// even if a default role is set.
func TestSmoke_NewUserNoRole(t *testing.T) {
	CreateRole(t, NewGlob.Client, "dummyrole", dummyRole)
	SetDefaultRole(t, NewGlob.Client, "dummyrole")
	AssertDefaultRole(t, NewGlob.Client, "\"dummyrole\"")

	password := CreateUser(t, NewGlob.Client, "dummyuser")
	userClient := NewGlob.Client
	userClient.Username = "dummyuser"
	userClient.Password = password

	PutSingleEventExpectErr(t, userClient, NewGlob.Stream)

	DeleteUser(t, NewGlob.Client, "dummyuser")
}

func TestSmokeRbacBasic(t *testing.T) {
	CreateRole(t, NewGlob.Client, "dummy", dummyRole)
	AssertRole(t, NewGlob.Client, "dummy", dummyRole)
	CreateUserWithRole(t, NewGlob.Client, "dummy", []string{"dummy"})
	userClient := NewGlob.Client
	userClient.Username = "dummy"
	userClient.Password = RegenPassword(t, NewGlob.Client, "dummy")
	checkAPIAccess(t, userClient, NewGlob.Stream, "editor")
	DeleteUser(t, NewGlob.Client, "dummy")
	DeleteRole(t, NewGlob.Client, "dummy")
}

func TestSmokeRoles(t *testing.T) {
	cases := []struct {
		roleName string
		body     string
	}{
		{
			roleName: "editor",
			body:     RoleEditor,
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
			roleName: "ingester",
			body:     RoleIngester(NewGlob.Stream),
		},
	}

	for _, tc := range cases {
		t.Run(tc.roleName, func(t *testing.T) {
			CreateRole(t, NewGlob.Client, tc.roleName, tc.body)
			AssertRole(t, NewGlob.Client, tc.roleName, tc.body)
			username := tc.roleName + "_user"
			password := CreateUserWithRole(t, NewGlob.Client, username, []string{tc.roleName})

			userClient := NewGlob.Client
			userClient.Username = username
			userClient.Password = password
			checkAPIAccess(t, userClient, NewGlob.Stream, tc.roleName)
			DeleteUser(t, NewGlob.Client, username)
			DeleteRole(t, NewGlob.Client, tc.roleName)
		})
	}
}

func TestLoadStreamBatchWithK6(t *testing.T) {
	if NewGlob.Mode == "load" {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.Url.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.Username),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.Password),
			"-e", fmt.Sprintf("P_STREAM=%s", NewGlob.Stream),
			"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
			"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", schema_count),
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

func TestLoadHistoricalStreamBatchWithK6(t *testing.T) {
	if NewGlob.Mode == "load" {
		historicalStream := NewGlob.Stream + "historical"
		timeHeader := map[string]string{"X-P-Time-Partition": "source_time"}
		CreateStreamWithHeader(t, NewGlob.Client, historicalStream, timeHeader)

		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.Url.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.Username),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.Password),
			"-e", fmt.Sprintf("P_STREAM=%s", historicalStream),
			"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
			"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", schema_count),
			"./scripts/load_historical_batch_events.js",
			"--vus=", vus,
			"--duration=", duration)

		cmd.Run()
		op, err := cmd.Output()
		if err != nil {
			t.Log(err)
		}
		t.Log(string(op))
		DeleteStream(t, NewGlob.Client, historicalStream)
	}
}

func TestLoadStreamNoBatchWithK6(t *testing.T) {
	if NewGlob.Mode == "load" {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.Url.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.Username),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.Password),
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

func TestDeleteStream(t *testing.T) {
	DeleteStream(t, NewGlob.Client, NewGlob.Stream)
}
