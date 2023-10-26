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
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	sleepDuration = 2 * time.Second
)

func readAsString(body io.Reader) string {
	r, _ := io.ReadAll(body)
	return string(r)
}

func readJsonBody[T any](body io.Reader) (res T, err error) {
	r, _ := io.ReadAll(body)
	err = json.Unmarshal(r, &res)
	return
}

func Sleep() {
	time.Sleep(sleepDuration)
}

func CreateStream(t *testing.T, client HTTPClient, stream string) {
	req, _ := client.NewRequest("PUT", "logstream/"+stream, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
}

func DeleteStream(t *testing.T, client HTTPClient, stream string) {
	req, _ := client.NewRequest("DELETE", "logstream/"+stream, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
}

func QueryLogStreamCount(t *testing.T, client HTTPClient, stream string, count uint64) {
	// Query last 10 minutes of data only
	endTime := time.Now().Format(time.RFC3339)
	startTime := time.Now().Add(-10 * time.Minute).Format(time.RFC3339)

	query := map[string]interface{}{
		"query":     "select count(*) as count from " + stream,
		"startTime": startTime,
		"endTime":   endTime,
	}
	queryJSON, _ := json.Marshal(query)
	req, _ := client.NewRequest("POST", "query", bytes.NewBuffer(queryJSON))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	expected := fmt.Sprintf(`[{"count":%d}]`, count)
	require.Equalf(t, expected, body, "Query count incorrect; Expected %s, Actual %s", expected, body)
}

func AssertStreamSchema(t *testing.T, client HTTPClient, stream string, schema string) {
	req, _ := client.NewRequest("GET", "logstream/"+stream+"/schema", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	require.JSONEq(t, schema, body, "Get schema response doesn't match with expected schema")
}

func CreateRole(t *testing.T, client HTTPClient, name string, role string) {
	req, _ := client.NewRequest("PUT", "role/"+name, strings.NewReader(role))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func AssertRole(t *testing.T, client HTTPClient, name string, role string) {
	req, _ := client.NewRequest("GET", "role/"+name, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	require.JSONEq(t, role, body, "Get role response doesn't match with retention config returned")
}

func CreateUser(t *testing.T, client HTTPClient, user string) string {
	req, _ := client.NewRequest("POST", "user/"+user, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	return body
}

func CreateUserWithRole(t *testing.T, client HTTPClient, user string, roles []string) string {
	payload, _ := json.Marshal(roles)
	req, _ := client.NewRequest("POST", "user/"+user, bytes.NewBuffer(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	return body
}

func AssignRolesToUser(t *testing.T, client HTTPClient, user string, roles []string) {
	payload, _ := json.Marshal(roles)
	req, _ := client.NewRequest("PUT", "user/"+user+"/role", bytes.NewBuffer(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func AssertUserRole(t *testing.T, client HTTPClient, user string, roleName, roleBody string) {
	req, _ := client.NewRequest("GET", "user/"+user+"/role", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	userRoleBody := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, userRoleBody)
	expectedRoleBody := fmt.Sprintf(`{"%s":%s}`, roleName, roleBody)
	require.JSONEq(t, userRoleBody, expectedRoleBody, "Get user role response doesn't match with expected role")
}

func RegenPassword(t *testing.T, client HTTPClient, user string) string {
	req, _ := client.NewRequest("POST", "user/"+user+"/generate-new-password", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	return body
}

func SetUserRole(t *testing.T, client HTTPClient, user string, roles []string) {
	payload, _ := json.Marshal(roles)
	req, _ := client.NewRequest("PUT", "user/"+user+"/role", bytes.NewBuffer(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func DeleteUser(t *testing.T, client HTTPClient, user string) {
	req, _ := client.NewRequest("DELETE", "user/"+user, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func DeleteRole(t *testing.T, client HTTPClient, roleName string) {
	req, _ := client.NewRequest("DELETE", "role/"+roleName, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func SetDefaultRole(t *testing.T, client HTTPClient, roleName string) {
	payload, _ := json.Marshal(roleName)
	req, _ := client.NewRequest("PUT", "role/default", bytes.NewBuffer(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func AssertDefaultRole(t *testing.T, client HTTPClient, roleName string) {
	req, _ := client.NewRequest("GET", "role/default", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	require.Equalf(t, roleName, body, "Get default role response doesn't match with expected role")
}

func PutSingleEventExpectErr(t *testing.T, client HTTPClient, stream string) {
	payload := `{
		"id": "id;objectId",
		"maxRunDistance": "float;1;20;1",
		"cpf": "cpf",
		"cnpj": "cnpj",
		"pretendSalary": "money",
		"age": "int;20;80",
		"gender": "gender",
		"firstName": "firstName",
		"lastName": "lastName",
		"phone": "maskInt;+55 (83) 9####-####",
		"address": "address",
		"hairColor": "color"
	}`
	req, _ := client.NewRequest("POST", "logstream/"+stream, bytes.NewBufferString(payload))
	response, err := client.Do(req)

	require.NoErrorf(t, err, "Request failed when expected to pass: %s", err)
	require.Equalf(t, 403, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func PutSingleEvent(t *testing.T, client HTTPClient, stream string) {
	payload := `{
		"id": "id;objectId",
		"maxRunDistance": "float;1;20;1",
		"cpf": "cpf",
		"cnpj": "cnpj",
		"pretendSalary": "money",
		"age": "int;20;80",
		"gender": "gender",
		"firstName": "firstName",
		"lastName": "lastName",
		"phone": "maskInt;+55 (83) 9####-####",
		"address": "address",
		"hairColor": "color"
	}`
	req, _ := client.NewRequest("POST", "logstream/"+stream, bytes.NewBufferString(payload))
	response, err := client.Do(req)

	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func checkAPIAccess(t *testing.T, client HTTPClient, stream string, role string) {
	switch role {
	case "editor":
		// Check access to non-protected API
		req, _ := client.NewRequest("GET", "liveness", nil)
		response, err := client.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

		// Check access to protected API with access
		req, _ = client.NewRequest("GET", "logstream", nil)
		response, err = client.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

		// Attempt to call protected API without access
		req, _ = client.NewRequest("DELETE", "logstream/"+stream, nil)
		response, err = client.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 403, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

	case "writer":
		// Check access to non-protected API
		req, _ := client.NewRequest("GET", "liveness", nil)
		response, err := client.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

		// Check access to protected API with access
		req, _ = client.NewRequest("GET", "logstream", nil)
		response, err = client.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

		// Attempt to call protected API without access
		req, _ = client.NewRequest("DELETE", "logstream/"+stream, nil)
		response, err = client.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 403, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

	case "reader":
		// Check access to non-protected API
		req, _ := client.NewRequest("GET", "liveness", nil)
		response, err := client.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

		// Check access to protected API with access
		req, _ = client.NewRequest("GET", "logstream", nil)
		response, err = client.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

		// Attempt to call protected API without access
		req, _ = client.NewRequest("DELETE", "logstream/"+stream, nil)
		response, err = client.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 403, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

	case "ingester":
		// Check access to non-protected API
		req, _ := client.NewRequest("GET", "liveness", nil)
		response, err := client.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

		// Check access to protected API with access
		PutSingleEvent(t, client, stream)

		// Attempt to call protected API without access
		req, _ = client.NewRequest("DELETE", "logstream/"+stream, nil)
		response, err = client.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 403, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
	}
}
