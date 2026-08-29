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
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	sleepDuration = 2 * time.Second
)

func flogStreamFields() []string {
	return []string{
		"p_timestamp",
		"host",
		"'user-identifier'",
		"datetime",
		"method",
		"request",
		"protocol",
		"status",
		"bytes",
		"referer",
	}
}

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
	t.Helper()
	req, _ := client.NewRequest("PUT", "logstream/"+stream, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "CreateStream(%s): request failed: %s", stream, err)
	require.Equalf(t, 200, response.StatusCode, "CreateStream(%s): server returned http code: %s and response: %s", stream, response.Status, readAsString(response.Body))
}

func CreateStreamWithHeader(t *testing.T, client HTTPClient, stream string, header map[string]string) {
	t.Helper()
	req, _ := client.NewRequest("PUT", "logstream/"+stream, nil)
	for k, v := range header {
		req.Header.Add(k, v)
	}
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
}

func CreateStreamWithCustompartitionError(t *testing.T, client HTTPClient, stream string, header map[string]string) {
	t.Helper()
	req, _ := client.NewRequest("PUT", "logstream/"+stream, nil)
	for k, v := range header {
		req.Header.Add(k, v)
	}
	response, _ := client.Do(req)
	require.Equalf(t, 500, response.StatusCode, "Server returned http code: %s", response.Status)
}

func CreateStreamWithSchemaBody(t *testing.T, client HTTPClient, stream string, header map[string]string, schema_payload string) {
	t.Helper()
	req, _ := client.NewRequest("PUT", "logstream/"+stream, bytes.NewBufferString(schema_payload))
	for k, v := range header {
		req.Header.Add(k, v)
	}
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
}

func DetectSchema(t *testing.T, client HTTPClient, sampleJson string, schemaBody string) {
	t.Helper()
	req, _ := client.NewRequest("POST", "logstream/schema/detect", bytes.NewBufferString(sampleJson))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	require.JSONEq(t, schemaBody, body, "Schema detection failed")
}

func DeleteStream(t *testing.T, client HTTPClient, stream string) {
	t.Helper()
	req, _ := client.NewRequest("DELETE", "logstream/"+stream, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "DeleteStream(%s): request failed: %s", stream, err)
	require.Equalf(t, 200, response.StatusCode, "DeleteStream(%s): server returned http code: %s and response: %s", stream, response.Status, readAsString(response.Body))
}

func DeleteAlert(t *testing.T, client HTTPClient, alert_id string) {
	t.Helper()
	req, _ := client.NewRequest("DELETE", "alerts/"+alert_id, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
}

func DeleteTarget(t *testing.T, client HTTPClient, target_id string) {
	t.Helper()
	req, _ := client.NewRequest("DELETE", "targets/"+target_id, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
}

func RunFlog(t *testing.T, client HTTPClient, stream string) {
	t.Helper()
	cmd := exec.Command("flog", "-f", "json", "-n", "50")
	var out strings.Builder
	cmd.Stdout = &out
	err := cmd.Run()
	require.NoErrorf(t, err, "Failed to run flog: %s", err)

	for _, obj := range strings.SplitN(out.String(), "\n", 50) {
		var payload strings.Builder
		payload.WriteRune('[')
		payload.WriteString(obj)
		payload.WriteRune(']')

		req, _ := client.NewRequest("POST", "ingest", bytes.NewBufferString(payload.String()))
		req.Header.Add("X-P-Stream", stream)
		response, err := client.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))
	}
}

func IngestOneEventWithTimePartition_TimeStampMismatch(t *testing.T, client HTTPClient, stream string) {
	t.Helper()
	var test_payload string = `{"source_time":"2024-03-26T18:08:00.434Z","level":"info","message":"Application is failing","version":"1.2.0","user_id":13912,"device_id":4138,"session_id":"abc","os":"Windows","host":"112.168.1.110","location":"ngeuprqhynuvpxgp","request_body":"rnkmffyawtdcindtrdqruyxbndbjpfsptzpwtujbmkwcqastmxwbvjwphmyvpnhordwljnodxhtvpjesjldtifswqbpyuhlcytmm","status_code":300,"app_meta":"ckgpibhmlusqqfunnpxbfxbc", "new_field_added_by":"ingestor 8020"}`
	req, _ := client.NewRequest("POST", "ingest", bytes.NewBufferString(test_payload))
	req.Header.Add("X-P-Stream", stream)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 400, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))
}

func IngestOneEventWithTimePartition_NoTimePartitionInLog(t *testing.T, client HTTPClient, stream string) {
	t.Helper()
	var test_payload string = `{"level":"info","message":"Application is failing","version":"1.2.0","user_id":13912,"device_id":4138,"session_id":"abc","os":"Windows","host":"112.168.1.110","location":"ngeuprqhynuvpxgp","request_body":"rnkmffyawtdcindtrdqruyxbndbjpfsptzpwtujbmkwcqastmxwbvjwphmyvpnhordwljnodxhtvpjesjldtifswqbpyuhlcytmm","status_code":300,"app_meta":"ckgpibhmlusqqfunnpxbfxbc", "new_field_added_by":"ingestor 8020"}`
	req, _ := client.NewRequest("POST", "ingest", bytes.NewBufferString(test_payload))
	req.Header.Add("X-P-Stream", stream)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 400, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))
}

func IngestOneEventWithTimePartition_IncorrectDateTimeFormatTimePartitionInLog(t *testing.T, client HTTPClient, stream string) {
	t.Helper()
	var test_payload string = `{"source_time":"2024-03-26", "level":"info","message":"Application is failing","version":"1.2.0","user_id":13912,"device_id":4138,"session_id":"abc","os":"Windows","host":"112.168.1.110","location":"ngeuprqhynuvpxgp","request_body":"rnkmffyawtdcindtrdqruyxbndbjpfsptzpwtujbmkwcqastmxwbvjwphmyvpnhordwljnodxhtvpjesjldtifswqbpyuhlcytmm","status_code":300,"app_meta":"ckgpibhmlusqqfunnpxbfxbc", "new_field_added_by":"ingestor 8020"}`
	req, _ := client.NewRequest("POST", "ingest", bytes.NewBufferString(test_payload))
	req.Header.Add("X-P-Stream", stream)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 400, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))
}

func IngestOneEventForStaticSchemaStream_NewFieldInLog(t *testing.T, client HTTPClient, stream string) {
	t.Helper()
	var test_payload string = `{"source_time":"2024-03-26", "level":"info","message":"Application is failing","version":"1.2.0","user_id":13912,"device_id":4138,"session_id":"abc","os":"Windows","host":"112.168.1.110","location":"ngeuprqhynuvpxgp","request_body":"rnkmffyawtdcindtrdqruyxbndbjpfsptzpwtujbmkwcqastmxwbvjwphmyvpnhordwljnodxhtvpjesjldtifswqbpyuhlcytmm","status_code":300,"app_meta":"ckgpibhmlusqqfunnpxbfxbc", "new_field_added_by":"ingestor 8020"}`
	req, _ := client.NewRequest("POST", "ingest", bytes.NewBufferString(test_payload))
	req.Header.Add("X-P-Stream", stream)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 400, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))
}

func IngestOneEventForStaticSchemaStream_SameFieldsInLog(t *testing.T, client HTTPClient, stream string) {
	t.Helper()
	var test_payload string = `{"source_time":"2024-03-26", "level":"info","message":"Application is failing","version":"1.2.0","user_id":13912,"device_id":4138,"session_id":"abc","os":"Windows","host":"112.168.1.110","location":"ngeuprqhynuvpxgp","request_body":"rnkmffyawtdcindtrdqruyxbndbjpfsptzpwtujbmkwcqastmxwbvjwphmyvpnhordwljnodxhtvpjesjldtifswqbpyuhlcytmm","status_code":300,"app_meta":"ckgpibhmlusqqfunnpxbfxbc"}`
	req, _ := client.NewRequest("POST", "ingest", bytes.NewBufferString(test_payload))
	req.Header.Add("X-P-Stream", stream)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))
}

func QueryLogStreamCount(t *testing.T, client HTTPClient, stream string, count uint64) {
	t.Helper()
	// Query last 30 minutes of data only
	endTime := time.Now().Add(time.Second).Format(time.RFC3339Nano)
	startTime := time.Now().Add(-30 * time.Minute).Format(time.RFC3339Nano)

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

func QueryLogStreamCount_Historical(t *testing.T, client HTTPClient, stream string, count uint64) {
	t.Helper()
	now := time.Now()
	startTime := now.AddDate(0, 0, -33).Format(time.RFC3339Nano)
	endTime := now.AddDate(0, 0, -27).Format(time.RFC3339Nano)

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

func QueryTwoLogStreamCount(t *testing.T, client HTTPClient, stream1 string, stream2 string, count uint64) {
	t.Helper()
	endTime := time.Now().Add(time.Second).Format(time.RFC3339Nano)
	startTime := time.Now().Add(-30 * time.Minute).Format(time.RFC3339Nano)

	query := map[string]interface{}{
		"query":     fmt.Sprintf("select sum(c) as count from (select count(*) as c from %s union all select count(*) as c from %s)", stream1, stream2),
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

func AssertQueryOK(t *testing.T, client HTTPClient, query string, args ...any) {
	t.Helper()
	endTime := time.Now().Add(time.Second).Format(time.RFC3339Nano)
	startTime := time.Now().Add(-30 * time.Minute).Format(time.RFC3339Nano)

	var finalQuery string
	if len(args) == 0 {
		finalQuery = query
	} else {
		finalQuery = fmt.Sprintf(query, args...)
	}

	queryJSON, _ := json.Marshal(map[string]interface{}{
		"query":     finalQuery,
		"startTime": startTime,
		"endTime":   endTime,
	})

	req, _ := client.NewRequest("POST", "query", bytes.NewBuffer(queryJSON))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
}

func AssertStreamSchema(t *testing.T, client HTTPClient, stream string, schema string) {
	t.Helper()
	req, _ := client.NewRequest("GET", "logstream/"+stream+"/schema", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "AssertStreamSchema(%s): request failed: %s", stream, err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "AssertStreamSchema(%s): server returned http code: %s and response: %s", stream, response.Status, body)
	require.JSONEq(t, schema, body, "AssertStreamSchema(%s): actual schema doesn't match expected.\nActual: %s", stream, body)
}

func CreateRole(t *testing.T, client HTTPClient, name string, role string) {
	t.Helper()
	req, _ := client.NewRequest("PUT", "role/"+name, strings.NewReader(role))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func AssertRole(t *testing.T, client HTTPClient, name string, role string) {
	t.Helper()
	req, _ := client.NewRequest("GET", "role/"+name, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	require.JSONEq(t, role, body, "Get role response doesn't match with retention config returned")
}

func CreateUser(t *testing.T, client HTTPClient, user string) string {
	t.Helper()
	req, _ := client.NewRequest("POST", "user/"+user, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))
	return body
}

func CreateUserWithRole(t *testing.T, client HTTPClient, user string, roles []string) string {
	t.Helper()
	payload, _ := json.Marshal(roles)
	req, _ := client.NewRequest("POST", "user/"+user, bytes.NewBuffer(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	return body
}

func AssignRolesToUser(t *testing.T, client HTTPClient, user string, roles []string) {
	t.Helper()
	payload, _ := json.Marshal(roles)
	req, _ := client.NewRequest("PUT", "user/"+user+"/role", bytes.NewBuffer(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func AssertUserRole(t *testing.T, client HTTPClient, user string, roleName, roleBody string) {
	t.Helper()
	req, _ := client.NewRequest("GET", "user/"+user+"/role", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	userRoleBody := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, userRoleBody)
	expectedRoleBody := fmt.Sprintf(`{"roles":{"%s":%s}, "group_roles": {}}`, roleName, roleBody)
	require.JSONEq(t, userRoleBody, expectedRoleBody, "Get user role response doesn't match with expected role")
}

func RegenPassword(t *testing.T, client HTTPClient, user string) string {
	t.Helper()
	req, _ := client.NewRequest("POST", "user/"+user+"/generate-new-password", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	return body
}

func SetUserRole(t *testing.T, client HTTPClient, user string, roles []string) {
	t.Helper()
	payload, _ := json.Marshal(roles)
	req, _ := client.NewRequest("PUT", "user/"+user+"/role", bytes.NewBuffer(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func DeleteUser(t *testing.T, client HTTPClient, user string) {
	t.Helper()
	req, _ := client.NewRequest("DELETE", "user/"+user, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func DeleteRole(t *testing.T, client HTTPClient, roleName string) {
	t.Helper()
	req, _ := client.NewRequest("DELETE", "role/"+roleName, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func SetDefaultRole(t *testing.T, client HTTPClient, roleName string) {
	t.Helper()
	payload, _ := json.Marshal(roleName)
	req, _ := client.NewRequest("PUT", "role/default", bytes.NewBuffer(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func AssertDefaultRole(t *testing.T, client HTTPClient, roleName string) {
	t.Helper()
	req, _ := client.NewRequest("GET", "role/default", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	require.Equalf(t, roleName, body, "Get default role response doesn't match with expected role")
}

func PutSingleEventExpectErr(t *testing.T, client HTTPClient, stream string) {
	t.Helper()
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
	t.Helper()
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

func checkAPIAccess(t *testing.T, queryClient HTTPClient, ingestClient HTTPClient, stream string, role string) {
	t.Helper()
	switch role {
	case "editor":
		// Check access to non-protected API
		req, _ := queryClient.NewRequest("GET", "liveness", nil)
		response, err := queryClient.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

		// Check access to protected API with access
		req, _ = queryClient.NewRequest("GET", "logstream", nil)
		response, err = queryClient.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

		// Attempt to call protected API without access
		req, _ = queryClient.NewRequest("DELETE", "logstream/"+stream, nil)
		response, err = queryClient.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

	case "writer":
		// Check access to non-protected API
		req, _ := queryClient.NewRequest("GET", "liveness", nil)
		response, err := queryClient.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

		// Check access to protected API with access
		req, _ = queryClient.NewRequest("GET", "logstream", nil)
		response, err = queryClient.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

		// Attempt to call protected API without access
		req, _ = queryClient.NewRequest("DELETE", "logstream/"+stream, nil)
		response, err = queryClient.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 403, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

	case "reader":
		// Check access to non-protected API
		req, _ := queryClient.NewRequest("GET", "liveness", nil)
		response, err := queryClient.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

		// Check access to protected API with access
		req, _ = queryClient.NewRequest("GET", "logstream", nil)
		response, err = queryClient.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

		// Attempt to call protected API without access
		req, _ = queryClient.NewRequest("DELETE", "logstream/"+stream, nil)
		response, err = queryClient.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 403, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

	case "ingestor":
		// Check access to non-protected API
		req, _ := queryClient.NewRequest("GET", "liveness", nil)
		response, err := queryClient.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

		// Check access to protected API with access
		PutSingleEvent(t, ingestClient, stream)

		// Attempt to call protected API without access
		req, _ = queryClient.NewRequest("DELETE", "logstream/"+stream, nil)
		response, err = queryClient.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 403, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
	}
}

// --- New utility functions for expanded test coverage ---

// Health check utilities
func AssertLiveness(t *testing.T, client HTTPClient) {
	t.Helper()
	req, _ := client.NewRequest("GET", "liveness", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Liveness request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Liveness check failed with status: %s", response.Status)
}

func AssertLivenessHead(t *testing.T, client HTTPClient) {
	t.Helper()
	req, _ := client.NewRequest("HEAD", "liveness", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "HEAD liveness request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "HEAD liveness check failed with status: %s", response.Status)
}

func AssertReadiness(t *testing.T, client HTTPClient) {
	t.Helper()
	req, _ := client.NewRequest("GET", "readiness", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Readiness request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Readiness check failed with status: %s", response.Status)
}

func AssertReadinessHead(t *testing.T, client HTTPClient) {
	t.Helper()
	req, _ := client.NewRequest("HEAD", "readiness", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "HEAD readiness request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "HEAD readiness check failed with status: %s", response.Status)
}

// Stream info and stats utilities
func AssertStreamInfo(t *testing.T, client HTTPClient, stream string) {
	t.Helper()
	req, _ := client.NewRequest("GET", "logstream/"+stream+"/info", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Stream info request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Stream info failed with status: %s, body: %s", response.Status, body)
	require.NotEmptyf(t, body, "Stream info response should not be empty")
}

func AssertStreamStats(t *testing.T, client HTTPClient, stream string) {
	t.Helper()
	req, _ := client.NewRequest("GET", "logstream/"+stream+"/stats", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Stream stats request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Stream stats failed with status: %s, body: %s", response.Status, body)
	require.NotEmptyf(t, body, "Stream stats response should not be empty")
}

// OTel ingestion utilities
func IngestOTelLogs(t *testing.T, client HTTPClient) {
	t.Helper()
	payload := getOTelLogPayload()
	req, _ := client.NewOTelRequest("POST", "logs", strings.NewReader(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "OTel logs ingestion request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "OTel logs ingestion failed with status: %s, body: %s", response.Status, readAsString(response.Body))
}

func IngestOTelTraces(t *testing.T, client HTTPClient) {
	t.Helper()
	payload := getOTelTracePayload()
	req, _ := client.NewOTelRequest("POST", "traces", strings.NewReader(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "OTel traces ingestion request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "OTel traces ingestion failed with status: %s, body: %s", response.Status, readAsString(response.Body))
}

func IngestOTelMetrics(t *testing.T, client HTTPClient) {
	t.Helper()
	payload := getOTelMetricPayload()
	req, _ := client.NewOTelRequest("POST", "metrics", strings.NewReader(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "OTel metrics ingestion request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "OTel metrics ingestion failed with status: %s, body: %s", response.Status, readAsString(response.Body))
}

// Alert lifecycle utilities
func GetAlertById(t *testing.T, client HTTPClient, alertId string) string {
	t.Helper()
	req, _ := client.NewRequest("GET", "alerts/"+alertId, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Get alert by ID request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Get alert by ID failed with status: %s, body: %s", response.Status, body)
	return body
}

func ModifyAlert(t *testing.T, client HTTPClient, alertId string, payload string) {
	t.Helper()
	req, _ := client.NewRequest("PUT", "alerts/"+alertId, strings.NewReader(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Modify alert request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Modify alert failed with status: %s, body: %s", response.Status, readAsString(response.Body))
}

func DisableAlert(t *testing.T, client HTTPClient, alertId string) {
	t.Helper()
	req, _ := client.NewRequest("PUT", "alerts/"+alertId+"/disable", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Disable alert request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Disable alert failed with status: %s, body: %s", response.Status, readAsString(response.Body))
}

func EnableAlert(t *testing.T, client HTTPClient, alertId string) {
	t.Helper()
	req, _ := client.NewRequest("PUT", "alerts/"+alertId+"/enable", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Enable alert request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Enable alert failed with status: %s, body: %s", response.Status, readAsString(response.Body))
}

func ListAlertTags(t *testing.T, client HTTPClient) string {
	t.Helper()
	req, _ := client.NewRequest("GET", "alerts/tags", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "List alert tags request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "List alert tags failed with status: %s, body: %s", response.Status, body)
	return body
}

func EvaluateAlert(t *testing.T, client HTTPClient, alertId string) {
	t.Helper()
	req, _ := client.NewRequest("POST", "alerts/"+alertId+"/evaluate", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Evaluate alert request failed: %s", err)
	body := readAsString(response.Body)
	// Evaluate may return 200 or other success codes depending on state
	require.Containsf(t, []int{200, 202}, response.StatusCode, "Evaluate alert returned unexpected status: %s, body: %s", response.Status, body)
}

// Dashboard CRUD utilities
func CreateDashboard(t *testing.T, client HTTPClient) string {
	t.Helper()
	payload := getDashboardCreateBody()
	req, _ := client.NewRequest("POST", "dashboards", strings.NewReader(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Create dashboard request failed: %s", err)
	body, _ := io.ReadAll(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Create dashboard failed with status: %s, body: %s", response.Status, string(body))
	reader := bytes.NewReader(body)
	return getIdFromDashboardResponse(reader)
}

func ListDashboards(t *testing.T, client HTTPClient) string {
	t.Helper()
	req, _ := client.NewRequest("GET", "dashboards", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "List dashboards request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "List dashboards failed with status: %s, body: %s", response.Status, body)
	return body
}

func GetDashboardById(t *testing.T, client HTTPClient, dashboardId string) string {
	t.Helper()
	req, _ := client.NewRequest("GET", "dashboards/"+dashboardId, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Get dashboard by ID request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Get dashboard failed with status: %s, body: %s", response.Status, body)
	return body
}

func UpdateDashboard(t *testing.T, client HTTPClient, dashboardId string) {
	t.Helper()
	payload := getDashboardUpdateBody()
	req, _ := client.NewRequest("PUT", "dashboards/"+dashboardId, strings.NewReader(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Update dashboard request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Update dashboard failed with status: %s, body: %s", response.Status, readAsString(response.Body))
}

func AddDashboardTile(t *testing.T, client HTTPClient, dashboardId string, stream string) {
	t.Helper()
	payload := getDashboardAddTileBody(stream)
	req, _ := client.NewRequest("PUT", "dashboards/"+dashboardId+"/add_tile", strings.NewReader(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Add dashboard tile request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Add tile failed with status: %s, body: %s", response.Status, readAsString(response.Body))
}

func ListDashboardTags(t *testing.T, client HTTPClient) string {
	t.Helper()
	req, _ := client.NewRequest("GET", "dashboards/list_tags", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "List dashboard tags request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "List dashboard tags failed with status: %s, body: %s", response.Status, body)
	return body
}

func DeleteDashboard(t *testing.T, client HTTPClient, dashboardId string) {
	t.Helper()
	req, _ := client.NewRequest("DELETE", "dashboards/"+dashboardId, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Delete dashboard request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Delete dashboard failed with status: %s, body: %s", response.Status, readAsString(response.Body))
}

// Filter CRUD utilities
func CreateFilter(t *testing.T, client HTTPClient, stream string) string {
	t.Helper()
	payload := getFilterCreateBody(stream)
	req, _ := client.NewRequest("POST", "filters", strings.NewReader(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Create filter request failed: %s", err)
	body, _ := io.ReadAll(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Create filter failed with status: %s, body: %s", response.Status, string(body))
	reader := bytes.NewReader(body)
	return getIdFromFilterResponse(reader)
}

func ListFilters(t *testing.T, client HTTPClient) string {
	t.Helper()
	req, _ := client.NewRequest("GET", "filters", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "List filters request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "List filters failed with status: %s, body: %s", response.Status, body)
	return body
}

func GetFilterById(t *testing.T, client HTTPClient, filterId string) string {
	t.Helper()
	req, _ := client.NewRequest("GET", "filters/"+filterId, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Get filter by ID request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Get filter failed with status: %s, body: %s", response.Status, body)
	return body
}

func UpdateFilter(t *testing.T, client HTTPClient, filterId string, stream string) {
	t.Helper()
	payload := getFilterUpdateBody(stream)
	req, _ := client.NewRequest("PUT", "filters/"+filterId, strings.NewReader(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Update filter request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Update filter failed with status: %s, body: %s", response.Status, readAsString(response.Body))
}

func DeleteFilter(t *testing.T, client HTTPClient, filterId string) {
	t.Helper()
	req, _ := client.NewRequest("DELETE", "filters/"+filterId, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Delete filter request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Delete filter failed with status: %s, body: %s", response.Status, readAsString(response.Body))
}

// Prism API utilities
func AssertPrismHome(t *testing.T, client HTTPClient) {
	t.Helper()
	req, _ := client.NewPrismRequest("GET", "home", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Prism home request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Prism home failed with status: %s, body: %s", response.Status, body)
}

func AssertPrismLogstreamInfo(t *testing.T, client HTTPClient, stream string) {
	t.Helper()
	req, _ := client.NewPrismRequest("GET", "logstream/"+stream+"/info", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Prism logstream info request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Prism logstream info failed with status: %s, body: %s", response.Status, body)
}

func AssertPrismDatasets(t *testing.T, client HTTPClient, stream string) {
	t.Helper()
	payload := getPrismDatasetsBody(stream)
	req, _ := client.NewPrismRequest("POST", "datasets", strings.NewReader(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Prism datasets request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Prism datasets failed with status: %s, body: %s", response.Status, body)
}

// RBAC add/remove role utilities
func AddRolesToUser(t *testing.T, client HTTPClient, user string, roles []string) {
	t.Helper()
	payload, _ := json.Marshal(roles)
	req, _ := client.NewRequest("PATCH", "user/"+user+"/role/add", bytes.NewBuffer(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Add roles request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Add roles failed with status: %s, body: %s", response.Status, readAsString(response.Body))
}

func RemoveRolesFromUser(t *testing.T, client HTTPClient, user string, roles []string) {
	t.Helper()
	payload, _ := json.Marshal(roles)
	req, _ := client.NewRequest("PATCH", "user/"+user+"/role/remove", bytes.NewBuffer(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Remove roles request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Remove roles failed with status: %s, body: %s", response.Status, readAsString(response.Body))
}

func ListAllRoles(t *testing.T, client HTTPClient) string {
	t.Helper()
	req, _ := client.NewRequest("GET", "roles", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "List roles request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "List roles failed with status: %s, body: %s", response.Status, body)
	return body
}

// Target CRUD utilities
func ListTargets(t *testing.T, client HTTPClient) string {
	t.Helper()
	req, _ := client.NewRequest("GET", "targets", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "List targets request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "List targets failed with status: %s, body: %s", response.Status, body)
	return body
}

func GetTargetById(t *testing.T, client HTTPClient, targetId string) string {
	t.Helper()
	req, _ := client.NewRequest("GET", "targets/"+targetId, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Get target by ID request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Get target failed with status: %s, body: %s", response.Status, body)
	return body
}

func UpdateTarget(t *testing.T, client HTTPClient, targetId string) {
	t.Helper()
	payload := getTargetUpdateBody()
	req, _ := client.NewRequest("PUT", "targets/"+targetId, strings.NewReader(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Update target request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Update target failed with status: %s, body: %s", response.Status, readAsString(response.Body))
}

// About, metrics, demo data utilities
func AssertAbout(t *testing.T, client HTTPClient) string {
	t.Helper()
	req, _ := client.NewRequest("GET", "about", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "About request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "About failed with status: %s, body: %s", response.Status, body)
	require.NotEmptyf(t, body, "About response should not be empty")
	return body
}

func AssertMetrics(t *testing.T, client HTTPClient) string {
	t.Helper()
	req, _ := client.NewRequest("GET", "metrics", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Metrics request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Metrics failed with status: %s, body: %s", response.Status, body)
	return body
}

func AssertDemoData(t *testing.T, client HTTPClient) {
	t.Helper()
	req, _ := client.NewRequest("GET", "demodata", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Demo data request failed: %s", err)
	require.Containsf(t, []int{200, 404}, response.StatusCode, "Demo data returned unexpected status: %s", response.Status)
}

// Hot tier utilities
func SetStreamHotTier(t *testing.T, client HTTPClient, stream string) {
	t.Helper()
	payload := getHotTierBody()
	req, _ := client.NewRequest("PUT", "logstream/"+stream+"/hottier", strings.NewReader(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Set hot tier request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Set hot tier failed with status: %s, body: %s", response.Status, readAsString(response.Body))
}

func GetStreamHotTier(t *testing.T, client HTTPClient, stream string) string {
	t.Helper()
	req, _ := client.NewRequest("GET", "logstream/"+stream+"/hottier", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Get hot tier request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Get hot tier failed with status: %s, body: %s", response.Status, body)
	return body
}

func DeleteStreamHotTier(t *testing.T, client HTTPClient, stream string) {
	t.Helper()
	req, _ := client.NewRequest("DELETE", "logstream/"+stream+"/hottier", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Delete hot tier request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Delete hot tier failed with status: %s, body: %s", response.Status, readAsString(response.Body))
}

// Dataset stats utility
func AssertDatasetStats(t *testing.T, client HTTPClient, streams []string) string {
	t.Helper()
	payload := getDatasetStatsBody(streams)
	req, _ := client.NewRequest("POST", "dataset_stats", strings.NewReader(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Dataset stats request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Dataset stats failed with status: %s, body: %s", response.Status, body)
	return body
}

// Negative test utilities
func AssertQueryError(t *testing.T, client HTTPClient, query string, expectedStatus int) {
	t.Helper()
	endTime := time.Now().Add(time.Second).Format(time.RFC3339Nano)
	startTime := time.Now().Add(-30 * time.Minute).Format(time.RFC3339Nano)

	queryJSON, _ := json.Marshal(map[string]interface{}{
		"query":     query,
		"startTime": startTime,
		"endTime":   endTime,
	})

	req, _ := client.NewRequest("POST", "query", bytes.NewBuffer(queryJSON))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, expectedStatus, response.StatusCode, "Expected status %d but got %s for query: %s", expectedStatus, response.Status, query)
}

func AssertIngestError(t *testing.T, client HTTPClient, stream string, payload string, expectedStatus int) {
	t.Helper()
	req, _ := client.NewRequest("POST", "ingest", bytes.NewBufferString(payload))
	req.Header.Add("X-P-Stream", stream)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, expectedStatus, response.StatusCode, "Expected status %d but got %s for ingest to stream %s", expectedStatus, response.Status, stream)
}

// Concurrent ingest+query utility
func ConcurrentIngestAndQuery(t *testing.T, ingestClient HTTPClient, queryClient HTTPClient, stream string, ingestCount int, queryConcurrency int) {
	t.Helper()
	var wg sync.WaitGroup
	var errors int64

	// Ingest goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < ingestCount; i++ {
			payload := fmt.Sprintf(`[{"level":"info","message":"concurrent test %d","host":"test-host"}]`, i)
			req, _ := ingestClient.NewRequest("POST", "ingest", bytes.NewBufferString(payload))
			req.Header.Add("X-P-Stream", stream)
			response, err := ingestClient.Do(req)
			if err != nil || response.StatusCode >= 500 {
				atomic.AddInt64(&errors, 1)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Query goroutines
	for i := 0; i < queryConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				endTime := time.Now().Add(time.Second).Format(time.RFC3339Nano)
				startTime := time.Now().Add(-30 * time.Minute).Format(time.RFC3339Nano)
				queryJSON, _ := json.Marshal(map[string]interface{}{
					"query":     "SELECT COUNT(*) as count FROM " + stream,
					"startTime": startTime,
					"endTime":   endTime,
				})
				req, _ := queryClient.NewRequest("POST", "query", bytes.NewBuffer(queryJSON))
				response, err := queryClient.Do(req)
				if err != nil || response.StatusCode >= 500 {
					atomic.AddInt64(&errors, 1)
				}
				time.Sleep(100 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
	require.Equalf(t, int64(0), errors, "Concurrent ingest+query had %d 500-level errors", errors)
}

// Cluster API utilities (for distributed mode)
func AssertClusterInfo(t *testing.T, client HTTPClient) string {
	t.Helper()
	req, _ := client.NewRequest("GET", "cluster/info", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Cluster info request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Cluster info failed with status: %s, body: %s", response.Status, body)
	return body
}

func AssertClusterMetrics(t *testing.T, client HTTPClient) string {
	t.Helper()
	req, _ := client.NewRequest("GET", "cluster/metrics", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Cluster metrics request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Cluster metrics failed with status: %s, body: %s", response.Status, body)
	return body
}

// LLM endpoint utility
func AssertLLMEndpointNoAPIKey(t *testing.T, client HTTPClient) {
	t.Helper()
	payload := `{"prompt": "test"}`
	req, _ := client.NewRequest("POST", "llm", strings.NewReader(payload))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "LLM request failed: %s", err)
	// Without API key, should return an error status (400 or 401 or 500)
	require.NotEqualf(t, 200, response.StatusCode, "LLM endpoint should fail without API key, got 200")
}

// --- Use case test utilities ---

func SetRetention(t *testing.T, client HTTPClient, stream string, body string) {
	t.Helper()
	req, _ := client.NewRequest("PUT", "logstream/"+stream+"/retention", strings.NewReader(body))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Set retention request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Set retention failed with status: %s, body: %s", response.Status, readAsString(response.Body))
}

func AssertRetention(t *testing.T, client HTTPClient, stream string, expectedBody string) {
	t.Helper()
	req, _ := client.NewRequest("GET", "logstream/"+stream+"/retention", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Get retention request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Get retention failed with status: %s, body: %s", response.Status, body)
	require.JSONEq(t, expectedBody, body, "Retention config doesn't match expected")
}

func AssertForbidden(t *testing.T, client HTTPClient, method, path string, body io.Reader) {
	t.Helper()
	req, _ := client.NewRequest(method, path, body)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	respBody := readAsString(response.Body)
	require.Equalf(t, 403, response.StatusCode, "Expected 403 Forbidden for %s %s, got status %d (%s), body: %s", method, path, response.StatusCode, response.Status, respBody)
}

func IngestCustomPayload(t *testing.T, client HTTPClient, stream string, payload string, expectedStatus int) {
	t.Helper()
	req, _ := client.NewRequest("POST", "ingest", bytes.NewBufferString(payload))
	req.Header.Add("X-P-Stream", stream)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Ingest request failed: %s", err)
	require.Equalf(t, expectedStatus, response.StatusCode, "Expected status %d for ingest to %s, got: %s, body: %s", expectedStatus, stream, response.Status, readAsString(response.Body))
}

func DiscoverOTelStreams(t *testing.T, client HTTPClient) []string {
	t.Helper()
	req, _ := client.NewRequest("GET", "logstream", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "List streams request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "List streams failed with status: %s", response.Status)

	var otelStreams []string
	var items []interface{}
	if err := json.Unmarshal([]byte(body), &items); err != nil {
		t.Logf("Could not parse stream list: %s", err)
		return nil
	}
	for _, item := range items {
		var name string
		switch v := item.(type) {
		case string:
			name = v
		case map[string]interface{}:
			if n, ok := v["name"].(string); ok {
				name = n
			}
		}
		if name == "" {
			continue
		}
		lower := strings.ToLower(name)
		if strings.Contains(lower, "otel") || strings.Contains(lower, "opentelemetry") {
			otelStreams = append(otelStreams, name)
		}
	}
	return otelStreams
}

func ConcurrentMultiStreamIngest(t *testing.T, client HTTPClient, streams []string, eventsPerStream int) {
	t.Helper()
	var wg sync.WaitGroup
	var errors int64

	for _, stream := range streams {
		wg.Add(1)
		go func(s string) {
			defer wg.Done()
			for i := 0; i < eventsPerStream; i++ {
				payload := fmt.Sprintf(`[{"level":"info","message":"concurrent multi-stream test %d","host":"test-host-%s"}]`, i, s)
				req, _ := client.NewRequest("POST", "ingest", bytes.NewBufferString(payload))
				req.Header.Add("X-P-Stream", s)
				resp, err := client.Do(req)
				if err != nil {
					atomic.AddInt64(&errors, 1)
					continue
				}
				resp.Body.Close()
				if resp.StatusCode >= 500 {
					atomic.AddInt64(&errors, 1)
				}
			}
		}(stream)
	}

	wg.Wait()
	require.Equalf(t, int64(0), errors, "Concurrent multi-stream ingest had %d 500-level errors", errors)
}

func QueryLogStreamCountMinimum(t *testing.T, client HTTPClient, stream string, minCount int) {
	t.Helper()
	endTime := time.Now().Add(time.Second).Format(time.RFC3339Nano)
	startTime := time.Now().Add(-30 * time.Minute).Format(time.RFC3339Nano)

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
	require.Equalf(t, 200, response.StatusCode, "Query failed: %s, body: %s", response.Status, body)

	var results []map[string]float64
	err = json.Unmarshal([]byte(body), &results)
	require.NoErrorf(t, err, "Failed to parse query response: %s", err)
	require.NotEmptyf(t, results, "Query returned no results")
	count := int(results[0]["count"])
	require.GreaterOrEqualf(t, count, minCount, "Expected at least %d events in %s, got %d", minCount, stream, count)
}
