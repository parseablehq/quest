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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	httpclient "quest/tests/integration/clients/http"
	"quest/tests/integration/clients/pb"
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

type PBDataset struct {
	Title string `json:"title"`
}

type PBDatasetInfo struct {
	DatasetType string            `json:"dataset_type"`
	Retention   []PBRetentionRule `json:"retention"`
}

type PBRetentionRule struct {
	Description string `json:"description"`
	Action      string `json:"action"`
	Duration    string `json:"duration"`
}

func CreateStream(t *testing.T, client pb.PBClient, dataset string) {
	t.Helper()
	result, err := client.Run(context.Background(), "dataset", "add", dataset, "--type", "logs")
	require.NoErrorf(t, err, "pb dataset add failed (exit=%d, stdout=%q, stderr=%q)", result.ExitCode, result.Stdout, result.Stderr)
}

func ListDatasetsWithPB(t *testing.T, client pb.PBClient) []PBDataset {
	t.Helper()
	var datasets []PBDataset
	result, err := client.RunJSON(context.Background(), &datasets, "dataset", "list")
	require.NoErrorf(t, err, "pb dataset list failed (exit=%d, stdout=%q, stderr=%q)", result.ExitCode, result.Stdout, result.Stderr)
	return datasets
}

func DatasetInfoWithPB(t *testing.T, client pb.PBClient, dataset string) PBDatasetInfo {
	t.Helper()
	var info PBDatasetInfo
	result, err := client.RunJSON(context.Background(), &info, "dataset", "info", dataset)
	require.NoErrorf(t, err, "pb dataset info failed (exit=%d, stdout=%q, stderr=%q)", result.ExitCode, result.Stdout, result.Stderr)
	return info
}

func DeleteStream(t *testing.T, client pb.PBClient, dataset string) {
	t.Helper()
	result, err := client.Run(context.Background(), "dataset", "remove", dataset)
	require.NoErrorf(t, err, "pb dataset remove failed (exit=%d, stdout=%q, stderr=%q)", result.ExitCode, result.Stdout, result.Stderr)
}

func CreateStreamWithHeader(t *testing.T, client httpclient.HTTPClient, stream string, header map[string]string) {
	req, _ := client.NewRequest("PUT", "logstream/"+stream, nil)
	for k, v := range header {
		req.Header.Add(k, v)
	}
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
}

func CreateStreamWithCustompartitionError(t *testing.T, client httpclient.HTTPClient, stream string, header map[string]string) {
	req, _ := client.NewRequest("PUT", "logstream/"+stream, nil)
	for k, v := range header {
		req.Header.Add(k, v)
	}
	response, _ := client.Do(req)
	require.Equalf(t, 500, response.StatusCode, "Server returned http code: %s", response.Status)
}

func CreateStreamWithSchemaBody(t *testing.T, client httpclient.HTTPClient, stream string, header map[string]string, schema_payload string) {

	req, _ := client.NewRequest("PUT", "logstream/"+stream, bytes.NewBufferString(schema_payload))
	for k, v := range header {
		req.Header.Add(k, v)
	}
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
}

func DetectSchema(t *testing.T, client httpclient.HTTPClient, sampleJson string, schemaBody string) {
	req, _ := client.NewRequest("POST", "logstream/schema/detect", bytes.NewBufferString(sampleJson))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	require.JSONEq(t, schemaBody, body, "Schema detection failed")
}

func DeleteAlert(t *testing.T, client httpclient.HTTPClient, alert_id string) {
	req, _ := client.NewRequest("DELETE", "alerts/"+alert_id, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
}

func DeleteTarget(t *testing.T, client httpclient.HTTPClient, target_id string) {
	req, _ := client.NewRequest("DELETE", "targets/"+target_id, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
}

func RunFlog(t *testing.T, client httpclient.HTTPClient, stream string) {
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

func IngestOneEventWithTimePartition_TimeStampMismatch(t *testing.T, client httpclient.HTTPClient, stream string) {
	var test_payload string = `{"source_time":"2024-03-26T18:08:00.434Z","level":"info","message":"Application is failing","version":"1.2.0","user_id":13912,"device_id":4138,"session_id":"abc","os":"Windows","host":"112.168.1.110","location":"ngeuprqhynuvpxgp","request_body":"rnkmffyawtdcindtrdqruyxbndbjpfsptzpwtujbmkwcqastmxwbvjwphmyvpnhordwljnodxhtvpjesjldtifswqbpyuhlcytmm","status_code":300,"app_meta":"ckgpibhmlusqqfunnpxbfxbc", "new_field_added_by":"ingestor 8020"}`
	req, _ := client.NewRequest("POST", "ingest", bytes.NewBufferString(test_payload))
	req.Header.Add("X-P-Stream", stream)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 400, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))
}

func IngestOneEventWithTimePartition_NoTimePartitionInLog(t *testing.T, client httpclient.HTTPClient, stream string) {
	var test_payload string = `{"level":"info","message":"Application is failing","version":"1.2.0","user_id":13912,"device_id":4138,"session_id":"abc","os":"Windows","host":"112.168.1.110","location":"ngeuprqhynuvpxgp","request_body":"rnkmffyawtdcindtrdqruyxbndbjpfsptzpwtujbmkwcqastmxwbvjwphmyvpnhordwljnodxhtvpjesjldtifswqbpyuhlcytmm","status_code":300,"app_meta":"ckgpibhmlusqqfunnpxbfxbc", "new_field_added_by":"ingestor 8020"}`
	req, _ := client.NewRequest("POST", "ingest", bytes.NewBufferString(test_payload))
	req.Header.Add("X-P-Stream", stream)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 400, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))
}

func IngestOneEventWithTimePartition_IncorrectDateTimeFormatTimePartitionInLog(t *testing.T, client httpclient.HTTPClient, stream string) {
	var test_payload string = `{"source_time":"2024-03-26", "level":"info","message":"Application is failing","version":"1.2.0","user_id":13912,"device_id":4138,"session_id":"abc","os":"Windows","host":"112.168.1.110","location":"ngeuprqhynuvpxgp","request_body":"rnkmffyawtdcindtrdqruyxbndbjpfsptzpwtujbmkwcqastmxwbvjwphmyvpnhordwljnodxhtvpjesjldtifswqbpyuhlcytmm","status_code":300,"app_meta":"ckgpibhmlusqqfunnpxbfxbc", "new_field_added_by":"ingestor 8020"}`
	req, _ := client.NewRequest("POST", "ingest", bytes.NewBufferString(test_payload))
	req.Header.Add("X-P-Stream", stream)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 400, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))
}

func IngestOneEventForStaticSchemaStream_NewFieldInLog(t *testing.T, client httpclient.HTTPClient, stream string) {
	var test_payload string = `{"source_time":"2024-03-26", "level":"info","message":"Application is failing","version":"1.2.0","user_id":13912,"device_id":4138,"session_id":"abc","os":"Windows","host":"112.168.1.110","location":"ngeuprqhynuvpxgp","request_body":"rnkmffyawtdcindtrdqruyxbndbjpfsptzpwtujbmkwcqastmxwbvjwphmyvpnhordwljnodxhtvpjesjldtifswqbpyuhlcytmm","status_code":300,"app_meta":"ckgpibhmlusqqfunnpxbfxbc", "new_field_added_by":"ingestor 8020"}`
	req, _ := client.NewRequest("POST", "ingest", bytes.NewBufferString(test_payload))
	req.Header.Add("X-P-Stream", stream)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 400, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))
}

func IngestOneEventForStaticSchemaStream_SameFieldsInLog(t *testing.T, client httpclient.HTTPClient, stream string) {
	var test_payload string = `{"source_time":"2024-03-26", "level":"info","message":"Application is failing","version":"1.2.0","user_id":13912,"device_id":4138,"session_id":"abc","os":"Windows","host":"112.168.1.110","location":"ngeuprqhynuvpxgp","request_body":"rnkmffyawtdcindtrdqruyxbndbjpfsptzpwtujbmkwcqastmxwbvjwphmyvpnhordwljnodxhtvpjesjldtifswqbpyuhlcytmm","status_code":300,"app_meta":"ckgpibhmlusqqfunnpxbfxbc"}`
	req, _ := client.NewRequest("POST", "ingest", bytes.NewBufferString(test_payload))
	req.Header.Add("X-P-Stream", stream)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))
}

func runSQLWithPB(t *testing.T, client pb.PBClient, query, startTime, endTime string, output any) {
	t.Helper()
	result, err := client.RunJSON(
		context.Background(),
		output,
		"sql", "run", query,
		"--from", startTime,
		"--to", endTime,
	)
	require.NoErrorf(t, err, "pb sql run failed (exit=%d, stdout=%q, stderr=%q)", result.ExitCode, result.Stdout, result.Stderr)
}

type PBCountRow struct {
	Count uint64 `json:"count"`
}

func QueryLogStreamCount(t *testing.T, client pb.PBClient, stream string, count uint64) {
	// Query last 30 minutes of data only
	endTime := time.Now().Add(time.Second).Format(time.RFC3339Nano)
	startTime := time.Now().Add(-30 * time.Minute).Format(time.RFC3339Nano)

	query := "select count(*) as count from " + stream
	var rows []PBCountRow
	runSQLWithPB(t, client, query, startTime, endTime, &rows)
	require.Equalf(t, []PBCountRow{{Count: count}}, rows, "Query count incorrect; Expected %d, Actual %v", count, rows)
}

func QueryLogStreamCount_Historical(t *testing.T, client pb.PBClient, stream string, count uint64) {
	// Query last 30 minutes of data only
	now := time.Now()
	startTime := now.AddDate(0, 0, -33).Format(time.RFC3339Nano)
	endTime := now.AddDate(0, 0, -27).Format(time.RFC3339Nano)

	query := "select count(*) as count from " + stream
	var rows []PBCountRow
	runSQLWithPB(t, client, query, startTime, endTime, &rows)
	require.Equalf(t, []PBCountRow{{Count: count}}, rows, "Query count incorrect; Expected %d, Actual %v", count, rows)
}

func QueryTwoLogStreamCount(t *testing.T, client pb.PBClient, stream1 string, stream2 string, count uint64) {
	// Query last 30 minutes of data only
	endTime := time.Now().Add(time.Second).Format(time.RFC3339Nano)
	startTime := time.Now().Add(-30 * time.Minute).Format(time.RFC3339Nano)

	query := fmt.Sprintf("select sum(c) as count from (select count(*) as c from %s union all select count(*) as c from %s)", stream1, stream2)
	var rows []PBCountRow
	runSQLWithPB(t, client, query, startTime, endTime, &rows)
	require.Equalf(t, []PBCountRow{{Count: count}}, rows, "Query count incorrect; Expected %d, Actual %v", count, rows)
}

func AssertQueryOK(t *testing.T, client pb.PBClient, query string, args ...any) {
	// Query last 30 minutes of data only
	endTime := time.Now().Add(time.Second).Format(time.RFC3339Nano)
	startTime := time.Now().Add(-30 * time.Minute).Format(time.RFC3339Nano)

	var finalQuery string
	if len(args) == 0 {
		finalQuery = query
	} else {
		finalQuery = fmt.Sprintf(query, args...)
	}

	var rows []json.RawMessage
	runSQLWithPB(t, client, finalQuery, startTime, endTime, &rows)
}

func AssertStreamSchema(t *testing.T, client httpclient.HTTPClient, stream string, schema string) {
	req, _ := client.NewRequest("GET", "logstream/"+stream+"/schema", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	require.JSONEq(t, schema, body, "Get schema response doesn't match with expected schema")
}

func CreateRole(t *testing.T, client httpclient.HTTPClient, name string, role string) {
	req, _ := client.NewRequest("PUT", "role/"+name, strings.NewReader(role))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func AssertRole(t *testing.T, client httpclient.HTTPClient, name string, role string) {
	req, _ := client.NewRequest("GET", "role/"+name, nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	require.JSONEq(t, role, body, "Get role response doesn't match with retention config returned")
}

func CreateUserWithRole(t *testing.T, client pb.PBClient, user string, roles []string) string {
	t.Helper()
	result, err := client.Run(context.Background(), "user", "add", user, "--role", strings.Join(roles, ","))
	require.NoErrorf(t, err, "pb user add failed (exit=%d, stdout=%q, stderr=%q)", result.ExitCode, result.Stdout, result.Stderr)
	password, err := pb.PasswordFromUserAddOutput(result.Stdout)
	require.NoErrorf(t, err, "pb user add returned no password (stdout=%q, stderr=%q)", result.Stdout, result.Stderr)
	return password
}

func AssertUserRole(t *testing.T, client httpclient.HTTPClient, user string, roleName, roleBody string) {
	req, _ := client.NewRequest("GET", "user/"+user+"/role", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	userRoleBody := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, userRoleBody)
	expectedRoleBody := fmt.Sprintf(`{"roles":{"%s":%s}, "group_roles": {}}`, roleName, roleBody)
	require.JSONEq(t, userRoleBody, expectedRoleBody, "Get user role response doesn't match with expected role")
}

func RegenPassword(t *testing.T, client httpclient.HTTPClient, user string) string {
	req, _ := client.NewRequest("POST", "user/"+user+"/generate-new-password", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	return body
}

func DeleteUser(t *testing.T, client pb.PBClient, user string) {
	t.Helper()
	result, err := client.Run(context.Background(), "user", "remove", user)
	require.NoErrorf(t, err, "pb user remove failed (exit=%d, stdout=%q, stderr=%q)", result.ExitCode, result.Stdout, result.Stderr)
}

func DeleteRole(t *testing.T, client pb.PBClient, roleName string) {
	t.Helper()
	result, err := client.Run(context.Background(), "role", "remove", roleName)
	require.NoErrorf(t, err, "pb role remove failed (exit=%d, stdout=%q, stderr=%q)", result.ExitCode, result.Stdout, result.Stderr)
}

func PutSingleEvent(t *testing.T, client httpclient.HTTPClient, stream string) {
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

func checkAPIAccess(t *testing.T, queryClient httpclient.HTTPClient, ingestClient httpclient.HTTPClient, stream string, role string) {
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
