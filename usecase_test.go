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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// helper to create a target and return its ID
func createTargetGetID(t *testing.T, client HTTPClient) string {
	t.Helper()
	req, _ := client.NewRequest("POST", "targets", strings.NewReader(getTargetBody()))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Create target failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Create target failed: %s", response.Status)

	targetsResp := ListTargets(t, client)
	return getIdFromTargetResponse(bytes.NewReader([]byte(targetsResp)))
}

// helper to create an alert and return its ID
func createAlertGetID(t *testing.T, client HTTPClient, stream, targetId string) string {
	t.Helper()
	alertBody := getAlertBody(stream, targetId)
	req, _ := client.NewRequest("POST", "alerts", strings.NewReader(alertBody))
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Create alert failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Create alert failed: %s", response.Status)

	req, _ = client.NewRequest("GET", "alerts", nil)
	response, err = client.Do(req)
	require.NoErrorf(t, err, "List alerts failed: %s", err)
	bodyAlerts, _ := io.ReadAll(response.Body)
	alertId, _, _, _ := getMetadataFromAlertResponse(bytes.NewReader(bodyAlerts))
	return alertId
}

// helper to build a user client for queries
func userQueryClient(username, password string) HTTPClient {
	c := NewGlob.QueryClient
	c.Username = username
	c.Password = password
	return c
}

// helper to build a user client for ingest
func userIngestClient(username, password string) HTTPClient {
	if NewGlob.IngestorUrl.String() != "" {
		c := NewGlob.IngestorClient
		c.Username = username
		c.Password = password
		return c
	}
	c := NewGlob.QueryClient
	c.Username = username
	c.Password = password
	return c
}

// UC1: DevOps engineer onboards a new microservice onto Parseable.
func TestUseCase_DevOpsNewMicroserviceOnboarding(t *testing.T) {
	stream := UniqueStream("uc1_microservice")
	client := NewGlob.QueryClient
	rt := NewResourceTracker(t, client)

	// 1. Create stream
	CreateStream(t, client, stream)
	rt.TrackStream(stream)

	// 2. Ingest 50 structured logs via flog
	RunFlogAuto(t, stream)

	// 3. Wait for ingest
	WaitForIngest(t, client, stream, 50, 180*time.Second)

	// 4. Verify schema matches expected
	AssertStreamSchema(t, client, stream, FlogJsonSchema)

	// 5. Set retention policy (20-day delete)
	SetRetention(t, client, stream, RetentionBody)

	// 6. Verify retention applied
	AssertRetention(t, client, stream, RetentionBody)

	// 7. Create alert target + alert on error level
	targetId := createTargetGetID(t, client)
	rt.TrackTarget(targetId)
	alertId := createAlertGetID(t, client, stream, targetId)
	rt.TrackAlert(alertId)

	// 8. Create dashboard + add tile for the stream
	dashboardId := CreateDashboard(t, client)
	rt.TrackDashboard(dashboardId)
	AddDashboardTile(t, client, dashboardId, stream)

	// 9. Create saved filter for error queries
	filterId := CreateFilter(t, client, stream)
	rt.TrackFilter(filterId)

	// 10. Verify via Prism (logstream info + datasets)
	AssertPrismLogstreamInfo(t, client, stream)
	AssertPrismDatasets(t, client, stream)
}

// UC2: SRE investigating production incident spanning two services.
func TestUseCase_SREIncidentInvestigation(t *testing.T) {
	stream1 := UniqueStream("uc2_frontend")
	stream2 := UniqueStream("uc2_backend")
	client := NewGlob.QueryClient
	rt := NewResourceTracker(t, client)

	// 1. Create two streams
	CreateStream(t, client, stream1)
	rt.TrackStream(stream1)
	CreateStream(t, client, stream2)
	rt.TrackStream(stream2)

	// 2. Ingest flog data into both
	RunFlogAuto(t, stream1)
	RunFlogAuto(t, stream2)

	// 3. Wait for ingest
	WaitForIngest(t, client, stream1, 50, 180*time.Second)
	WaitForIngest(t, client, stream2, 50, 180*time.Second)

	// 4. Cross-stream union query
	QueryTwoLogStreamCount(t, client, stream1, stream2, 100)

	// 5. Run analytical queries (GROUP BY, DISTINCT on flog fields)
	AssertQueryOK(t, client, "SELECT method, COUNT(*) as cnt FROM %s GROUP BY method", stream1)
	AssertQueryOK(t, client, "SELECT DISTINCT host FROM %s", stream2)

	// 6. Create correlation (inner join on host)
	correlationId := CreateCorrelation(t, client, stream1, stream2)
	rt.TrackCorrelation(correlationId)

	// 7. Verify correlation via GetById
	GetCorrelationById(t, client, correlationId)

	// 8. Get dataset stats for both streams
	AssertDatasetStats(t, client, []string{stream1, stream2})
}

// UC3: Platform team manages per-team stream access with multi-privilege roles.
func TestUseCase_PlatformTeamMultiTenantRBAC(t *testing.T) {
	streamA := UniqueStream("uc3_tenant_a")
	streamB := UniqueStream("uc3_tenant_b")
	client := NewGlob.QueryClient
	rt := NewResourceTracker(t, client)

	// 1. Create two streams
	CreateStream(t, client, streamA)
	rt.TrackStream(streamA)
	CreateStream(t, client, streamB)
	rt.TrackStream(streamB)

	// 2. Create role "team_a_role": writer on tenant_a + reader on tenant_b
	teamARoleName := UniqueStream("team_a_role")
	teamARoleBody := getMultiPrivilegeRoleBody(streamA, streamB)
	CreateRole(t, client, teamARoleName, teamARoleBody)
	rt.TrackRole(teamARoleName)

	// 3. Create role "team_b_role": writer on tenant_b + reader on tenant_a
	teamBRoleName := UniqueStream("team_b_role")
	teamBRoleBody := getMultiPrivilegeRoleBody(streamB, streamA)
	CreateRole(t, client, teamBRoleName, teamBRoleBody)
	rt.TrackRole(teamBRoleName)

	// 4. Create user per role
	teamAUserName := UniqueStream("team_a_user")
	teamBUserName := UniqueStream("team_b_user")
	teamAPassword := CreateUserWithRole(t, client, teamAUserName, []string{teamARoleName})
	rt.TrackUser(teamAUserName)
	teamBPassword := CreateUserWithRole(t, client, teamBUserName, []string{teamBRoleName})
	rt.TrackUser(teamBUserName)

	// 5. Build client for team_a user
	teamAQuery := userQueryClient(teamAUserName, teamAPassword)
	teamAIngest := userIngestClient(teamAUserName, teamAPassword)

	// 6. Assert team_a can ingest to tenant_a (200)
	IngestCustomPayload(t, teamAIngest, streamA, `[{"level":"info","message":"team_a log","host":"10.0.0.1"}]`, 200)

	// 7. Assert team_a cannot delete tenant_a (403)
	AssertForbidden(t, teamAQuery, "DELETE", "logstream/"+streamA, nil)

	// 8. Assert team_a can read tenant_b but cannot ingest (403)
	AssertQueryOK(t, teamAQuery, "SELECT COUNT(*) as count FROM %s", streamB)
	IngestCustomPayload(t, teamAIngest, streamB, `[{"level":"info","message":"should fail","host":"10.0.0.2"}]`, 403)

	// 9. Build client for team_b user
	teamBIngest := userIngestClient(teamBUserName, teamBPassword)

	// 10. Assert team_b can ingest to tenant_b (200)
	IngestCustomPayload(t, teamBIngest, streamB, `[{"level":"info","message":"team_b log","host":"10.0.0.3"}]`, 200)

	// 11. Assert team_b cannot ingest to tenant_a (403)
	IngestCustomPayload(t, teamBIngest, streamA, `[{"level":"info","message":"should fail","host":"10.0.0.4"}]`, 403)

	// 12. Add writer role for tenant_b to team_a via PATCH
	writerBRoleName := UniqueStream("uc3_writer_b")
	writerBRoleBody := RoleWriter(streamB)
	CreateRole(t, client, writerBRoleName, writerBRoleBody)
	rt.TrackRole(writerBRoleName)
	AddRolesToUser(t, client, teamAUserName, []string{writerBRoleName})

	// 13. Verify team_a can NOW ingest to tenant_b (200)
	IngestCustomPayload(t, teamAIngest, streamB, `[{"level":"info","message":"team_a now can write to b","host":"10.0.0.5"}]`, 200)

	// 14. Remove the added role
	RemoveRolesFromUser(t, client, teamAUserName, []string{writerBRoleName})
}

// UC4: Security auditor verifying RBAC enforcement per role type.
func TestUseCase_SecurityAuditAccessControls(t *testing.T) {
	stream := UniqueStream("uc4_audit")
	client := NewGlob.QueryClient
	rt := NewResourceTracker(t, client)

	// 1. Create stream, ingest data, wait for sync
	CreateStream(t, client, stream)
	rt.TrackStream(stream)
	RunFlogAuto(t, stream)
	WaitForIngest(t, client, stream, 50, 180*time.Second)

	// 2. Create four roles
	editorRole := UniqueStream("uc4_editor")
	writerRole := UniqueStream("uc4_writer")
	readerRole := UniqueStream("uc4_reader")
	ingestorRole := UniqueStream("uc4_ingestor")
	CreateRole(t, client, editorRole, RoleEditor)
	rt.TrackRole(editorRole)
	CreateRole(t, client, writerRole, RoleWriter(stream))
	rt.TrackRole(writerRole)
	CreateRole(t, client, readerRole, RoleReader(stream))
	rt.TrackRole(readerRole)
	CreateRole(t, client, ingestorRole, Roleingestor(stream))
	rt.TrackRole(ingestorRole)

	// 3. Create four users, one per role
	editorUser := UniqueStream("uc4_editor_u")
	writerUser := UniqueStream("uc4_writer_u")
	readerUser := UniqueStream("uc4_reader_u")
	ingestorUser := UniqueStream("uc4_ingestor_u")
	editorPass := CreateUserWithRole(t, client, editorUser, []string{editorRole})
	rt.TrackUser(editorUser)
	writerPass := CreateUserWithRole(t, client, writerUser, []string{writerRole})
	rt.TrackUser(writerUser)
	readerPass := CreateUserWithRole(t, client, readerUser, []string{readerRole})
	rt.TrackUser(readerUser)
	ingestorPass := CreateUserWithRole(t, client, ingestorUser, []string{ingestorRole})
	rt.TrackUser(ingestorUser)

	// 4. Build clients
	readerQ := userQueryClient(readerUser, readerPass)
	readerI := userIngestClient(readerUser, readerPass)
	writerQ := userQueryClient(writerUser, writerPass)
	writerI := userIngestClient(writerUser, writerPass)
	ingestorQ := userQueryClient(ingestorUser, ingestorPass)
	ingestorI := userIngestClient(ingestorUser, ingestorPass)
	editorQ := userQueryClient(editorUser, editorPass)
	editorI := userIngestClient(editorUser, editorPass)

	// 5. Reader: can query (200), cannot ingest (403), cannot delete (403)
	AssertQueryOK(t, readerQ, "SELECT COUNT(*) FROM %s", stream)
	IngestCustomPayload(t, readerI, stream, `[{"host":"test"}]`, 403)
	AssertForbidden(t, readerQ, "DELETE", "logstream/"+stream, nil)

	// 6. Writer: can query (200), can ingest (200), cannot delete (403)
	AssertQueryOK(t, writerQ, "SELECT COUNT(*) FROM %s", stream)
	IngestCustomPayload(t, writerI, stream, `[{"host":"test"}]`, 200)
	AssertForbidden(t, writerQ, "DELETE", "logstream/"+stream, nil)

	// 7. Ingestor: can ingest (200), cannot query (403)
	IngestCustomPayload(t, ingestorI, stream, `[{"host":"test"}]`, 200)
	AssertQueryError(t, ingestorQ, "SELECT COUNT(*) FROM "+stream, 403)

	// 8. Editor: can do all operations (200)
	AssertQueryOK(t, editorQ, "SELECT COUNT(*) FROM %s", stream)
	IngestCustomPayload(t, editorI, stream, `[{"host":"test"}]`, 200)
	AssertStreamInfo(t, editorQ, stream)

	// 9. Test default role assignment behavior
	SetDefaultRole(t, client, readerRole)
	AssertDefaultRole(t, client, fmt.Sprintf(`"%s"`, readerRole))

	// 10. Verify listing all roles includes the four created
	rolesBody := ListAllRoles(t, client)
	require.Contains(t, rolesBody, editorRole)
	require.Contains(t, rolesBody, writerRole)
	require.Contains(t, rolesBody, readerRole)
	require.Contains(t, rolesBody, ingestorRole)
}

// UC5: DevOps engineer sets up complete alerting pipeline with real data.
func TestUseCase_AlertLifecycleWithDataIngestion(t *testing.T) {
	stream := UniqueStream("uc5_alertpipe")
	client := NewGlob.QueryClient
	rt := NewResourceTracker(t, client)

	// 1. Create stream, ingest data with known patterns
	CreateStream(t, client, stream)
	rt.TrackStream(stream)
	RunFlogAuto(t, stream)

	// 2. Wait for ingest, verify data queryable
	WaitForIngest(t, client, stream, 50, 180*time.Second)

	// 3. Create webhook target
	targetId := createTargetGetID(t, client)
	rt.TrackTarget(targetId)

	// 4. Create alert with threshold on the stream
	alertId := createAlertGetID(t, client, stream, targetId)
	rt.TrackAlert(alertId)

	// 5. Get alert by ID, verify state field present
	alertDetails := GetAlertById(t, client, alertId)
	require.Contains(t, alertDetails, `"state"`, "Alert response should contain state field")

	// 6. Modify alert (change severity, threshold)
	modifyBody := getAlertModifyBody(stream, targetId)
	ModifyAlert(t, client, alertId, modifyBody)

	// 7. Get alert again, verify fields updated
	alertDetails = GetAlertById(t, client, alertId)
	require.Contains(t, alertDetails, "Modified", "Alert title should reflect modification")

	// 8. Disable alert, verify state
	DisableAlert(t, client, alertId)

	// 9. Enable alert, verify state
	EnableAlert(t, client, alertId)

	// 10. Evaluate alert
	req, _ := client.NewRequest("PUT", "alerts/"+alertId+"/evaluate_alert", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Evaluate alert request failed: %s", err)
	t.Logf("Evaluate alert returned status: %d", response.StatusCode)

	// 11. List alert tags, verify "quest-test" tag exists
	tagsBody := ListAlertTags(t, client)
	require.Contains(t, tagsBody, "quest-test", "Alert tags should include quest-test")

	// 12. Get stream info + stats to confirm ingestion metrics
	AssertStreamInfo(t, client, stream)
	AssertStreamStats(t, client, stream)
}

// UC6: Platform engineer sets up OTel-based observability for all signal types.
func TestUseCase_OTelFullPipelineIngestion(t *testing.T) {
	client := NewGlob.QueryClient

	// 1. Health check (liveness + readiness)
	AssertLiveness(t, client)
	AssertReadiness(t, client)

	// 2. Ingest OTel logs via /v1/logs
	IngestOTelLogs(t, client)

	// 3. Ingest OTel traces via /v1/traces
	IngestOTelTraces(t, client)

	// 4. Ingest OTel metrics via /v1/metrics
	IngestOTelMetrics(t, client)

	// 5. Wait for OTel data to land
	time.Sleep(5 * time.Second)

	// 6. Discover auto-created OTel streams
	otelStreams := DiscoverOTelStreams(t, client)
	t.Logf("Discovered %d OTel streams: %v", len(otelStreams), otelStreams)

	// 7. Query each OTel stream to verify data exists
	for _, stream := range otelStreams {
		AssertQueryOK(t, client, "SELECT COUNT(*) as count FROM \"%s\"", stream)
	}

	// 8. Get stream info for OTel streams
	for _, stream := range otelStreams {
		AssertStreamInfo(t, client, stream)
	}

	// 9. Check dataset stats for OTel streams
	if len(otelStreams) > 0 {
		AssertDatasetStats(t, client, otelStreams)
	}
}

// UC7: Data engineer managing data lifecycle and tiering.
func TestUseCase_DataEngineerRetentionAndHotTier(t *testing.T) {
	stream := UniqueStream("uc7_retention")
	client := NewGlob.QueryClient
	rt := NewResourceTracker(t, client)

	// 1. Create stream, ingest data
	CreateStream(t, client, stream)
	rt.TrackStream(stream)
	RunFlogAuto(t, stream)

	// 2. Wait for ingest
	WaitForIngest(t, client, stream, 50, 180*time.Second)

	// 3. Set retention policy
	SetRetention(t, client, stream, RetentionBody)

	// 4. Verify retention applied
	AssertRetention(t, client, stream, RetentionBody)

	// 5. Get stream info and stats
	AssertStreamInfo(t, client, stream)
	AssertStreamStats(t, client, stream)

	// 6. Set hot tier config
	SetStreamHotTier(t, client, stream)

	// 7. Verify hot tier config
	GetStreamHotTier(t, client, stream)

	// 8. Query again to ensure hot tier doesn't break queries
	AssertQueryOK(t, client, "SELECT COUNT(*) FROM %s", stream)

	// 9. Delete hot tier
	DeleteStreamHotTier(t, client, stream)
}

// UC8: Team lead builds a multi-stream monitoring dashboard.
func TestUseCase_DashboardDrivenMonitoringSetup(t *testing.T) {
	streamWeb := UniqueStream("uc8_web")
	streamAPI := UniqueStream("uc8_api")
	streamDB := UniqueStream("uc8_db")
	client := NewGlob.QueryClient
	rt := NewResourceTracker(t, client)

	// 1. Create three streams
	CreateStream(t, client, streamWeb)
	rt.TrackStream(streamWeb)
	CreateStream(t, client, streamAPI)
	rt.TrackStream(streamAPI)
	CreateStream(t, client, streamDB)
	rt.TrackStream(streamDB)

	// 2. Ingest data into all three
	RunFlogAuto(t, streamWeb)
	RunFlogAuto(t, streamAPI)
	RunFlogAuto(t, streamDB)

	// 3. Wait for ingest
	WaitForIngest(t, client, streamWeb, 50, 180*time.Second)
	WaitForIngest(t, client, streamAPI, 50, 180*time.Second)
	WaitForIngest(t, client, streamDB, 50, 180*time.Second)

	// 4. Create dashboard
	dashboardId := CreateDashboard(t, client)
	rt.TrackDashboard(dashboardId)

	// 5. Add tiles for each stream
	AddDashboardTile(t, client, dashboardId, streamWeb)
	AddDashboardTile(t, client, dashboardId, streamAPI)
	AddDashboardTile(t, client, dashboardId, streamDB)

	// 6. Update dashboard metadata
	UpdateDashboard(t, client, dashboardId)

	// 7. List dashboard tags, verify present
	tagsBody := ListDashboardTags(t, client)
	require.Contains(t, tagsBody, "quest-test", "Dashboard tags should include quest-test")

	// 8. Create saved filter for web errors
	filterId := CreateFilter(t, client, streamWeb)
	rt.TrackFilter(filterId)

	// 9. Verify Prism datasets for each stream
	AssertPrismDatasets(t, client, streamWeb)
	AssertPrismDatasets(t, client, streamAPI)
	AssertPrismDatasets(t, client, streamDB)

	// 10. Get dashboard by ID, verify structure
	dashBody := GetDashboardById(t, client, dashboardId)
	require.Contains(t, dashBody, "Quest Test Dashboard", "Dashboard should contain expected title")
}

// UC9: SRE correlates two streams with different schemas.
func TestUseCase_CorrelationAcrossDifferentSchemas(t *testing.T) {
	streamFlog := UniqueStream("uc9_flog")
	streamStructured := UniqueStream("uc9_struct")
	client := NewGlob.QueryClient
	rt := NewResourceTracker(t, client)

	// 1. Create flog stream (dynamic schema)
	CreateStream(t, client, streamFlog)
	rt.TrackStream(streamFlog)

	// 2. Create structured stream (dynamic schema, different shape)
	CreateStream(t, client, streamStructured)
	rt.TrackStream(streamStructured)

	// 3. Ingest flog data into first, structured event into second
	RunFlogAuto(t, streamFlog)
	ic := NewGlob.QueryClient
	if NewGlob.IngestorUrl.String() != "" {
		ic = NewGlob.IngestorClient
	}
	IngestCustomPayload(t, ic, streamStructured,
		`[{"host":"192.168.1.1","level":"error","message":"backend error","status_code":500}]`, 200)

	// 4. Wait for ingest
	WaitForIngest(t, client, streamFlog, 50, 180*time.Second)
	WaitForQueryable(t, client, streamStructured, 30*time.Second)

	// 5. Create correlation on shared field "host"
	correlationId := CreateCorrelationWithFields(t, client, streamFlog, streamStructured, "host", "host")
	rt.TrackCorrelation(correlationId)

	// 6. Verify correlation via GetById
	GetCorrelationById(t, client, correlationId)

	// 7. Modify correlation
	ModifyCorrelation(t, client, correlationId, streamFlog, streamStructured)

	// 8. Query both streams still work after correlation changes
	AssertQueryOK(t, client, "SELECT * FROM %s LIMIT 5", streamFlog)
	AssertQueryOK(t, client, "SELECT * FROM %s LIMIT 5", streamStructured)
}

// UC10: Developer's microservice starts sending logs with new fields.
func TestUseCase_StreamSchemaEvolution(t *testing.T) {
	dynamicStream := UniqueStream("uc10_dyn")
	staticStream := UniqueStream("uc10_static")
	client := NewGlob.QueryClient
	rt := NewResourceTracker(t, client)
	ic := NewGlob.QueryClient
	if NewGlob.IngestorUrl.String() != "" {
		ic = NewGlob.IngestorClient
	}

	// 1. Create dynamic stream + static schema stream
	CreateStream(t, client, dynamicStream)
	rt.TrackStream(dynamicStream)
	staticHeader := map[string]string{"X-P-Static-Schema-Flag": "true"}
	CreateStreamWithSchemaBody(t, client, staticStream, staticHeader, SchemaPayload)
	rt.TrackStream(staticStream)

	// 2. Ingest event matching schema to dynamic stream (200)
	matchingEvent := `{"source_time":"2024-03-26T00:00:00Z","level":"info","message":"test","version":"1.0","user_id":1,"device_id":1,"session_id":"a","os":"Linux","host":"10.0.0.1","uuid":"u1","location":"us","timezone":"UTC","user_agent":"test","runtime":"go","request_body":"body","status_code":200,"response_time":10,"process_id":1,"app_meta":"meta"}`
	IngestCustomPayload(t, ic, dynamicStream, matchingEvent, 200)

	// 3. Ingest event WITH NEW FIELD to dynamic stream (200, schema evolves)
	IngestCustomPayload(t, ic, dynamicStream, getDynamicSchemaEvent(), 200)

	// 4. Verify dynamic stream schema now includes new field
	WaitForQueryable(t, client, dynamicStream, 30*time.Second)
	req, _ := client.NewRequest("GET", "logstream/"+dynamicStream+"/schema", nil)
	response, err := client.Do(req)
	require.NoErrorf(t, err, "Get schema failed: %s", err)
	schemaBody := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Get schema failed: %s", response.Status)
	require.Contains(t, schemaBody, "extra_field", "Dynamic schema should include extra_field after evolution")

	// 5. Ingest matching event to static stream (200)
	IngestCustomPayload(t, ic, staticStream, matchingEvent, 200)

	// 6. Ingest event with new field to static stream (400, rejected)
	IngestCustomPayload(t, ic, staticStream, getDynamicSchemaEvent(), 400)

	// 7. Verify static stream schema unchanged
	req, _ = client.NewRequest("GET", "logstream/"+staticStream+"/schema", nil)
	response, err = client.Do(req)
	require.NoErrorf(t, err, "Get schema failed: %s", err)
	staticSchemaBody := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Get schema failed: %s", response.Status)
	require.NotContains(t, staticSchemaBody, "extra_field", "Static schema should NOT include extra_field")
}

// UC11: Multiple teams simultaneously ingesting and querying.
func TestUseCase_ConcurrentMultiTeamWorkload(t *testing.T) {
	streams := []string{UniqueStream("uc11_t1"), UniqueStream("uc11_t2"), UniqueStream("uc11_t3")}
	client := NewGlob.QueryClient
	rt := NewResourceTracker(t, client)
	ic := NewGlob.QueryClient
	if NewGlob.IngestorUrl.String() != "" {
		ic = NewGlob.IngestorClient
	}

	// 1. Create three team streams
	for _, s := range streams {
		CreateStream(t, client, s)
		rt.TrackStream(s)
	}

	// 2-4. Launch goroutines: ingest to all three simultaneously (20 events each)
	ConcurrentMultiStreamIngest(t, ic, streams, 20)

	// 5. Wait for ingest, verify each stream has data
	for _, s := range streams {
		WaitForIngest(t, client, s, 1, 30*time.Second)
	}

	// 6. Cross-stream query across two streams
	AssertQueryOK(t, client, "SELECT COUNT(*) as total FROM (SELECT * FROM %s UNION ALL SELECT * FROM %s)", streams[0], streams[1])
}

// UC12: Verifying distributed mode end-to-end.
func TestUseCase_DistributedIngestQueryPipeline(t *testing.T) {
	// 1. Skip if not distributed mode
	if NewGlob.IngestorUrl.String() == "" {
		t.Skip("Skipping distributed test: no ingestor URL configured (standalone mode)")
	}

	stream := UniqueStream("uc12_dist")
	client := NewGlob.QueryClient
	rt := NewResourceTracker(t, client)

	// 2. Verify cluster health (info + metrics)
	AssertClusterInfo(t, client)
	AssertClusterMetrics(t, client)

	// 3. Create stream via querier
	CreateStream(t, client, stream)
	rt.TrackStream(stream)

	// 4. Ingest via ingestor (RunFlog)
	RunFlog(t, NewGlob.IngestorClient, stream)

	// 5. Wait for ingest
	WaitForIngest(t, client, stream, 50, 180*time.Second)

	// 6. Run analytical queries (GROUP BY, DISTINCT, ORDER BY) via querier
	AssertQueryOK(t, client, "SELECT method, COUNT(*) as cnt FROM %s GROUP BY method", stream)
	AssertQueryOK(t, client, "SELECT DISTINCT host FROM %s", stream)
	AssertQueryOK(t, client, "SELECT * FROM %s ORDER BY p_timestamp DESC LIMIT 10", stream)

	// 7. Verify stream info via querier
	AssertStreamInfo(t, client, stream)

	// 8. Verify Prism home via querier
	AssertPrismHome(t, client)
}

// UC13: Security test verifying users cannot escalate privileges.
func TestUseCase_RBACRoleEscalationPrevention(t *testing.T) {
	stream := UniqueStream("uc13_escalation")
	client := NewGlob.QueryClient
	rt := NewResourceTracker(t, client)

	// 1. Create stream
	CreateStream(t, client, stream)
	rt.TrackStream(stream)

	// 2. Create reader role for the stream
	roleName := UniqueStream("uc13_reader")
	CreateRole(t, client, roleName, RoleReader(stream))
	rt.TrackRole(roleName)

	// 3. Create reader user, get credentials
	userName := UniqueStream("uc13_reader_u")
	readerPass := CreateUserWithRole(t, client, userName, []string{roleName})
	rt.TrackUser(userName)

	// 4. Build reader client
	readerQ := userQueryClient(userName, readerPass)

	// 5. Reader tries to create a role -> expect 403
	AssertForbidden(t, readerQ, "PUT", "role/uc13_hack_role", strings.NewReader(RoleEditor))

	// 6. Reader tries to create a user -> expect 403
	AssertForbidden(t, readerQ, "POST", "user/uc13_hack_user", nil)

	// 7. Reader tries to delete stream -> expect 403
	AssertForbidden(t, readerQ, "DELETE", "logstream/"+stream, nil)

	// 8. Reader tries to create alert -> expect 403
	alertPayload := fmt.Sprintf(`{
		"severity": "low",
		"title": "HackAlert",
		"query": "select count(*) from %s",
		"alertType": "threshold",
		"thresholdConfig": {"operator": "=", "value": 1},
		"evalConfig": {"rollingWindow": {"evalStart": "5m", "evalEnd": "now", "evalFrequency": 1}},
		"notificationConfig": {"interval": 1},
		"targets": [],
		"tags": ["hack"]
	}`, stream)
	AssertForbidden(t, readerQ, "POST", "alerts", strings.NewReader(alertPayload))

	// 9. Reader tries to set retention -> expect 403
	AssertForbidden(t, readerQ, "PUT", "logstream/"+stream+"/retention", strings.NewReader(RetentionBody))
}
