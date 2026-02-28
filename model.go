// Copyright (c) 2023 Cloudnatively Services Pvt Ltd
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
	"encoding/json"
	"fmt"
	"io"
	"time"
)

const SchemaPayload string = `{
  "fields":[
   {
     "name": "source_time",
     "data_type": "string"
   },
   {
     "name": "level",
     "data_type": "string"
   },
   {
     "name": "message",
     "data_type": "string"
   },
       {
     "name": "version",
     "data_type": "string"
   },
   {
     "name": "user_id",
     "data_type": "int"
   },
   {
     "name": "device_id",
     "data_type": "int"
   },
   {
     "name": "session_id",
     "data_type": "string"
   },
   {
     "name": "os",
     "data_type": "string"
   },
   {
     "name": "host",
     "data_type": "string"
   },
   {
     "name": "uuid",
     "data_type": "string"
   },
   {
     "name": "location",
     "data_type": "string"
   },
   {
     "name": "timezone",
     "data_type": "string"
   },
   {
     "name": "user_agent",
     "data_type": "string"
   },
   {
     "name": "runtime",
     "data_type": "string"
   },
   {
     "name": "request_body",
     "data_type": "string"
   },
   {
     "name": "status_code",
     "data_type": "int"
   },
   {
     "name": "response_time",
     "data_type": "int"
   },
   {
     "name": "process_id",
     "data_type": "int"
   },
   {
     "name": "app_meta",
     "data_type": "string"
   }
 ]
 }`

const SampleJson string = `
{
        "app_meta": "bkfmqbmmjzbhkxdjzzlaebqp",
        "device_id": 42,
        "host": "112.168.1.110",
        "level": "warn",
        "location": "ffxkmbwbtxplhgnz",
        "message": "Logging a request",
        "meta-source": "quest-smoke-test",
        "meta-test": "Fixed-Logs",
        "os": "Linux",
        "p_src_ip": "127.0.0.1",
        "p_timestamp": "2024-10-27T05:13:26.744Z",
        "p_user_agent": "Mozilla/5.0",
        "process_id": 123,
        "request_body": "ffywhsbtsgvraxjuixlsxtrgotcahkicyxnaermtqmfgzlwbqkxqmonrwojmawsyxsovcjlbkbvjsesfznpukicdtghnvvirtauo",
        "response_time": 100,
        "runtime": "qld",
        "session_id": "pqr",
        "source_time": "2024-10-27T05:13:26.742Z",
        "status_code": 300,
        "timezone": "ftj",
        "user_agent":"OrangeOS",
        "user_id": 72278,
        "uuid": "d679e104-778d-4bbe-b9b6-e6f2b48922ad",
        "version": "1.1.0"
    }
`

const FlogJsonSchema string = `{
    "fields": [
        {
            "name": "bytes",
            "data_type": "Float64",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "datetime",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "host",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "method",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "p_src_ip",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "p_timestamp",
            "data_type": {
                "Timestamp": [
                    "Millisecond",
                    null
                ]
            },
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "p_user_agent",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "protocol",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "referer",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "request",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "status",
            "data_type": "Float64",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "user-identifier",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        }
    ],
    "metadata": {}
}`

const SchemaBody string = `{
    "fields": [
        {
            "name": "app_meta",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "device_id",
            "data_type": "Float64",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "host",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "level",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "location",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "message",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "meta-source",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "meta-test",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "os",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "p_src_ip",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "p_timestamp",
            "data_type": {
                "Timestamp": [
                    "Millisecond",
                    null
                ]
            },
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "p_user_agent",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "process_id",
            "data_type": "Float64",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "request_body",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "response_time",
            "data_type": "Float64",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "runtime",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "session_id",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "source_time",
            "data_type": {
                "Timestamp": [
                    "Millisecond",
                    null
                ]
            },
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "status_code",
            "data_type": "Float64",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "timezone",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "user_agent",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "user_id",
            "data_type": "Float64",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "uuid",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "version",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        }
    ],
    "metadata": {}
}`

const RetentionBody string = `[
  {
    "description": "delete after 20 days",
    "action": "delete",
    "duration": "20d"
  }
]`

const (
	TestUser  string = "alice"
	dummyRole string = `{"actions":[{"privilege": "editor"},{"privilege": "writer", "resource": {"stream": "app"}}], "roleType":"user"}`
)

const RoleEditor string = `{"actions":[{"privilege": "editor"}],"roleType":"user"}`

func RoleWriter(stream string) string {
	return fmt.Sprintf(`{"actions":[{"privilege": "writer", "resource": {"stream": "%s"}}],"roleType":"user"}`, stream)
}

func RoleReader(stream string) string {
	return fmt.Sprintf(`{"actions":[{"privilege": "reader", "resource": {"stream": "%s"}}],"roleType":"user"}`, stream)
}

func Roleingestor(stream string) string {
	return fmt.Sprintf(`{"actions":[{"privilege": "ingestor", "resource": {"stream": "%s"}}],"roleType":"user"}`, stream)
}

func getTargetBody() string {
	return `          {
              "name":"targetName",
              "type": "webhook",
              "endpoint": "https://webhook.site/ec627445-d52b-44e9-948d-56671df3581e",
              "headers": {},
              "skipTlsCheck": true
          }
`
}

func getIdFromTargetResponse(body io.Reader) string {
	type TargetConf struct {
		Type string `json:"type"`
		Id   string `json:"id"`
	}
	var response []TargetConf
	if err := json.NewDecoder(body).Decode(&response); err != nil {
		fmt.Printf("Error decoding: %v\n", err)
	}

	target := response[0]
	return target.Id
}

func getAlertBody(stream string, targetId string) string {
	return fmt.Sprintf(`
    {
      "severity": "medium",
      "title": "AlertTitle",
      "query": "select count(level) from %s where level = 'info'",
      "alertType": "threshold",
      "thresholdConfig": {
        "operator": "=",
        "value": 100
      },
      "anomalyConfig": {
        "historicDuration": "1d"
        },
      "forecastConfig": {
        "historicDuration": "1d",
        "forecastDuration": "3h"
        },
      "evalConfig": {
          "rollingWindow": {
              "evalStart": "5m",
              "evalEnd": "now",
              "evalFrequency": 1
          }
      },
      "notificationConfig": {
          "interval": 1
      },
      "targets": [
          "%s"
      ],
      "tags": ["quest-test"]
    }`, stream, targetId)
}

func getMetadataFromAlertResponse(body io.Reader) (string, string, string, []string) {
	type AlertConfig struct {
		Severity  string   `json:"severity"`
		Title     string   `json:"title"`
		Id        string   `json:"id"`
		State     string   `json:"state"`
		AlertType string   `json:"alertType"`
		Tags      []string `json:"tags"`
		Created   string   `json:"created"`
		Datasets  []string `json:"datasets"`
	}

	var response []AlertConfig
	if err := json.NewDecoder(body).Decode(&response); err != nil {
		fmt.Printf("Error decoding: %v\n", err)
	}

	alert := response[0]
	return alert.Id, alert.State, alert.Created, alert.Datasets
}

func createAlertResponse(id string, state string, created string, datasets []string) string {
	datasetsJSON, _ := json.Marshal(datasets)
	return fmt.Sprintf(`
  [
    {
        "title": "AlertTitle",
        "created": "%s",
        "alertType": "threshold",
        "id": "%s",
        "severity": "Medium",
        "state": "%s",
        "tags": [
            "quest-test"
        ],
        "datasets": %s,
        "notificationState": "notify"
    }
]`, created, id, state, string(datasetsJSON))
}

// --- New payload models for expanded test coverage ---

// Dashboard payloads
func getDashboardCreateBody() string {
	return `{
		"title": "Quest Test Dashboard",
		"description": "Dashboard created by quest integration test",
		"tags": ["quest-test", "smoke"],
		"tiles": []
	}`
}

func getDashboardUpdateBody() string {
	return `{
		"title": "Quest Test Dashboard Updated",
		"description": "Dashboard updated by quest integration test",
		"tags": ["quest-test", "smoke", "updated"]
	}`
}

func getDashboardAddTileBody(stream string) string {
	return fmt.Sprintf(`{
		"name": "Log Count Tile",
		"description": "Shows total log count",
		"query": "SELECT COUNT(*) as count FROM %s",
		"visualization": "table",
		"order": 1
	}`, stream)
}

type DashboardResponse struct {
	Id          string   `json:"id"`
	Title       string   `json:"title"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
}

func getIdFromDashboardResponse(body io.Reader) string {
	var response DashboardResponse
	if err := json.NewDecoder(body).Decode(&response); err != nil {
		fmt.Printf("Error decoding dashboard: %v\n", err)
	}
	return response.Id
}

// Filter payloads
func getFilterCreateBody(stream string) string {
	return fmt.Sprintf(`{
		"stream_name": "%s",
		"filter_name": "Quest Test Filter",
		"query": {
			"filter_type": "sql",
			"filter_query": "SELECT * FROM %s WHERE level = 'error'"
		},
		"tags": ["quest-test"]
	}`, stream, stream)
}

func getFilterUpdateBody(stream string) string {
	return fmt.Sprintf(`{
		"stream_name": "%s",
		"filter_name": "Quest Test Filter Updated",
		"query": {
			"filter_type": "sql",
			"filter_query": "SELECT * FROM %s WHERE level = 'warn'"
		},
		"tags": ["quest-test", "updated"]
	}`, stream, stream)
}

type FilterResponse struct {
	FilterId   string `json:"filter_id"`
	FilterName string `json:"filter_name"`
}

func getIdFromFilterResponse(body io.Reader) string {
	var response FilterResponse
	if err := json.NewDecoder(body).Decode(&response); err != nil {
		fmt.Printf("Error decoding filter: %v\n", err)
	}
	return response.FilterId
}

// OTel log payload
func getOTelLogPayload() string {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return fmt.Sprintf(`{
		"resourceLogs": [
			{
				"resource": {
					"attributes": [
						{"key": "service.name", "value": {"stringValue": "quest-test-service"}},
						{"key": "host.name", "value": {"stringValue": "quest-test-host"}}
					]
				},
				"scopeLogs": [
					{
						"scope": {"name": "quest-test"},
						"logRecords": [
							{
								"timeUnixNano": "%s",
								"severityNumber": 9,
								"severityText": "INFO",
								"body": {"stringValue": "Quest OTel test log message"},
								"attributes": [
									{"key": "log.type", "value": {"stringValue": "quest-otel-test"}}
								],
								"traceId": "abcdef1234567890abcdef1234567890",
								"spanId": "abcdef1234567890"
							}
						]
					}
				]
			}
		]
	}`, now)
}

// OTel trace payload
func getOTelTracePayload() string {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return fmt.Sprintf(`{
		"resourceSpans": [
			{
				"resource": {
					"attributes": [
						{"key": "service.name", "value": {"stringValue": "quest-test-service"}},
						{"key": "host.name", "value": {"stringValue": "quest-test-host"}}
					]
				},
				"scopeSpans": [
					{
						"scope": {"name": "quest-test"},
						"spans": [
							{
								"traceId": "abcdef1234567890abcdef1234567890",
								"spanId": "abcdef1234567890",
								"name": "quest-test-span",
								"kind": 1,
								"startTimeUnixNano": "%s",
								"endTimeUnixNano": "%s",
								"status": {"code": 1},
								"attributes": [
									{"key": "http.method", "value": {"stringValue": "GET"}},
									{"key": "http.status_code", "value": {"intValue": "200"}}
								]
							}
						]
					}
				]
			}
		]
	}`, now, now)
}

// OTel metric payload
func getOTelMetricPayload() string {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return fmt.Sprintf(`{
		"resourceMetrics": [
			{
				"resource": {
					"attributes": [
						{"key": "service.name", "value": {"stringValue": "quest-test-service"}},
						{"key": "host.name", "value": {"stringValue": "quest-test-host"}}
					]
				},
				"scopeMetrics": [
					{
						"scope": {"name": "quest-test"},
						"metrics": [
							{
								"name": "quest.test.counter",
								"unit": "1",
								"sum": {
									"dataPoints": [
										{
											"startTimeUnixNano": "%s",
											"timeUnixNano": "%s",
											"asInt": "42",
											"attributes": [
												{"key": "env", "value": {"stringValue": "test"}}
											]
										}
									],
									"aggregationTemporality": 2,
									"isMonotonic": true
								}
							}
						]
					}
				]
			}
		]
	}`, now, now)
}

// Alert modification payloads
func getAlertModifyBody(stream string, targetId string) string {
	return fmt.Sprintf(`{
		"severity": "high",
		"title": "AlertTitle Modified",
		"query": "select count(level) from %s where level = 'error'",
		"alertType": "threshold",
		"thresholdConfig": {
			"operator": ">=",
			"value": 50
		},
		"evalConfig": {
			"rollingWindow": {
				"evalStart": "10m",
				"evalEnd": "now",
				"evalFrequency": 2
			}
		},
		"notificationConfig": {
			"interval": 5
		},
		"targets": ["%s"],
		"tags": ["quest-test", "modified"]
	}`, stream, targetId)
}

// Hot tier payloads
func getHotTierBody() string {
	return `{
		"size": "10GiB"
	}`
}

// Dataset stats payload
func getDatasetStatsBody(streams []string) string {
	streamsJSON, _ := json.Marshal(streams)
	return fmt.Sprintf(`{
		"streams": %s
	}`, string(streamsJSON))
}

// Prism datasets query payload
func getPrismDatasetsBody(stream string) string {
	now := time.Now().UTC()
	startTime := now.Add(-30 * time.Minute).Format(time.RFC3339Nano)
	endTime := now.Add(time.Second).Format(time.RFC3339Nano)
	return fmt.Sprintf(`{
		"query": "SELECT * FROM %s LIMIT 10",
		"startTime": "%s",
		"endTime": "%s"
	}`, stream, startTime, endTime)
}

// Target update payload
func getTargetUpdateBody() string {
	return `{
		"name": "targetNameUpdated",
		"type": "webhook",
		"endpoint": "https://webhook.site/ec627445-d52b-44e9-948d-56671df3581e",
		"headers": {"X-Custom": "quest-test"},
		"skipTlsCheck": true
	}`
}

// RBAC add/remove role payloads
func getRoleAddBody(roleName string) string {
	return fmt.Sprintf(`["%s"]`, roleName)
}

func getMultiPrivilegeRoleBody(writerStream, readerStream string) string {
	return fmt.Sprintf(`[{"privilege": "writer", "resource": {"stream": "%s"}}, {"privilege": "reader", "resource": {"stream": "%s"}}]`, writerStream, readerStream)
}

// NewSampleJsonWithFields merges base SampleJson fields with extra fields.
func NewSampleJsonWithFields(extraFields map[string]interface{}) string {
	var base map[string]interface{}
	_ = json.Unmarshal([]byte(SampleJson), &base)
	for k, v := range extraFields {
		base[k] = v
	}
	out, _ := json.Marshal(base)
	return string(out)
}

// NewSampleJsonBatch generates a JSON array of count events, each with a unique ID.
func NewSampleJsonBatch(count int) string {
	var base map[string]interface{}
	_ = json.Unmarshal([]byte(SampleJson), &base)

	events := make([]map[string]interface{}, count)
	for i := 0; i < count; i++ {
		evt := make(map[string]interface{}, len(base)+1)
		for k, v := range base {
			evt[k] = v
		}
		evt["batch_id"] = i
		evt["p_timestamp"] = time.Now().UTC().Format(time.RFC3339Nano)
		events[i] = evt
	}
	out, _ := json.Marshal(events)
	return string(out)
}

func getDynamicSchemaEvent() string {
	return `{
		"source_time": "2024-10-27T05:13:26.742Z",
		"level": "info",
		"message": "Event with extra field",
		"version": "1.2.0",
		"user_id": 42,
		"device_id": 100,
		"session_id": "sess123",
		"os": "Linux",
		"host": "192.168.1.1",
		"uuid": "test-uuid-001",
		"location": "us-east-1",
		"timezone": "UTC",
		"user_agent": "TestAgent",
		"runtime": "go",
		"request_body": "test body",
		"status_code": 200,
		"response_time": 50,
		"process_id": 999,
		"app_meta": "test-meta",
		"extra_field": "this is a new field not in original schema"
	}`
}
