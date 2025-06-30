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
	dummyRole string = `[{"privilege": "editor"},{"privilege": "writer", "resource": {"stream": "app"}}]`
)

const RoleEditor string = `[{"privilege": "editor"}]`

func RoleWriter(stream string) string {
	return fmt.Sprintf(`[{"privilege": "writer", "resource": {"stream": "%s"}}]`, stream)
}

func RoleReader(stream string) string {
	return fmt.Sprintf(`[{"privilege": "reader", "resource": {"stream": "%s", "tag": null}}]`, stream)
}

func Roleingestor(stream string) string {
	return fmt.Sprintf(`[{"privilege": "ingestor", "resource": {"stream": "%s"}}]`, stream)
}

func getTargetBody() string {
	return `          {
              "type": "webhook",
              "endpoint": "https://webhook.site/ec627445-d52b-44e9-948d-56671df3581e",
              "headers": {},
              "skipTlsCheck": true,
              "repeat": {
                  "interval": "1m",
                  "times": 1
              }
          }
`
}

func getIdFromTargetResponse(body io.Reader) string {
	type TargetConf struct {
		Type         string `json:"type"`
		Endpoint     string `json:"endpoint"`
		Headers      string `json:"headers"`
		SkipTlsCheck string `json:"skipTlsCheck"`
		Repeat       string `json:"repeat"`
		Id           string `json:"id"`
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
      "stream": "%s",
      "alertType": "threshold",
      "aggregates": {
          "operator": null,
          "aggregateConfig": [
              {
                  "aggregateFunction": "count",
                  "conditions": {
                      "conditionConfig": [
                          {
                              "column": "status",
                              "operator": ">=",
                              "value": "200"
                          }
                      ]
                  },
                  "column": "status",
                  "operator": "<=",
                  "value": 100
              }
          ]
      },
      "evalConfig": {
          "rollingWindow": {
              "evalStart": "5m",
              "evalEnd": "now",
              "evalFrequency": 1
          }
      },
      "targets": [
          "%s"
      ]
    }`, stream, targetId)
}

func getIdStateFromAlertResponse(body io.Reader) (string, string) {
	type AlertConfig struct {
		Severity   string `json:"severity"`
		Title      string `json:"title"`
		Id         string `json:"id"`
		State      string `json:"state"`
		Stream     string `json:"stream"`
		AlertType  string `json:"alertType"`
		Aggregates string `json:"aggregates"`
		EvalConfig string `json:"evalConfig"`
		Targets    string `json:"targets"`
	}

	var response []AlertConfig
	if err := json.NewDecoder(body).Decode(&response); err != nil {
		fmt.Printf("Error decoding: %v\n", err)
	}

	alert := response[0]
	return alert.Id, alert.State
}

func createAlertResponse(id string, state string, stream string, targetId string) string {
	return fmt.Sprintf(`
  [{
    "version": "v1",
    "id": "%s",
    "state": "%s",
    "severity": "medium",
    "title": "AlertTitle",
    "stream": "%s",
    "alertType": "threshold",
    "aggregates": {
        "operator": null,
        "aggregateConfig": [
            {
                "aggregateFunction": "count",
                "conditions": {
                    "operator": null,
                    "conditionConfig": [                  
                        {
                            "column": "status",
                            "operator": ">=",
                            "value": "200"
                        }
                    ]
                },
                "groupBy": null,
                "column": "status",
                "operator": "<=",
                "value": 100
            }
        ]
    },
    "evalConfig": {
        "rollingWindow": {
            "evalStart": "5m",
            "evalEnd": "now",
            "evalFrequency": 1
        }
    },
    "targets": [
        "%s"
    ]
  }]`, id, state, stream, targetId)
}
