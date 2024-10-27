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

import "fmt"

const AlertBody string = `{
  "version": "v1",
  "alerts": [
    {
      "message": "server side error occurred",
      "name": "Status Alert",
      "rule": {
        "config": "status_code != 500",
        "type": "composite"
      },
      "targets": [
        {
          "type": "webhook",
          "endpoint": "https://webhook.site/6b184e08-82c4-46dc-b344-5b85414c2a71",
          "headers": {},
          "skip_tls_check": true,
          "repeat": {
            "interval": "3m 20s",
            "times": 5
          }
        },
        {
          "type": "slack",
          "endpoint": "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",
          "repeat": {
            "interval": "3m",
            "times": 2
          }
        }
      ]
    }
  ]
}`

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
[
    {
        "device_id": 1855,
        "host": "192.168.1.100",
        "level": "error",
        "message": "Application started",
        "os": "macOS",
        "p_metadata": "containername=log-generator^namespace=go-apasdp^host=10.116.0.3^source=quest-test^podlabels=app=go-app,pod-template-hash=6c87bc9cc9^containerimage=ghcr.io/parseablehq/quest",
        "p_tags": "",
        "p_timestamp": "2024-10-27T05:13:26.744",
        "process_id": 279,
        "response_time": 34,
        "session_id": "xyz",
        "source_time": "2024-10-27T05:13:26.743",
        "status_code": 400,
        "timezone": "qtv",
        "user_id": 46008,
        "uuid": "8d01b3cb-825a-4988-92db-96b5493db772",
        "version": "1.2.0"
    },
    {
        "device_id": 4499,
        "host": "192.168.1.100",
        "level": "info",
        "message": "Application started",
        "os": "Linux",
        "p_metadata": "containername=log-generator^namespace=go-apasdp^host=10.116.0.3^source=quest-test^podlabels=app=go-app,pod-template-hash=6c87bc9cc9^containerimage=ghcr.io/parseablehq/quest",
        "p_tags": "",
        "p_timestamp": "2024-10-27T05:13:26.744",
        "request_body": "npcftnmxcdrydlsvcwotlzbokmqwacnoitaezoddrvmtzeszjdpliukklzoxdkewimglolofpdedyutvsaobebojjokzflcdmlvu",
        "response_time": 34,
        "runtime": "bfa",
        "session_id": "abc",
        "source_time": "2024-10-27T05:13:26.743",
        "timezone": "hzr",
        "user_id": 76351,
        "uuid": "cf33d641-ffab-4523-989c-58dd02a011b7",
        "version": "1.0.0"
    },
    {
        "device_id": 2733,
        "host": "192.168.1.100",
        "level": "info",
        "message": "Application started",
        "os": "Linux",
        "p_metadata": "containername=log-generator^namespace=go-apasdp^host=10.116.0.3^source=quest-test^podlabels=app=go-app,pod-template-hash=6c87bc9cc9^containerimage=ghcr.io/parseablehq/quest",
        "p_tags": "",
        "p_timestamp": "2024-10-27T05:13:26.744",
        "process_id": 975,
        "session_id": "pqr",
        "source_time": "2024-10-27T05:13:26.743",
        "status_code": 500,
        "timezone": "cut",
        "user_agent": "PineApple",
        "user_id": 69023,
        "uuid": "2daf065d-ecff-4a3e-a5e1-7b4a9e404bc1",
        "version": "1.1.0"
    },
    {
        "device_id": 150,
        "host": "112.168.1.110",
        "level": "error",
        "location": "wukxvqjlqdxjjpvy",
        "message": "Application is failing",
        "os": "Windows",
        "p_metadata": "containername=log-generator^namespace=go-apasdp^host=10.116.0.3^source=quest-test^podlabels=app=go-app,pod-template-hash=6c87bc9cc9^containerimage=ghcr.io/parseablehq/quest",
        "p_tags": "",
        "p_timestamp": "2024-10-27T05:13:26.744",
        "request_body": "jrdjsnssrmemqdphdabrmukpsguddyifqthxockkaqxccainkyywqohuefynnxlofpgvdsoijnqavipzbkcoxegfurbxehsbnftc",
        "response_time": 22,
        "session_id": "pqr",
        "source_time": "2024-10-27T05:13:26.742",
        "timezone": "wxu",
        "user_id": 88506,
        "uuid": "050a043a-83f3-4f2a-81be-8d7b68f081e9",
        "version": "1.2.0"
    },
    {
        "device_id": 4489,
        "host": "112.168.1.110",
        "level": "warn",
        "location": "vnudttwllunegitb",
        "message": "Logging a request",
        "os": "Windows",
        "p_metadata": "containername=log-generator^namespace=go-apasdp^host=10.116.0.3^source=quest-test^podlabels=app=go-app,pod-template-hash=6c87bc9cc9^containerimage=ghcr.io/parseablehq/quest",
        "p_tags": "",
        "p_timestamp": "2024-10-27T05:13:26.744",
        "process_id": 258,
        "runtime": "nsz",
        "session_id": "xyz",
        "source_time": "2024-10-27T05:13:26.742",
        "user_agent": "PearOS",
        "user_id": 87865,
        "uuid": "9f33b8e3-b875-4736-8c86-355a5dfc95a0",
        "version": "1.1.0"
    },
    {
        "device_id": 780,
        "host": "112.168.1.110",
        "level": "info",
        "message": "Application started",
        "os": "Linux",
        "p_metadata": "containername=log-generator^namespace=go-apasdp^host=10.116.0.3^source=quest-test^podlabels=app=go-app,pod-template-hash=6c87bc9cc9^containerimage=ghcr.io/parseablehq/quest",
        "p_tags": "",
        "p_timestamp": "2024-10-27T05:13:26.744",
        "process_id": 365,
        "response_time": 34,
        "runtime": "qld",
        "session_id": "xyz",
        "source_time": "2024-10-27T05:13:26.742",
        "status_code": 200,
        "user_id": 71218,
        "uuid": "54438529-5ad1-480f-b44b-f871e96afd39",
        "version": "1.0.0"
    },
    {
        "device_id": 2772,
        "host": "112.168.1.110",
        "level": "warn",
        "message": "Logging a request",
        "os": "macOS",
        "p_metadata": "containername=log-generator^namespace=go-apasdp^host=10.116.0.3^source=quest-test^podlabels=app=go-app,pod-template-hash=6c87bc9cc9^containerimage=ghcr.io/parseablehq/quest",
        "p_tags": "",
        "p_timestamp": "2024-10-27T05:13:26.744",
        "process_id": 885,
        "request_body": "wuoaqbuldmizuoxtjcohvytpjgcfkgrvqjmipzlhespctmmmmxszpjeadpurgeiifgcryqzqjxngohdyssxfkqzmyftqtlwikkrp",
        "session_id": "pqr",
        "source_time": "2024-10-27T05:13:26.742",
        "timezone": "nhw",
        "user_agent": "OrangeOS",
        "user_id": 83057,
        "uuid": "a93337df-0ca4-4d46-9b94-7a4ea3490a27",
        "version": "1.0.0"
    },
    {
        "device_id": 2925,
        "host": "112.168.1.110",
        "level": "warn",
        "message": "Logging a request",
        "os": "Linux",
        "p_metadata": "containername=log-generator^namespace=go-apasdp^host=10.116.0.3^source=quest-test^podlabels=app=go-app,pod-template-hash=6c87bc9cc9^containerimage=ghcr.io/parseablehq/quest",
        "p_tags": "",
        "p_timestamp": "2024-10-27T05:13:26.744",
        "process_id": 563,
        "request_body": "dihfniynpucmgxhcknzxazlqbglzoygxmdpnnaabfgksxnirzwaojampaobiqxbcolgutjyvyuffkasvwqvtmazlzxdstjhszztf",
        "response_time": 70,
        "session_id": "abc",
        "source_time": "2024-10-27T05:13:26.742",
        "timezone": "ftj",
        "user_id": 57348,
        "uuid": "b816bf5d-3d26-4a9e-a7ae-51aac1662e06",
        "version": "1.2.0"
    },
    {
        "app_meta": "qraspufqzwdvgafcmcmxkqmp",
        "device_id": 1627,
        "host": "172.162.1.120",
        "level": "error",
        "message": "Application started",
        "os": "Windows",
        "p_metadata": "containername=log-generator^namespace=go-apasdp^host=10.116.0.3^source=quest-test^podlabels=app=go-app,pod-template-hash=6c87bc9cc9^containerimage=ghcr.io/parseablehq/quest",
        "p_tags": "",
        "p_timestamp": "2024-10-27T05:13:26.744",
        "runtime": "ptp",
        "session_id": "xyz",
        "source_time": "2024-10-27T05:13:26.742",
        "timezone": "tol",
        "user_agent": "OrangeOS",
        "user_id": 94706,
        "uuid": "10048586-da52-41fb-889b-54a377ab6a9f",
        "version": "1.2.0"
    },
    {
        "app_meta": "bkfmqbmmjzbhkxdjzzlaebqp",
        "device_id": 42,
        "host": "112.168.1.110",
        "level": "warn",
        "location": "ffxkmbwbtxplhgnz",
        "message": "Logging a request",
        "os": "Linux",
        "p_metadata": "containername=log-generator^namespace=go-apasdp^host=10.116.0.3^source=quest-test^podlabels=app=go-app,pod-template-hash=6c87bc9cc9^containerimage=ghcr.io/parseablehq/quest",
        "p_tags": "",
        "p_timestamp": "2024-10-27T05:13:26.744",
        "request_body": "ffywhsbtsgvraxjuixlsxtrgotcahkicyxnaermtqmfgzlwbqkxqmonrwojmawsyxsovcjlbkbvjsesfznpukicdtghnvvirtauo",
        "session_id": "pqr",
        "source_time": "2024-10-27T05:13:26.742",
        "status_code": 300,
        "user_id": 72278,
        "uuid": "d679e104-778d-4bbe-b9b6-e6f2b48922ad",
        "version": "1.1.0"
    }
]
`

const FlogJsonSchema string = `{
    "fields": [
        {
            "name": "bytes",
            "data_type": "Int64",
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
            "name": "p_metadata",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "p_tags",
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
            "data_type": "Int64",
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
            "name": "device_id",
            "data_type": "Int64",
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
            "name": "message",
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
            "name": "p_metadata",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "p_tags",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "p_timestamp",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "process_id",
            "data_type": "Int64",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "response_time",
            "data_type": "Int64",
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
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "status_code",
            "data_type": "Int64",
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
            "name": "user_id",
            "data_type": "Int64",
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
            "name": "runtime",
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
            "name": "location",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
        },
        {
            "name": "app_meta",
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
