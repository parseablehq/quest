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
	"testing"

	"github.com/stretchr/testify/require"
)

const sample_module_config_per_stream = `
[{
    "mode": "col",
    "algorithm": "z-score",
    "start_time": "2023-10-12T08:38:47.069Z",
    "end_time": "2023-10-12T08:40:47.069Z",
    "interval_window": "5s",
    "column_to_watch": "api_timeToResponseInNS"
}]
`

func test_module_registration_flow(t *testing.T) error {

	module_name := "panorama"
	stream_name := NewGlob.Stream

	sample_proxy_route_body := `
{
  "stream_name": "` + stream_name + `"
}
`

	println("Module Registration flow for: " + module_name)

	println("Getting list of modules:")
	req, _ := NewGlob.Client.NewRequest("GET", "modules", bytes.NewBufferString("{}"))
	response, err := NewGlob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))

	println("Updating config")
	req, _ = NewGlob.Client.NewRequest("PUT", "modules/"+module_name+"/config/"+stream_name, bytes.NewBufferString(sample_module_config_per_stream))
	response, err = NewGlob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))

	println("Receiving config")
	req, _ = NewGlob.Client.NewRequest("GET", "modules/"+module_name+"/config/"+stream_name, bytes.NewBufferString("{}"))
	response, err = NewGlob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	// require.Equalf(t, bytes.NewBufferString((sample_module_config_per_stream)), response.Body, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))

	println("Testing Proxy Route")
	req, _ = NewGlob.Client.NewRequest("GET", "modules/"+module_name+"/anomaly", bytes.NewBufferString(sample_proxy_route_body))
	response, err = NewGlob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))

	println("Stop anomaly watching by putting config as empty")
	req, _ = NewGlob.Client.NewRequest("PUT", "modules/"+module_name+"/config/"+stream_name, bytes.NewBufferString("{}"))
	response, err = NewGlob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))

	println("Testing DeRegistering Module")
	req, _ = NewGlob.Client.NewRequest("DELETE", "modules/"+module_name, bytes.NewBufferString("{}"))
	response, err = NewGlob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))

	println("Testing Duplicate DeRegister ")
	req, _ = NewGlob.Client.NewRequest("DELETE", "modules/"+module_name, bytes.NewBufferString("{}"))
	response, err = NewGlob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 400, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))

	return nil
}
