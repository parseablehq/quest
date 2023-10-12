package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

const sampleModuleRegistrationJSON = `{
  "version": "v0.0.2",
  "username": "admin",
  "password": "admin",
  "url": "http://0.0.0.0:5000",
  "streamConfig": {
    "path": "/api/v1/logstream/{stream_name}/config"
  },
  "routes": [
    {
      "serverPath": "anomaly",
      "modulePath": "/api/v1/anomaly",
      "method": "GET"
    },
    {
      "serverPath": "models",
      "modulePath": "/api/v1/models",
      "method": "GET"
    }
  ]
}`

func test_module_registration_flow(t *testing.T) error {

	start_server()
	module_name := "random_module"
	stream_name := "demo"

	println("Module Registration flow for: " + module_name)

	println("Testing Registering Module")
	req, _ := NewGlob.Client.NewRequest("PUT", "modules/"+module_name, bytes.NewBufferString(sampleModuleRegistrationJSON))
	response, err := NewGlob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))

	println("Getting list of modules:")
	req, _ = NewGlob.Client.NewRequest("GET", "modules", bytes.NewBufferString("{}"))
	response, err = NewGlob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))

	println("Updating config")
	req, _ = NewGlob.Client.NewRequest("PUT", "modules/"+module_name+"/config/"+stream_name, bytes.NewBufferString("[]"))
	response, err = NewGlob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))

	stop_server()

	println("Testing DeRegistering Module")
	req, _ = NewGlob.Client.NewRequest("DELETE", "modules/"+module_name, bytes.NewBufferString("{}"))
	response, err = NewGlob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))

	return nil
}
