package main

import (
	"bytes"
	"flag"
	"fmt"
	"net/url"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type Glob struct {
	Url      url.URL
	Username string
	Password string
	Stream   string
	Client   HTTPClient
}

var glob = func() Glob {
	testing.Init()
	var targetUrl string
	var username string
	var password string
	var stream string

	flag.StringVar(&targetUrl, "url", "http://localhost:8000", "Specify url. Default is root")
	flag.StringVar(&username, "user", "admin", "Specify username. Default is admin")
	flag.StringVar(&password, "pass", "admin", "Specify pass. Default is admin")
	flag.StringVar(&stream, "stream", "app", "Specify stream. Default is app")

	flag.Parse()

	parsedTargetUrl, err := url.Parse(targetUrl)
	if err != nil {
		panic("Could not parse url")
	}

	client := DefaultClient(*parsedTargetUrl, username, password)

	return Glob{
		Url:      *parsedTargetUrl,
		Username: username,
		Password: password,
		Stream:   stream,
		Client:   client,
	}
}()

func TestStreamDoesNotExist(t *testing.T) {
	req, _ := glob.Client.NewRequest("POST", "logstream/"+glob.Stream, nil)
	response, err := glob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 404, response.StatusCode, "Server returned http code: %s and response: %s\n", response.Status, readAsString(response.Body))
}

func TestEmptyListLogStream(t *testing.T) {
	req, _ := glob.Client.NewRequest("GET", "logstream", nil)
	response, err := glob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status)
	res, err := readJsonBody[[]string](bytes.NewBufferString(body))
	require.NoErrorf(t, err, "Unmarshal failed: %s", err)
	require.Empty(t, res, "List is not empty")
}

func TestCreateStream(t *testing.T) {
	req, _ := glob.Client.NewRequest("PUT", "logstream/"+glob.Stream, nil)
	response, err := glob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s\n", response.Status, readAsString(response.Body))
}

func TestFlogStream(t *testing.T) {
	cmd := exec.Command("flog", "-f", "json", "-n", "50")
	var out strings.Builder
	cmd.Stdout = &out
	err := cmd.Run()
	require.NoErrorf(t, err, "Failed to run flog: %s", err)

	streamName := glob.Stream + "flogjson"
	for _, obj := range strings.SplitN(out.String(), "\n", 50) {
		var payload strings.Builder
		payload.WriteRune('[')
		payload.WriteString(obj)
		payload.WriteRune(']')

		req, _ := glob.Client.NewRequest("POST", "ingest", bytes.NewBufferString(payload.String()))
		req.Header.Add("X-P-Stream", streamName)
		response, err := glob.Client.Do(req)
		require.NoErrorf(t, err, "Request failed: %s", err)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s resp %s", response.Status, readAsString(response.Body))
	}

	QueryLogStreamCount(t, glob.Client, streamName, 50)
	AssertStreamSchema(t, glob.Client, streamName, FlogJsonSchema)
}

func TestK6Stream(t *testing.T) {
	cmd := exec.Command("k6",
		"run",
		"-e", fmt.Sprintf("P_URL=%s", glob.Url.String()),
		"-e", fmt.Sprintf("P_USERNAME=%s", glob.Username),
		"-e", fmt.Sprintf("P_PASSWORD=%s", glob.Password),
		"-e", fmt.Sprintf("P_STREAM=%s", glob.Stream),
		"../testcases/smoke.js")

	cmd.Run()
	QueryLogStreamCount(t, glob.Client, glob.Stream, 60000)
	AssertStreamSchema(t, glob.Client, glob.Stream, SchemaBody)
}

func TestSetAlert(t *testing.T) {
	req, _ := glob.Client.NewRequest("PUT", "logstream/"+glob.Stream+"/alert", strings.NewReader(AlertBody))
	response, err := glob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func TestGetAlert(t *testing.T) {
	req, _ := glob.Client.NewRequest("GET", "logstream/"+glob.Stream+"/alert", nil)
	response, err := glob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	require.JSONEq(t, AlertBody, body, "Get alert response doesn't match with Alert config returned")
}

func TestSetRetention(t *testing.T) {
	req, _ := glob.Client.NewRequest("PUT", "logstream/"+glob.Stream+"/retention", strings.NewReader(RetentionBody))
	response, err := glob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func TestGetRetention(t *testing.T) {
	req, _ := glob.Client.NewRequest("GET", "logstream/"+glob.Stream+"/retention", nil)
	response, err := glob.Client.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	require.JSONEq(t, RetentionBody, body, "Get retention response doesn't match with retention config returned")
}

func TestRbacBasic(t *testing.T) {
	SetRole(t, glob.Client, "dummy", dummyRole)
	AssertRole(t, glob.Client, "dummy", dummyRole)
	CreateUserWithRole(t, glob.Client, "dummy", []string{"dummy"})
	userClient := glob.Client
	userClient.Username = "dummy"
	userClient.Password = RegenPassword(t, glob.Client, "dummy")
	checkAPIAccess(t, userClient, glob.Stream)
	DeleteUser(t, glob.Client, "dummy")
	DeleteRole(t, glob.Client, "dummy")
}

func TestRoles(t *testing.T) {
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
			body:     RoleReader(glob.Stream),
		},
		{
			roleName: "writer",
			body:     RoleWriter(glob.Stream),
		},
	}

	for _, tc := range cases {
		t.Run(tc.roleName, func(t *testing.T) {
			SetRole(t, glob.Client, tc.roleName, tc.body)
			AssertRole(t, glob.Client, tc.roleName, tc.body)
			username := tc.roleName + "_user"
			password := CreateUserWithRole(t, glob.Client, username, []string{tc.roleName})

			userClient := glob.Client
			userClient.Username = username
			userClient.Password = password
			checkAPIAccess(t, userClient, glob.Stream)
			DeleteUser(t, glob.Client, username)
			DeleteRole(t, glob.Client, tc.roleName)
		})
	}
}
