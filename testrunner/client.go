package main

import (
	"io"
	"net/http"
	"net/url"
	"time"
)

type HTTPClient struct {
	client   http.Client
	Url      url.URL
	Username string
	Password string
}

func DefaultClient(url url.URL, username string, password string) HTTPClient {
	return HTTPClient{
		client:   http.Client{Timeout: 60 * time.Second},
		Url:      url,
		Username: username,
		Password: password,
	}
}

func (client *HTTPClient) baseAPIURL(path string) (x string) {
	x, _ = url.JoinPath(client.Url.String(), "api/v1/", path)
	return
}

func (client *HTTPClient) NewRequest(method string, path string, body io.Reader) (req *http.Request, err error) {
	req, err = http.NewRequest(method, client.baseAPIURL(path), body)
	if err != nil {
		return
	}
	req.SetBasicAuth(client.Username, client.Password)
	req.Header.Add("Content-Type", "application/json")
	return
}

func (client *HTTPClient) Do(req *http.Request) (*http.Response, error) {
	return client.client.Do(req)
}
