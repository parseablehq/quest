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

package httpclient

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

func joinURLPath(baseURL string, reference string) (string, error) {
	parsedReference, err := url.Parse(reference)
	if err != nil {
		return "", err
	}
	joined, err := url.JoinPath(baseURL, parsedReference.Path)
	if err != nil {
		return "", err
	}
	parsedJoined, err := url.Parse(joined)
	if err != nil {
		return "", err
	}
	parsedJoined.RawQuery = parsedReference.RawQuery
	parsedJoined.Fragment = parsedReference.Fragment
	return parsedJoined.String(), nil
}

func (client *HTTPClient) baseAPIURL(path string) (string, error) {
	apiBase, err := url.JoinPath(client.Url.String(), "api/v1/")
	if err != nil {
		return "", err
	}
	return joinURLPath(apiBase, path)
}

func (client *HTTPClient) NewRequest(method string, path string, body io.Reader) (req *http.Request, err error) {
	requestURL, err := client.baseAPIURL(path)
	if err != nil {
		return nil, err
	}
	return client.NewRequestAtPath(method, requestURL, body)
}

func (client *HTTPClient) NewRequestAtPath(method string, path string, body io.Reader) (req *http.Request, err error) {
	requestURL := path
	if parsedURL, parseErr := url.Parse(path); parseErr != nil || !parsedURL.IsAbs() {
		requestURL, err = joinURLPath(client.Url.String(), path)
		if err != nil {
			return nil, err
		}
	}

	req, err = http.NewRequest(method, requestURL, body)
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
