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
	"fmt"
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
	fmt.Println("Url = ", req.URL)
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
