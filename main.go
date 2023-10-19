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
	"flag"
	"net/url"
	"testing"
)

func main() {
	println("hello")
}

type Glob struct {
	Url                   url.URL
	Username              string
	Password              string
	Stream                string
	Client                HTTPClient
	Mode                  string
	PanoramaBaseAddress   string
	PanoramaAdminUsername string
	PanoramaAdminPassword string
}

var NewGlob = func() Glob {
	testing.Init()
	var targetUrl string
	var username string
	var password string
	var stream string
	var mode string

	var panoramaAddress string
	var panoramaAdminUsername string
	var panoramaAdminPassword string

	flag.StringVar(&targetUrl, "url", "http://localhost:8000", "Specify url. Default is root")
	flag.StringVar(&username, "user", "admin", "Specify username. Default is admin")
	flag.StringVar(&password, "pass", "admin", "Specify pass. Default is admin")
	flag.StringVar(&stream, "stream", "app", "Specify stream. Default is app")
	flag.StringVar(&mode, "mode", "smoke", "Specify mode. Default is smoke")
	flag.StringVar(&panoramaAddress, "panorama-address", "http://localhost:5000", "Specify panorama address. Default is http://localhost:5000")

	flag.StringVar(&panoramaAdminUsername, "panorama-admin-username", "pano_admin", "Specify module connection username. Default is pano_admin")
	flag.StringVar(&panoramaAdminPassword, "panorama-admin-password", "pano_admin", "Specify Module connection password. Default is pano_admin")

	flag.Parse()

	parsedTargetUrl, err := url.Parse(targetUrl)
	if err != nil {
		panic("Could not parse url")
	}

	client := DefaultClient(*parsedTargetUrl, username, password)

	return Glob{
		Url:                   *parsedTargetUrl,
		Username:              username,
		Password:              password,
		Stream:                stream,
		Client:                client,
		Mode:                  mode,
		PanoramaBaseAddress:   panoramaAddress,
		PanoramaAdminUsername: panoramaAdminUsername,
		PanoramaAdminPassword: panoramaAdminPassword,
	}
}()
