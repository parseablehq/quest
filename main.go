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
	Url      url.URL
	Username string
	Password string
	Stream   string
	Client   HTTPClient
	Mode     string
	MinIoConfig
}

type MinIoConfig struct {
	Url    string
	User   string
	Pass   string
	Bucket string
}

var NewGlob = func() Glob {
	testing.Init()
	var targetUrl string
	var username string
	var password string
	var stream string
	var mode string
	// XXX
	var minioUrl string
	var minioUser string
	var minioPass string
	var minioBucket string

	flag.StringVar(&targetUrl, "url", "http://localhost:8000", "Specify url. Default is root")
	flag.StringVar(&username, "user", "admin", "Specify username. Default is admin")
	flag.StringVar(&password, "pass", "admin", "Specify pass. Default is admin")
	flag.StringVar(&stream, "stream", "app", "Specify stream. Default is app")
	flag.StringVar(&mode, "mode", "smoke", "Specify mode. Default is smoke")

	flag.StringVar(&minioUrl, "minio-url", "localhost:9000", "Specify MinIO URL. Default is localhost:9000")
	flag.StringVar(&minioUser, "minio-user", "minioadmin", "Specify MinIO User. Default is `minioadmin`")
	flag.StringVar(&minioPass, "minio-pass", "minioadmin", "Specify MinIO Password. Default is `minioadmin`")
	flag.StringVar(&minioBucket, "minio-bucket", "parseable", "Specify the name of MinIO Bucket. Default is `integrity-test`")

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
		Mode:     mode,
		MinIoConfig: MinIoConfig{
			Url:    minioUrl,
			User:   minioUser,
			Pass:   minioPass,
			Bucket: minioBucket,
		},
	}
}()
