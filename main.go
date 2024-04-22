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
	QueryUrl         url.URL
	QueryUsername    string
	QueryPassword    string
	IngestorUrl      url.URL
	IngestorUsername string
	IngestorPassword string
	Stream           string
	QueryClient      HTTPClient
	IngestorClient   HTTPClient
	Mode             string
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
	var targetQueryUrl string
	var queryUsername string
	var queryPassword string

	var targetIngestorUrl string
	var ingestorUsername string
	var ingestorPassword string

	var stream string
	var mode string
	// XXX
	var minioUrl string
	var minioUser string
	var minioPass string
	var minioBucket string

	flag.StringVar(&targetQueryUrl, "query-url", "http://localhost:8000", "Specify url. Default is root")
	flag.StringVar(&queryUsername, "query-user", "admin", "Specify username. Default is admin")
	flag.StringVar(&queryPassword, "query-pass", "admin", "Specify pass. Default is admin")

	flag.StringVar(&targetIngestorUrl, "ingestor-url", "", "Specify url. Default is root")
	flag.StringVar(&ingestorUsername, "ingestor-user", "admin", "Specify username. Default is admin")
	flag.StringVar(&ingestorPassword, "ingestor-pass", "admin", "Specify pass. Default is admin")

	flag.StringVar(&stream, "stream", "app", "Specify stream. Default is app")
	flag.StringVar(&mode, "mode", "smoke", "Specify mode. Default is smoke")

	flag.StringVar(&minioUrl, "minio-url", "localhost:9000", "Specify MinIO URL. Default is localhost:9000")
	flag.StringVar(&minioUser, "minio-user", "minioadmin", "Specify MinIO User. Default is `minioadmin`")
	flag.StringVar(&minioPass, "minio-pass", "minioadmin", "Specify MinIO Password. Default is `minioadmin`")
	flag.StringVar(&minioBucket, "minio-bucket", "parseable", "Specify the name of MinIO Bucket. Default is `integrity-test`")

	flag.Parse()

	parsedQueryTargetUrl, err := url.Parse(targetQueryUrl)
	if err != nil {
		panic("Could not parse url")
	}

	queryClient := DefaultClient(*parsedQueryTargetUrl, queryUsername, queryPassword)

	if targetIngestorUrl != "" {
		parsedIngestorTargetUrl, err := url.Parse(targetIngestorUrl)
		if err != nil {
			panic("Could not parse url")
		}

		ingestorClient := DefaultClient(*parsedIngestorTargetUrl, ingestorUsername, ingestorPassword)
		return Glob{
			QueryUrl:         *parsedQueryTargetUrl,
			QueryUsername:    queryUsername,
			QueryPassword:    queryPassword,
			QueryClient:      queryClient,
			IngestorUrl:      *parsedIngestorTargetUrl,
			IngestorUsername: ingestorUsername,
			IngestorPassword: ingestorPassword,
			IngestorClient:   ingestorClient,
			Stream:           stream,
			Mode:             mode,
			MinIoConfig: MinIoConfig{
				Url:    minioUrl,
				User:   minioUser,
				Pass:   minioPass,
				Bucket: minioBucket,
			},
		}
	} else {
		return Glob{
			QueryUrl:      *parsedQueryTargetUrl,
			QueryUsername: queryUsername,
			QueryPassword: queryPassword,
			QueryClient:   queryClient,
			Stream:        stream,
			Mode:          mode,
			MinIoConfig: MinIoConfig{
				Url:    minioUrl,
				User:   minioUser,
				Pass:   minioPass,
				Bucket: minioBucket,
			},
		}
	}

}()
