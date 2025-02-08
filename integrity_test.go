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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go"
	"github.com/stretchr/testify/require"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type Flog struct {
	Host      string `json:"host"`
	UserId    string `json:"user-identifier"`
	Timestamp string `json:"datetime"`
	Method    string `json:"method"`
	Request   string `json:"request"`
	Protocol  string `json:"protocol"`
	Status    uint16 `json:"status"`
	ByteCount uint64 `json:"bytes"`
	Referer   string `json:"referer"`
}

// Same as `Flog`, but all fields are pointers, because `parquet-go` is only
// working when fields are pointers.
type ParquetFlog struct {
	Host      *string `parquet:"name=host, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	UserId    *string `parquet:"name=user-identifier, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Timestamp *string `parquet:"name=datetime, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Method    *string `parquet:"name=method, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Request   *string `parquet:"name=request, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Protocol  *string `parquet:"name=protocol, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Status    *uint16 `parquet:"name=status, type=INT32, encoding=PLAIN"`
	ByteCount *uint64 `parquet:"name=bytes, type=INT32, encoding=PLAIN"`
	Referer   *string `parquet:"name=referer, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

func (flog *ParquetFlog) Deref() Flog {
	return Flog{
		Host:      *flog.Host,
		UserId:    *flog.UserId,
		Timestamp: *flog.Timestamp,
		Method:    *flog.Method,
		Request:   *flog.Request,
		Protocol:  *flog.Protocol,
		Status:    *flog.Status,
		ByteCount: *flog.ByteCount,
		Referer:   *flog.Referer,
	}
}

// - Send logs to Parseable
// - Wait for sync
// - Download parquet files from the store created by Parseable for the minute
// - Compare the sent logs with the ones loaded from the downloaded parquet
func TestIntegrity(t *testing.T) {
	CreateStream(t, NewGlob.QueryClient, NewGlob.Stream)
	iterations := 1
	flogsPerIteration := 100

	parseableSyncWait := 3 * time.Minute // NOTE: This needs to be in sync with Parseable's.

	// - Generate log files using `flog`
	// - Load them into `Flog` structs
	// - Ingest them into Parseable

	flogs := make([]Flog, 0, iterations*flogsPerIteration)

	for i := 0; i < iterations; i++ {
		flogsFile := fmt.Sprintf("%d.log", i)

		err := exec.Command("flog",
			"--number", strconv.Itoa(flogsPerIteration),
			"--format", "json",
			"--type", "log",
			"--overwrite",
			"--output", flogsFile).Run()
		if err != nil {
			slog.Error("couldn't generate flogs", "error", err)
		}

		loadedFlogs := loadFlogsFromFile(flogsFile)

		err = ingestFlogs(loadedFlogs, NewGlob.Stream)
		if err != nil {
			t.Fatal("error ingesting flogs", err)
		}

		flogs = append(flogs, loadedFlogs...)

		slog.Info("ingested logs, sleeping...",
			"iteration", i+1,
			"log_count", len(loadedFlogs))

		// Wait for the events to be sync'd.
		time.Sleep(parseableSyncWait)
		// XXX: We don't need to sleep for the entire minute, just until the next minute boundary.
	}

	parquetFiles := downloadParquetFiles(NewGlob.Stream, NewGlob.MinIoConfig)
	actualFlogs := loadFlogsFromParquetFiles(parquetFiles)

	rowCount := len(actualFlogs)

	for i, expectedFlog := range flogs {
		// The rows in parquet written by Parseable will be latest first, so we
		// compare the first of ours with the last of what we got from Parseable's
		// store.
		actualFlog := actualFlogs[rowCount-i-1].Deref()
		require.Equal(t, actualFlog, expectedFlog)
	}

	DeleteStream(t, NewGlob.QueryClient, NewGlob.Stream)
}

func ingestFlogs(flogs []Flog, stream string) error {
	payload, _ := json.Marshal(flogs)
	if NewGlob.IngestorUrl.String() == "" {
		req, _ := NewGlob.QueryClient.NewRequest(http.MethodPost, "ingest", bytes.NewBuffer(payload))
		req.Header.Add("X-P-Stream", stream)
		response, err := NewGlob.QueryClient.Do(req)
		if err != nil {
			return err
		}

		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("couldn't ingest logs, status code = %d", response.StatusCode)
		}
	} else {
		req, _ := NewGlob.IngestorClient.NewRequest(http.MethodPost, "ingest", bytes.NewBuffer(payload))
		req.Header.Add("X-P-Stream", stream)
		response, err := NewGlob.QueryClient.Do(req)
		if err != nil {
			return err
		}

		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("couldn't ingest logs, status code = %d", response.StatusCode)
		}
	}

	return nil
}

func downloadParquetFiles(stream string, config MinIoConfig) []string {
	client, err := minio.New(config.Url, config.User, config.Pass, false)
	if err != nil {
		slog.Error("couldn't create MinIO client", "error", err)
	}

	downloadedFileNames := make([]string, 0, 10)

	slog.Info("downloading parquet files from MinIO",
		"bucket", config.Bucket,
		"stream", stream)

	for objectInfo := range client.ListObjectsV2(config.Bucket, stream, true, nil) {
		key := objectInfo.Key

		if !isParquetFile(key) {
			slog.Info("skipping path, not a parquet file", "key", key)
			continue
		}

		parquetObject, err := client.GetObject(config.Bucket, key, minio.GetObjectOptions{})
		if err != nil {
			slog.Error("couldn't get object", "key", key, "error", err)
		}

		// Write the MinIO Object we got, into `downloadPath`.

		fileName := strings.ReplaceAll(key, "/", ".")
		f, _ := os.Create(fileName)
		_, err = io.Copy(f, parquetObject)

		if err != nil {
			slog.Error("couldn't copy", "fileName", fileName, "error", err)
		}

		downloadedFileNames = append(downloadedFileNames, fileName)

		f.Close()
	}

	// Reverse the filenames, because we want latest files first (only if there are multiple files)
	if len(downloadedFileNames) > 1 {
		for i, j := 0, len(downloadedFileNames)-1; i < j; i, j = i+1, j-1 {
			downloadedFileNames[i], downloadedFileNames[j] = downloadedFileNames[j], downloadedFileNames[i]
		}
	}

	slog.Info("downloaded files", "paths", downloadedFileNames)

	return downloadedFileNames
}

func loadFlogsFromParquetFile(path string) []ParquetFlog {
	fr, err := local.NewLocalFileReader(path)
	slog.Info("reading parquet file", "path", path)
	if err != nil {
		slog.Error("can't create local file reader", "error", err)
	}

	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(ParquetFlog), 4)
	if err != nil {
		slog.Error("can't create parquet reader", "error", err)
	}

	defer pr.ReadStop()

	flogs := make([]ParquetFlog, pr.GetNumRows())

	if err = pr.Read(&flogs); err != nil {
		slog.Error("can't read parquet file", "error", err)
	}

	return flogs
}

func loadFlogsFromParquetFiles(parquetFiles []string) []ParquetFlog {
	slog.Info("loading flogs from parquet files", "paths", parquetFiles, "count", len(parquetFiles))
	flogs := make([]ParquetFlog, 0, len(parquetFiles)*10)

	for _, parquetFile := range parquetFiles {
		flogs = append(flogs, loadFlogsFromParquetFile(parquetFile)...)
	}

	return flogs
}

func isParquetFile(path string) bool {
	return filepath.Ext(path) == ".parquet"
}

func loadFlogsFromFile(path string) []Flog {
	f, err := os.Open(path)
	if err != nil {
		slog.Error("couldn't open file", "path", path, "error", err)
	}

	lines := bufio.NewScanner(f)
	lines.Split(bufio.ScanLines)

	flogs := make([]Flog, 0, 10)

	for lines.Scan() {
		line := lines.Bytes()
		flog := Flog{}

		err := json.Unmarshal(line, &flog)
		if err != nil {
			slog.Error("couldn't unmarshal line", "line", string(line), "error", err)
		}

		flogs = append(flogs, flog)
	}

	return flogs
}
