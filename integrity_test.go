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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/minio/minio-go"
	"github.com/stretchr/testify/require"
)

type Flog struct {
	Host      string  `json:"host"`
	UserId    string  `json:"user-identifier"`
	Timestamp string  `json:"datetime"`
	Method    string  `json:"method"`
	Request   string  `json:"request"`
	Protocol  string  `json:"protocol"`
	Status    float64 `json:"status"`
	ByteCount float64 `json:"bytes"`
	Referer   string  `json:"referer"`
}

// - Send logs to Parseable
// - Wait for sync
// - Download parquet files from the store created by Parseable for the minute
// - Compare the sent logs with the ones loaded from the downloaded parquet
func TestIntegrity(t *testing.T) {
	t.Parallel()
	stream := uniqueStream(t)
	CreateStream(t, NewGlob.QueryClient, stream)
	iterations := 1
	flogsPerIteration := 100

	parseableSyncWait := 2 * time.Minute // NOTE: This needs to be in sync with Parseable's.

	// - Generate log files using `flog`
	// - Load them into `Flog` structs
	// - Ingest them into Parseable

	flogs := make([]Flog, 0, iterations*flogsPerIteration)

	tmpDir := t.TempDir()
	for i := range iterations {
		flogsFile := filepath.Join(tmpDir, fmt.Sprintf("%s_%d_%s.log", stream, i, randSuffix()))

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

		err = ingestFlogs(loadedFlogs, stream)
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

	parquetFiles := downloadParquetFiles(stream, NewGlob.MinIoConfig)
	actualFlogs := loadFlogsFromParquetFiles(parquetFiles)

	require.Equal(t, len(flogs), len(actualFlogs), "row count mismatch")

	// Row order from parquet not deterministic under concurrent writes.
	// Sort both slices on a stable composite key before compare.
	sortKey := func(f Flog) string {
		return fmt.Sprintf("%s|%s|%s|%s|%v|%v|%s",
			f.Timestamp, f.Host, f.UserId, f.Request, f.Status, f.ByteCount, f.Referer)
	}
	sort.Slice(flogs, func(i, j int) bool { return sortKey(flogs[i]) < sortKey(flogs[j]) })
	sort.Slice(actualFlogs, func(i, j int) bool { return sortKey(actualFlogs[i]) < sortKey(actualFlogs[j]) })

	for i, expectedFlog := range flogs {
		actualFlog := actualFlogs[i]
		require.Equal(t, expectedFlog, actualFlog)
	}

	DeleteStream(t, NewGlob.QueryClient, stream)
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

	slog.Info("downloaded files", "paths", downloadedFileNames)

	return downloadedFileNames
}

func loadFlogsFromParquetFile(path string) []Flog {
	slog.Info("reading parquet file", "path", path)

	rdr, err := file.OpenParquetFile(path, false)
	if err != nil {
		slog.Error("can't open parquet file", "error", err)
		return nil
	}
	defer rdr.Close()

	arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		slog.Error("can't create arrow reader", "error", err)
		return nil
	}

	tbl, err := arrowRdr.ReadTable(context.Background())
	if err != nil {
		slog.Error("can't read table", "error", err)
		return nil
	}
	defer tbl.Release()

	numRows := int(tbl.NumRows())
	slog.Info("read parquet", "rows", numRows, "path", path)

	// Build column index for lookup by name
	colIndex := make(map[string]int)
	for i := 0; i < int(tbl.NumCols()); i++ {
		colIndex[tbl.Column(i).Name()] = i
	}

	getStringCol := func(name string) *array.String {
		idx, ok := colIndex[name]
		if !ok {
			return nil
		}
		return tbl.Column(idx).Data().Chunk(0).(*array.String)
	}

	getFloat64Col := func(name string) *array.Float64 {
		idx, ok := colIndex[name]
		if !ok {
			return nil
		}
		return tbl.Column(idx).Data().Chunk(0).(*array.Float64)
	}

	hostCol := getStringCol("host")
	userIdCol := getStringCol("user-identifier")
	datetimeCol := getStringCol("datetime")
	methodCol := getStringCol("method")
	requestCol := getStringCol("request")
	protocolCol := getStringCol("protocol")
	statusCol := getFloat64Col("status")
	bytesCol := getFloat64Col("bytes")
	refererCol := getStringCol("referer")

	flogs := make([]Flog, numRows)
	for i := range numRows {
		flogs[i] = Flog{
			Host:      hostCol.Value(i),
			UserId:    userIdCol.Value(i),
			Timestamp: datetimeCol.Value(i),
			Method:    methodCol.Value(i),
			Request:   requestCol.Value(i),
			Protocol:  protocolCol.Value(i),
			Status:    statusCol.Value(i),
			ByteCount: bytesCol.Value(i),
			Referer:   refererCol.Value(i),
		}
	}

	return flogs
}

func loadFlogsFromParquetFiles(parquetFiles []string) []Flog {
	slog.Info("loading flogs from parquet files", "paths", parquetFiles, "count", len(parquetFiles))
	flogs := make([]Flog, 0, len(parquetFiles)*10)

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
