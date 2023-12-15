package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
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
// - Download parquet from the store created by Parseable for the minute
// - Compare the sent logs with the ones loaded from the downloaded parquet
func TestIntegrity(t *testing.T) {
	// Load the logs we want to send to Parseable from the file.

	// NOTE: We can generate logs on the fly by running the `flog` command.
	flogsFile, err := os.Open("data/flogs.txt")
	if err != nil {
		t.Fatalf("couldn't open fake logs file (%s)", err)
	}

	lines := bufio.NewScanner(flogsFile)
	lines.Split(bufio.ScanLines)

	flogs := make([]Flog, 0, 10)

	for lines.Scan() {
		line := lines.Bytes()
		flog := Flog{}

		err := json.Unmarshal(line, &flog)
		if err != nil {
			t.Fatalf("Invalid flog line: %s", err)
		}

		flogs = append(flogs, flog)
	}

	stream := "integrity"

	go ingestFlogs(t, flogs, stream)

	// Wait for the events to be synched.
	time.Sleep(1 * time.Minute) // FIXME

	// Download the parquet file from MinIO.

	// FIXME: Timing can go wrong and we may fail to get the object from MinIO.
	path := getPath(time.Now(), stream)
	minioConfig := NewGlob.MinIoConfig
	parquetFile := "object.parquet"

	path = "integrity/date=2023-12-14/hour=17/minute=36/S1LT-6168.data.parquet"
	downloadParquetFromMinio(t, path, minioConfig, parquetFile)

	// Load logs from the downloaded file and compare with the ones we ingested.

	actualFlogs := loadFlogsFromParquetFile(t, parquetFile)
	rowCount := len(actualFlogs)

	for i, expectedFlog := range flogs {
		// The rows in parquet written by Parseable will be latest first, so we
		// compare the first of ours, with the last of what we got from Parseable's
		// store.
		actualFlog := actualFlogs[rowCount-i-1].Deref()
		require.Equal(t, actualFlog, expectedFlog)
	}
}

func ingestFlogs(t *testing.T, flogs []Flog, stream string) {
	payload, _ := json.Marshal(flogs)

	req, _ := NewGlob.Client.NewRequest(http.MethodPost, "ingest", bytes.NewBuffer(payload))
	req.Header.Add("X-P-Stream", stream)
	response, err := NewGlob.Client.Do(req)

	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t,
		200, response.StatusCode,
		"Server returned http code: %s resp %s",
		response.Status,
		readAsString(response.Body))
}

func getPath(now time.Time, stream string) string {
	year, month, day := now.Date()
	hostname, _ := os.Hostname()
	path := fmt.Sprintf("%s/date=%d-%d-%d/hour=%d/minute=%d/%s.data.parquet",
		stream,
		year, month, day,
		now.Hour(),
		now.Minute(),
		hostname,
	) // NOTE: This logic should be in sync with Parseable's.
	return path
}

func downloadParquetFromMinio(t *testing.T, path string, config MinIoConfig, downloadPath string) {
	s3Client, err := minio.New(config.Url, config.User, config.Pass, false)
	if err != nil {
		t.Fatal("can't create minio client", err)
	}

	fmt.Printf("Getting the object at %s in %s...\n", path, config.Bucket)

	parquetObj, err := s3Client.GetObject(config.Bucket, path, minio.GetObjectOptions{})
	if err != nil {
		t.Fatal("can't get object", err)
	}

	// Write the MinIO Object we got, into `downloadPath`.

	f, _ := os.Create(downloadPath)
	_, err = io.Copy(f, parquetObj)

	if err != nil {
		t.Fatal("couldn't copy:", err)
	}

	f.Close()
}

func loadFlogsFromParquetFile(t *testing.T, parquetFile string) []ParquetFlog {
	fr, err := local.NewLocalFileReader(parquetFile)
	if err != nil {
		t.Fatal("unable to read the parquet file", err)
	}

	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(ParquetFlog), 4)
	if err != nil {
		t.Fatal("can't create parquet reader", err)
	}

	defer pr.ReadStop()

	rowCount := int(pr.GetNumRows())
	flogs := make([]ParquetFlog, rowCount)

	if err = pr.Read(&flogs); err != nil {
		t.Fatal("parquet read error", err)
	}

	return flogs
}
