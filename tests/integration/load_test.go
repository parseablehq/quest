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
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	vus                     = "10"
	duration                = "2m"
	schema_count            = "10"
	events_count            = "5"
	parseableLoadSettleWait = 3 * time.Minute // Allows asynchronous flush and conversion to finish.
)

// loadPhaseLock lets all finite regular load tests join the active read phase,
// while the smoke load remains exclusive from that phase.
type loadPhaseLock struct {
	readersMu  sync.Mutex
	resourceMu sync.Mutex
	readers    int
}

func (l *loadPhaseLock) RLock() {
	l.readersMu.Lock()
	l.readers++
	if l.readers == 1 {
		l.resourceMu.Lock()
	}
	l.readersMu.Unlock()
}

func (l *loadPhaseLock) RUnlock() {
	l.readersMu.Lock()
	l.readers--
	if l.readers == 0 {
		l.resourceMu.Unlock()
	}
	l.readersMu.Unlock()
}

func (l *loadPhaseLock) Lock() {
	l.resourceMu.Lock()
}

func (l *loadPhaseLock) Unlock() {
	l.resourceMu.Unlock()
}

var k6Mu loadPhaseLock

func TestLoadStreamBatchWithK6_StaticSchema(t *testing.T) {
	// Verifies batch ingestion into a static-schema stream under load.
	if NewGlob.Mode == "load" {
		t.Parallel()

		staticSchemaStream := NewGlob.Stream + "loadbatchstaticschema"
		staticSchemaFlagHeader := map[string]string{"X-P-Static-Schema-Flag": "true"}
		CreateStreamWithSchemaBody(t, NewGlob.QueryClient, staticSchemaStream, staticSchemaFlagHeader, SchemaPayload)
		if NewGlob.IngestorUrl.String() == "" {
			cmd := exec.Command("k6",
				"run",
				"--address", "",
				"--vus", vus,
				"--duration", duration,
				"-e", fmt.Sprintf("P_URL=%s", &NewGlob.QueryUrl),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", staticSchemaStream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
				"./scripts/load_batch_events.js")

			runK6Load(t, cmd)
		} else {
			cmd := exec.Command("k6",
				"run",
				"--address", "",
				"--vus", vus,
				"--duration", duration,
				"-e", fmt.Sprintf("P_URL=%s", &NewGlob.IngestorUrl),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", staticSchemaStream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
				"./scripts/load_batch_events.js")

			runK6Load(t, cmd)
		}
	}
}

func TestSmokeLoadWithK6Streams(t *testing.T) {
	// Verifies smoke ingestion for normal and custom-partition streams.
	t.Parallel()
	stream := NewGlob.Stream + "smokeload"
	CreateStream(t, NewGlob.PBClient, stream)
	k6Mu.Lock()
	defer k6Mu.Unlock()
	runK6Smoke(t, stream)

	customPartitionStream := NewGlob.Stream + "smokeloadcustompartition"
	customHeader := map[string]string{"X-P-Custom-Partition": "level"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, customPartitionStream, customHeader)
	runK6Smoke(t, customPartitionStream)

	time.Sleep(parseableLoadSettleWait)

	t.Run("LoadWithK6Stream", func(t *testing.T) {
		QueryLogStreamCount(t, NewGlob.PBClient, stream, 20000)
		AssertStreamSchema(t, NewGlob.QueryClient, stream, SchemaBody)
	})

	t.Run("Load_CustomPartition_WithK6Stream", func(t *testing.T) {
		QueryLogStreamCount(t, NewGlob.PBClient, customPartitionStream, 20000)
	})

}

func runK6Smoke(t *testing.T, stream string) {
	t.Helper()
	url := NewGlob.QueryUrl.String()
	username := NewGlob.QueryUsername
	password := NewGlob.QueryPassword
	if NewGlob.IngestorUrl.String() != "" {
		url = NewGlob.IngestorUrl.String()
		username = NewGlob.IngestorUsername
		password = NewGlob.IngestorPassword
	}

	cmd := exec.Command("k6",
		"run",
		"--address", "",
		"-e", fmt.Sprintf("P_URL=%s", url),
		"-e", fmt.Sprintf("P_USERNAME=%s", username),
		"-e", fmt.Sprintf("P_PASSWORD=%s", password),
		"-e", fmt.Sprintf("P_STREAM=%s", stream),
		"./scripts/smoke.js")

	op, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "k6 failed: %s", string(op))
	t.Log(string(op))
}

func runK6Load(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	k6Mu.RLock()
	defer k6Mu.RUnlock()
	op, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "k6 failed: %s", string(op))
	t.Log(string(op))
	time.Sleep(parseableLoadSettleWait)
}

// func TestSmokeLoad_TimePartition_WithK6Stream(t *testing.T) {
// 	time_partition_stream := NewGlob.Stream + "timepartition"
// 	timeHeader := map[string]string{"X-P-Time-Partition": "source_time", "X-P-Time-Partition-Limit": "365d"}
// 	CreateStreamWithHeader(t, NewGlob.QueryClient, time_partition_stream, timeHeader)
// 	if NewGlob.IngestorUrl.String() == "" {
// 		cmd := exec.Command("k6",
// 			"run",
// 			"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
// 			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
// 			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
// 			"-e", fmt.Sprintf("P_STREAM=%s", time_partition_stream),
// 			"./scripts/smoke.js")

// 		cmd.Run()
// 		cmd.Output()
// 	} else {
// 		cmd := exec.Command("k6",
// 			"run",
// 			"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
// 			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
// 			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
// 			"-e", fmt.Sprintf("P_STREAM=%s", time_partition_stream),
// 			"./scripts/smoke.js")

// 		cmd.Run()
// 		cmd.Output()
// 	}
// 	time.Sleep(120 * time.Second)
// 	QueryLogStreamCount_Historical(t, NewGlob.PBClient, time_partition_stream, 20000)
// 	DeleteStream(t, NewGlob.PBClient, time_partition_stream)
// }

// func TestSmokeLoad_TimeAndCustomPartition_WithK6Stream(t *testing.T) {
// 	custom_partition_stream := NewGlob.Stream + "timecustompartition"
// 	customHeader := map[string]string{"X-P-Custom-Partition": "level", "X-P-Time-Partition": "source_time", "X-P-Time-Partition-Limit": "365d"}
// 	CreateStreamWithHeader(t, NewGlob.QueryClient, custom_partition_stream, customHeader)
// 	if NewGlob.IngestorUrl.String() == "" {
// 		cmd := exec.Command("k6",
// 			"run",
// 			"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
// 			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
// 			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
// 			"-e", fmt.Sprintf("P_STREAM=%s", custom_partition_stream),
// 			"./scripts/smoke.js")

// 		cmd.Run()
// 		cmd.Output()
// 	} else {
// 		cmd := exec.Command("k6",
// 			"run",
// 			"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
// 			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
// 			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
// 			"-e", fmt.Sprintf("P_STREAM=%s", custom_partition_stream),
// 			"./scripts/smoke.js")

// 		cmd.Run()
// 		cmd.Output()
// 	}
// 	time.Sleep(180 * time.Second)
// 	QueryLogStreamCount_Historical(t, NewGlob.PBClient, custom_partition_stream, 20000)
// 	DeleteStream(t, NewGlob.PBClient, custom_partition_stream)
// }

func TestLoadStreamBatchWithK6(t *testing.T) {
	// Verifies batch ingestion into a normal stream under load.
	if NewGlob.Mode == "load" {
		t.Parallel()

		stream := NewGlob.Stream + "loadbatch"
		CreateStream(t, NewGlob.PBClient, stream)
		if NewGlob.IngestorUrl.String() == "" {
			cmd := exec.Command("k6",
				"run",
				"--address", "",
				"--vus", vus,
				"--duration", duration,
				"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", stream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
				"./scripts/load_batch_events.js")

			runK6Load(t, cmd)
		} else {
			cmd := exec.Command("k6",
				"run",
				"--address", "",
				"--vus", vus,
				"--duration", duration,
				"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", stream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
				"./scripts/load_batch_events.js")

			runK6Load(t, cmd)
		}
	}
}

// func TestLoadHistoricalStreamBatchWithK6(t *testing.T) {
// 	if NewGlob.Mode == "load" {
// 		historicalStream := NewGlob.Stream + "historical"
// 		timeHeader := map[string]string{"X-P-Time-Partition": "source_time"}
// 		CreateStreamWithHeader(t, NewGlob.QueryClient, historicalStream, timeHeader)
// 		if NewGlob.IngestorUrl.String() == "" {
// 			cmd := exec.Command("k6",
// 				"run",
// 				"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
// 				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
// 				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
// 				"-e", fmt.Sprintf("P_STREAM=%s", historicalStream),
// 				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
// 				"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
// 				"./scripts/load_historical_batch_events.js",
// 				"--vus=", vus,
// 				"--duration=", duration)

// 			cmd.Run()
// 			op, err := cmd.Output()
// 			if err != nil {
// 				t.Log(err)
// 			}
// 			t.Log(string(op))
// 		} else {
// 			cmd := exec.Command("k6",
// 				"run",
// 				"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
// 				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
// 				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
// 				"-e", fmt.Sprintf("P_STREAM=%s", historicalStream),
// 				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
// 				"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
// 				"./scripts/load_historical_batch_events.js",
// 				"--vus=", vus,
// 				"--duration=", duration)

// 			cmd.Run()
// 			op, err := cmd.Output()
// 			if err != nil {
// 				t.Log(err)
// 			}
// 			t.Log(string(op))
// 		}

// 		DeleteStream(t, NewGlob.PBClient, historicalStream)
// 	}
// }

func TestLoadStreamBatchWithCustomPartitionWithK6(t *testing.T) {
	// Verifies batch ingestion into a custom-partition stream under load.
	if NewGlob.Mode != "load" {
		return
	}
	t.Parallel()

	customPartitionStream := NewGlob.Stream + "loadbatchcustompartition"
	customHeader := map[string]string{"X-P-Custom-Partition": "level"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, customPartitionStream, customHeader)
	if NewGlob.IngestorUrl.String() == "" {
		cmd := exec.Command("k6",
			"run",
			"--address", "",
			"--vus", vus,
			"--duration", duration,
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", customPartitionStream),
			"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
			"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
			"./scripts/load_batch_events.js")

		runK6Load(t, cmd)
	} else {
		cmd := exec.Command("k6",
			"run",
			"--address", "",
			"--vus", vus,
			"--duration", duration,
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", customPartitionStream),
			"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
			"-e", fmt.Sprintf("P_EVENTS_COUNT=%s", events_count),
			"./scripts/load_batch_events.js")

		runK6Load(t, cmd)
	}
}

func TestLoadStreamNoBatchWithK6(t *testing.T) {
	// Verifies single-event ingestion into a normal stream under load.
	if NewGlob.Mode == "load" {
		t.Parallel()

		stream := NewGlob.Stream + "loadsingle"
		CreateStream(t, NewGlob.PBClient, stream)
		if NewGlob.IngestorUrl.String() == "" {
			cmd := exec.Command("k6",
				"run",
				"--address", "",
				"--vus", vus,
				"--duration", duration,
				"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", stream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"./scripts/load_single_event.js")

			runK6Load(t, cmd)
		} else {
			cmd := exec.Command("k6",
				"run",
				"--address", "",
				"--vus", vus,
				"--duration", duration,
				"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
				"-e", fmt.Sprintf("P_STREAM=%s", stream),
				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
				"./scripts/load_single_event.js")

			runK6Load(t, cmd)
		}

	}
}

// func TestLoadHistoricalStreamNoBatchWithK6(t *testing.T) {
// 	if NewGlob.Mode == "load" {
// 		historicalStream := NewGlob.Stream + "historical"
// 		timeHeader := map[string]string{"X-P-Time-Partition": "source_time"}
// 		CreateStreamWithHeader(t, NewGlob.QueryClient, historicalStream, timeHeader)
// 		if NewGlob.IngestorUrl.String() == "" {
// 			cmd := exec.Command("k6",
// 				"run",
// 				"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
// 				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
// 				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
// 				"-e", fmt.Sprintf("P_STREAM=%s", historicalStream),
// 				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
// 				"./scripts/load_single_events.js",
// 				"--vus=", vus,
// 				"--duration=", duration)

// 			cmd.Run()
// 			op, err := cmd.Output()
// 			if err != nil {
// 				t.Log(err)
// 			}
// 			t.Log(string(op))
// 		} else {
// 			cmd := exec.Command("k6",
// 				"run",
// 				"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
// 				"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
// 				"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
// 				"-e", fmt.Sprintf("P_STREAM=%s", historicalStream),
// 				"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
// 				"./scripts/load_single_events.js",
// 				"--vus=", vus,
// 				"--duration=", duration)

// 			cmd.Run()
// 			op, err := cmd.Output()
// 			if err != nil {
// 				t.Log(err)
// 			}
// 			t.Log(string(op))
// 		}

// 		DeleteStream(t, NewGlob.PBClient, historicalStream)
// 	}
// }

func TestLoadStreamNoBatchWithCustomPartitionWithK6(t *testing.T) {
	// Verifies single-event ingestion into a custom-partition stream under load.
	if NewGlob.Mode != "load" {
		return
	}
	t.Parallel()

	customPartitionStream := NewGlob.Stream + "loadsinglecustompartition"
	customHeader := map[string]string{"X-P-Custom-Partition": "level"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, customPartitionStream, customHeader)
	if NewGlob.IngestorUrl.String() == "" {
		cmd := exec.Command("k6",
			"run",
			"--address", "",
			"--vus", vus,
			"--duration", duration,
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.QueryUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.QueryUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.QueryPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", customPartitionStream),
			"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
			"./scripts/load_single_event.js")

		runK6Load(t, cmd)
	} else {
		cmd := exec.Command("k6",
			"run",
			"--address", "",
			"--vus", vus,
			"--duration", duration,
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.IngestorUrl.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.IngestorUsername),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.IngestorPassword),
			"-e", fmt.Sprintf("P_STREAM=%s", customPartitionStream),
			"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
			"./scripts/load_single_event.js")

		runK6Load(t, cmd)
	}
}
