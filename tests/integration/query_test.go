// Copyright (c) 2023 Cloudnatively Services Pvt Ltd
//
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"testing"
	"time"
)

func TestSmokeIngestAndQuery(t *testing.T) {
	// Verifies ingestion and SQL queries across two streams.
	t.Parallel()
	stream1 := NewGlob.Stream + "ingestquery1"
	stream2 := NewGlob.Stream + "ingestquery2"
	CreateStream(t, NewGlob.PBClient, stream1)
	CreateStream(t, NewGlob.PBClient, stream2)

	if NewGlob.IngestorUrl.String() == "" {
		RunFlog(t, NewGlob.QueryClient, stream1)
		RunFlog(t, NewGlob.QueryClient, stream2)
	} else {
		RunFlog(t, NewGlob.IngestorClient, stream1)
		RunFlog(t, NewGlob.IngestorClient, stream2)
	}

	// Parseable persists ingested events in a two-minute batch. Both streams are
	// populated before this wait so all ingestion and query assertions can share
	// the same batch window.
	time.Sleep(120 * time.Second)

	t.Run("IngestEventsToStream", func(t *testing.T) {
		QueryLogStreamCount(t, NewGlob.PBClient, stream1, 50)
		AssertStreamSchema(t, NewGlob.QueryClient, stream1, FlogJsonSchema)
	})

	t.Run("RunQueries", func(t *testing.T) {
		QueryLogStreamCount(t, NewGlob.PBClient, stream1, 50)
		AssertQueryOK(t, NewGlob.PBClient, "SELECT * FROM %s", stream1)
		AssertQueryOK(t, NewGlob.PBClient, "SELECT * FROM %s OFFSET 25 LIMIT 25", stream1)

		for _, item := range flogStreamFields() {
			AssertQueryOK(t, NewGlob.PBClient, "SELECT %s FROM %s", item, stream1)
		}

		AssertQueryOK(t, NewGlob.PBClient, "SELECT * FROM %s WHERE method = 'POST'", stream1)
		AssertQueryOK(t, NewGlob.PBClient, "SELECT method, COUNT(*) FROM %s GROUP BY method", stream1)
		AssertQueryOK(t, NewGlob.PBClient, `SELECT DATE_TRUNC('minute', p_timestamp) as minute, COUNT(*) FROM %s GROUP BY minute`, stream1)
	})

	t.Run("QueryTwoStreams", func(t *testing.T) {
		QueryTwoLogStreamCount(t, NewGlob.PBClient, stream1, stream2, 100)
	})
}
