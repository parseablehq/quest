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

	"github.com/stretchr/testify/require"
)

func TestSmokeListLogStream(t *testing.T) {
	// Verifies that the dataset list includes a newly created stream.
	t.Parallel()
	streamName := NewGlob.Stream + "list"
	CreateStream(t, NewGlob.PBClient, streamName)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.PBClient, streamName)
	})
	datasets := ListDatasetsWithPB(t, NewGlob.PBClient)
	require.Contains(t, datasets, PBDataset{Title: streamName})
}

func TestSmokeCreateStream(t *testing.T) {
	// Verifies that PB creates a logs dataset.
	t.Parallel()
	stream := NewGlob.Stream + "create"
	CreateStream(t, NewGlob.PBClient, stream)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.PBClient, stream)
	})
	info := DatasetInfoWithPB(t, NewGlob.PBClient, stream)
	require.Equal(t, "logs", info.DatasetType)
}

func TestSmokeDeleteStream(t *testing.T) {
	// Verifies that PB deletes an empty stream.
	t.Parallel()
	stream := NewGlob.Stream + "delete"
	CreateStream(t, NewGlob.PBClient, stream)
	DeleteStream(t, NewGlob.PBClient, stream)
	datasets := ListDatasetsWithPB(t, NewGlob.PBClient)
	require.NotContains(t, datasets, PBDataset{Title: stream})
}

func TestSmokeDetectSchema(t *testing.T) {
	// Verifies that schema detection returns the expected schema.
	t.Parallel()
	DetectSchema(t, NewGlob.QueryClient, SampleJson, SchemaBody)
}

// func TestTimePartition_TimeStampMismatch(t *testing.T) {
// 	historicalStream := NewGlob.Stream + "historical"
// 	timeHeader := map[string]string{"X-P-Time-Partition": "source_time"}
// 	CreateStreamWithHeader(t, NewGlob.QueryClient, historicalStream, timeHeader)
// 	if NewGlob.IngestorUrl.String() == "" {
// 		IngestOneEventWithTimePartition_TimeStampMismatch(t, NewGlob.QueryClient, historicalStream)
// 	} else {
// 		IngestOneEventWithTimePartition_TimeStampMismatch(t, NewGlob.IngestorClient, historicalStream)
// 	}
// 	DeleteStream(t, NewGlob.PBClient, historicalStream)
// }

// func TestTimePartition_NoTimePartitionInLog(t *testing.T) {
// 	historicalStream := NewGlob.Stream + "historical"
// 	timeHeader := map[string]string{"X-P-Time-Partition": "source_time"}
// 	CreateStreamWithHeader(t, NewGlob.QueryClient, historicalStream, timeHeader)
// 	if NewGlob.IngestorUrl.String() == "" {
// 		IngestOneEventWithTimePartition_NoTimePartitionInLog(t, NewGlob.QueryClient, historicalStream)
// 	} else {
// 		IngestOneEventWithTimePartition_NoTimePartitionInLog(t, NewGlob.IngestorClient, historicalStream)
// 	}
// 	DeleteStream(t, NewGlob.PBClient, historicalStream)
// }

// func TestTimePartition_IncorrectDateTimeFormatTimePartitionInLog(t *testing.T) {
// 	historicalStream := NewGlob.Stream + "historical"
// 	timeHeader := map[string]string{"X-P-Time-Partition": "source_time"}
// 	CreateStreamWithHeader(t, NewGlob.QueryClient, historicalStream, timeHeader)
// 	if NewGlob.IngestorUrl.String() == "" {
// 		IngestOneEventWithTimePartition_IncorrectDateTimeFormatTimePartitionInLog(t, NewGlob.QueryClient, historicalStream)
// 	} else {
// 		IngestOneEventWithTimePartition_IncorrectDateTimeFormatTimePartitionInLog(t, NewGlob.IngestorClient, historicalStream)
// 	}
// 	DeleteStream(t, NewGlob.PBClient, historicalStream)
// }

func TestStaticSchemaIngestion(t *testing.T) {
	// Verifies that a static schema accepts matching fields and rejects new fields.
	t.Parallel()
	staticSchemaStream := NewGlob.Stream + "staticschema"
	staticSchemaFlagHeader := map[string]string{"X-P-Static-Schema-Flag": "true"}
	CreateStreamWithSchemaBody(t, NewGlob.QueryClient, staticSchemaStream, staticSchemaFlagHeader, SchemaPayload)

	client := NewGlob.QueryClient
	if NewGlob.IngestorUrl.String() != "" {
		client = NewGlob.IngestorClient
	}

	t.Run("AcceptMatchingFields", func(t *testing.T) {
		IngestOneEventForStaticSchemaStream_SameFieldsInLog(t, client, staticSchemaStream)
	})
	t.Run("RejectNewField", func(t *testing.T) {
		IngestOneEventForStaticSchemaStream_NewFieldInLog(t, client, staticSchemaStream)
	})
}

func TestCreateStream_WithCustomPartition_Success(t *testing.T) {
	// Verifies that a stream accepts one custom partition field.
	t.Parallel()
	customPartitionStream := NewGlob.Stream + "custompartitionsuccess"
	customHeader := map[string]string{"X-P-Custom-Partition": "level"}
	CreateStreamWithHeader(t, NewGlob.QueryClient, customPartitionStream, customHeader)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.PBClient, customPartitionStream)
	})
}

func TestCreateStream_WithCustomPartition_Error(t *testing.T) {
	// Verifies that multiple custom partition fields are rejected.
	t.Parallel()
	customPartitionStream := NewGlob.Stream + "custompartitionerror"
	customHeader := map[string]string{"X-P-Custom-Partition": "level,os"}
	CreateStreamWithCustompartitionError(t, NewGlob.QueryClient, customPartitionStream, customHeader)
}
