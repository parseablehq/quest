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
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSmokeSetRetention(t *testing.T) {
	// Verifies that retention rules can be set on a stream.
	t.Parallel()
	stream := NewGlob.Stream + "setretention"
	CreateStream(t, NewGlob.PBClient, stream)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.PBClient, stream)
	})
	req, _ := NewGlob.QueryClient.NewRequest("PUT", "logstream/"+stream+"/retention", strings.NewReader(RetentionBody))
	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))
}

func TestSmokeGetRetention(t *testing.T) {
	// Verifies that PB returns the configured retention rules.
	t.Parallel()
	stream := NewGlob.Stream + "getretention"
	CreateStream(t, NewGlob.PBClient, stream)
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.PBClient, stream)
	})

	req, _ := NewGlob.QueryClient.NewRequest("PUT", "logstream/"+stream+"/retention", strings.NewReader(RetentionBody))
	response, err := NewGlob.QueryClient.Do(req)
	require.NoErrorf(t, err, "Request failed: %s", err)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, readAsString(response.Body))

	info := DatasetInfoWithPB(t, NewGlob.PBClient, stream)
	var expected []PBRetentionRule
	require.NoError(t, json.Unmarshal([]byte(RetentionBody), &expected))
	require.Equal(t, expected, info.Retention, "Get retention response doesn't match with retention config returned")
}
