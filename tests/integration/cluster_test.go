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
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

type clusterNode struct {
	DomainName string  `json:"domain_name"`
	Reachable  bool    `json:"reachable"`
	NodeType   string  `json:"node_type"`
	Status     *string `json:"status"`
}

type clusterMetric struct {
	Address  string `json:"address"`
	NodeType string `json:"node_type"`
}

func clusterRequest(t *testing.T, method, path string, payload any) (int, []byte) {
	t.Helper()
	var body *bytes.Reader
	if payload == nil {
		body = bytes.NewReader(nil)
	} else {
		encoded, err := json.Marshal(payload)
		require.NoError(t, err)
		body = bytes.NewReader(encoded)
	}
	req, err := NewGlob.QueryClient.NewRequest(method, path, body)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	return response.StatusCode, []byte(readAsString(response.Body))
}

func TestSmokeClusterManagement(t *testing.T) {
	t.Parallel()

	if NewGlob.IngestorUrl.String() == "" {
		for _, path := range []string{"cluster/info", "cluster/metrics"} {
			status, body := clusterRequest(t, "GET", path, nil)
			require.Equalf(t, 404, status, "standalone exposed distributed endpoint %s: %s", path, body)
		}
		return
	}

	status, body := clusterRequest(t, "GET", "cluster/info", nil)
	require.Equalf(t, 200, status, "cluster info failed: %s", body)
	var nodes []clusterNode
	require.NoError(t, json.Unmarshal(body, &nodes))
	require.NotEmpty(t, nodes)

	var liveIngestor *clusterNode
	var hasQuerier bool
	for i := range nodes {
		node := &nodes[i]
		require.NotEmpty(t, node.DomainName)
		require.NotEmpty(t, node.NodeType)
		if node.NodeType == "ingestor" && node.Reachable {
			liveIngestor = node
		}
		if node.NodeType == "querier" {
			hasQuerier = true
		}
	}
	require.NotNil(t, liveIngestor, "cluster info did not contain a reachable ingestor")
	require.True(t, hasQuerier, "cluster info did not contain a querier")

	t.Run("ClusterMetrics", func(t *testing.T) {
		status, body := clusterRequest(t, "GET", "cluster/metrics", nil)
		require.Equalf(t, 200, status, "cluster metrics failed: %s", body)
		var metrics []clusterMetric
		require.NoError(t, json.Unmarshal(body, &metrics))
		require.NotEmpty(t, metrics)
		for _, metric := range metrics {
			require.NotEmpty(t, metric.Address)
			require.NotEmpty(t, metric.NodeType)
		}
	})
}
