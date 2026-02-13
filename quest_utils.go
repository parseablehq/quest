// Copyright (c) 2023 Cloudnatively Services Pvt Ltd
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
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// TestMain — Health gate: polls liveness + readiness before running any tests.
// ---------------------------------------------------------------------------

func TestMain(m *testing.M) {
	const (
		maxWait  = 60 * time.Second
		interval = 2 * time.Second
	)

	deadline := time.Now().Add(maxWait)
	ready := false

	for time.Now().Before(deadline) {
		if checkEndpoint(NewGlob.QueryClient, "liveness") && checkEndpoint(NewGlob.QueryClient, "readiness") {
			ready = true
			break
		}
		time.Sleep(interval)
	}

	if !ready {
		fmt.Fprintf(os.Stderr, "FATAL: Parseable not reachable at %s after %s (checked /liveness + /readiness)\n",
			NewGlob.QueryUrl.String(), maxWait)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func checkEndpoint(client HTTPClient, path string) bool {
	req, err := client.NewRequest("GET", path, nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// ---------------------------------------------------------------------------
// WaitForIngest — Polls query count until >= minCount or timeout.
// Replaces all hardcoded time.Sleep calls after ingestion.
// ---------------------------------------------------------------------------

func WaitForIngest(t *testing.T, client HTTPClient, stream string, minCount int, timeout time.Duration) int {
	t.Helper()

	const pollInterval = 2 * time.Second
	deadline := time.Now().Add(timeout)

	for {
		count := queryCount(client, stream)
		if count >= minCount {
			return count
		}
		if time.Now().After(deadline) {
			t.Fatalf("WaitForIngest: timed out after %s waiting for stream %q to reach %d events (last count: %d)",
				timeout, stream, minCount, count)
		}
		time.Sleep(pollInterval)
	}
}

// WaitForQueryable — Polls until stream returns count > 0.
// Lighter variant of WaitForIngest for "data landed" checks.
func WaitForQueryable(t *testing.T, client HTTPClient, stream string, timeout time.Duration) {
	t.Helper()
	WaitForIngest(t, client, stream, 1, timeout)
}

// queryCount returns the current count for a stream, or -1 on error.
func queryCount(client HTTPClient, stream string) int {
	endTime := time.Now().Add(time.Second).Format(time.RFC3339Nano)
	startTime := time.Now().Add(-30 * time.Minute).Format(time.RFC3339Nano)

	query := map[string]interface{}{
		"query":     "select count(*) as count from " + stream,
		"startTime": startTime,
		"endTime":   endTime,
	}
	queryJSON, _ := json.Marshal(query)
	req, err := client.NewRequest("POST", "query", bytes.NewBuffer(queryJSON))
	if err != nil {
		return -1
	}
	resp, err := client.Do(req)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return -1
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return -1
	}

	var results []map[string]float64
	if err := json.Unmarshal(body, &results); err != nil {
		return -1
	}
	if len(results) == 0 {
		return 0
	}
	return int(results[0]["count"])
}

// ---------------------------------------------------------------------------
// UniqueStream — Returns a unique stream name with the given prefix.
// Uses crypto/rand for collision-free names in parallel/repeated runs.
// ---------------------------------------------------------------------------

func UniqueStream(prefix string) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return prefix + "_" + hex.EncodeToString(b)
}

// ---------------------------------------------------------------------------
// ResourceTracker — Guarantees cleanup even on t.Fatal/panic.
// Registers t.Cleanup in constructor; runs all cleanups in LIFO order.
// ---------------------------------------------------------------------------

type ResourceTracker struct {
	t        *testing.T
	client   HTTPClient
	mu       sync.Mutex
	cleanups []func()
}

func NewResourceTracker(t *testing.T, client HTTPClient) *ResourceTracker {
	t.Helper()
	rt := &ResourceTracker{t: t, client: client}
	t.Cleanup(rt.cleanup)
	return rt
}

func (rt *ResourceTracker) cleanup() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	// Run in LIFO order
	for i := len(rt.cleanups) - 1; i >= 0; i-- {
		rt.cleanups[i]()
	}
}

func (rt *ResourceTracker) addCleanup(fn func()) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.cleanups = append(rt.cleanups, fn)
}

func (rt *ResourceTracker) TrackStream(stream string) {
	rt.addCleanup(func() {
		deleteIgnoring404(rt.client, "DELETE", "logstream/"+stream)
	})
}

func (rt *ResourceTracker) TrackUser(username string) {
	rt.addCleanup(func() {
		deleteIgnoring404(rt.client, "DELETE", "user/"+username)
	})
}

func (rt *ResourceTracker) TrackRole(roleName string) {
	rt.addCleanup(func() {
		deleteIgnoring404(rt.client, "DELETE", "role/"+roleName)
	})
}

func (rt *ResourceTracker) TrackAlert(alertID string) {
	rt.addCleanup(func() {
		deleteIgnoring404(rt.client, "DELETE", "alerts/"+alertID)
	})
}

func (rt *ResourceTracker) TrackTarget(targetID string) {
	rt.addCleanup(func() {
		deleteIgnoring404(rt.client, "DELETE", "targets/"+targetID)
	})
}

func (rt *ResourceTracker) TrackDashboard(dashID string) {
	rt.addCleanup(func() {
		deleteIgnoring404(rt.client, "DELETE", "dashboards/"+dashID)
	})
}

func (rt *ResourceTracker) TrackFilter(filterID string) {
	rt.addCleanup(func() {
		deleteIgnoring404(rt.client, "DELETE", "filters/"+filterID)
	})
}

func (rt *ResourceTracker) TrackCorrelation(corrID string) {
	rt.addCleanup(func() {
		deleteIgnoring404(rt.client, "DELETE", "correlation/"+corrID)
	})
}

func (rt *ResourceTracker) AddCustomCleanup(fn func()) {
	rt.addCleanup(fn)
}

// deleteIgnoring404 sends a DELETE (or other method) request and ignores 404.
func deleteIgnoring404(client HTTPClient, method, path string) {
	req, err := client.NewRequest(method, path, nil)
	if err != nil {
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
	// 200 or 404 are both fine during cleanup
}

// ---------------------------------------------------------------------------
// RunFlogAuto — Wraps RunFlog using the appropriate client.
// ---------------------------------------------------------------------------

func RunFlogAuto(t *testing.T, stream string) {
	t.Helper()
	if NewGlob.IngestorUrl.String() != "" {
		RunFlog(t, NewGlob.IngestorClient, stream)
	} else {
		RunFlog(t, NewGlob.QueryClient, stream)
	}
}

// ---------------------------------------------------------------------------
// Response Assertion Helpers
// ---------------------------------------------------------------------------

func AssertResponseCode(t *testing.T, resp *http.Response, expected int) {
	t.Helper()
	if resp.StatusCode != expected {
		body, _ := io.ReadAll(resp.Body)
		snippet := string(body)
		if len(snippet) > 200 {
			snippet = snippet[:200] + "..."
		}
		t.Fatalf("Expected status %d, got %d. Body: %s", expected, resp.StatusCode, snippet)
	}
}

func AssertResponseBodyContains(t *testing.T, resp *http.Response, substring string) {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %s", err)
	}
	if !strings.Contains(string(body), substring) {
		snippet := string(body)
		if len(snippet) > 200 {
			snippet = snippet[:200] + "..."
		}
		t.Fatalf("Response body does not contain %q. Body: %s", substring, snippet)
	}
}

func AssertResponseBodyNotEmpty(t *testing.T, resp *http.Response) {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %s", err)
	}
	if len(body) == 0 {
		t.Fatal("Response body is empty")
	}
}
