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
	"testing"
)

const (
	vus          = "10"
	duration     = "5m"
	schema_count = "20"
)

func TestStreamBatchLoadWithK6(t *testing.T) {
	if NewGlob.Mode == "load" {
		CreateStream(t, NewGlob.Client, NewGlob.Stream)
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.Url.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.Username),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.Password),
			"-e", fmt.Sprintf("P_STREAM=%s", NewGlob.Stream),
			"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
			"./scripts/load_batch_events.js",
			"--vus=", vus,
			"--duration=", duration)

		cmd.Run()
		op, err := cmd.Output()
		if err != nil {
			t.Log(err)
		}
		t.Log(string(op))
	}
}

func TestStreamLoadWithK6(t *testing.T) {
	if NewGlob.Mode == "load" {
		cmd := exec.Command("k6",
			"run",
			"-e", fmt.Sprintf("P_URL=%s", NewGlob.Url.String()),
			"-e", fmt.Sprintf("P_USERNAME=%s", NewGlob.Username),
			"-e", fmt.Sprintf("P_PASSWORD=%s", NewGlob.Password),
			"-e", fmt.Sprintf("P_STREAM=%s", NewGlob.Stream),
			"-e", fmt.Sprintf("P_SCHEMA_COUNT=%s", schema_count),
			"./scripts/load_single_events.js",
			"--vus=", vus,
			"--duration=", duration)

		cmd.Run()
		op, err := cmd.Output()
		if err != nil {
			t.Log(err)
		}
		t.Log(string(op))
	}
}
