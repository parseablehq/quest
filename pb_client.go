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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"time"
)

const defaultPBTimeout = 60 * time.Second

type PBClient struct {
	Binary  string
	Timeout time.Duration
}

type PBResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
	Duration time.Duration
}

func DefaultPBClient(binary string) PBClient {
	return PBClient{
		Binary:  binary,
		Timeout: defaultPBTimeout,
	}
}

func (client PBClient) Run(ctx context.Context, args ...string) (PBResult, error) {
	if client.Binary == "" {
		client.Binary = "pb"
	}

	if client.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, client.Timeout)
		defer cancel()
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, client.Binary, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	startedAt := time.Now()
	err := cmd.Run()
	result := PBResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: 0,
		Duration: time.Since(startedAt),
	}

	if err == nil {
		return result, nil
	}

	result.ExitCode = -1
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		result.ExitCode = exitError.ExitCode()
	}

	if ctx.Err() != nil {
		return result, fmt.Errorf("pb command timed out: %w", ctx.Err())
	}

	return result, err
}

func (client PBClient) RunJSON(ctx context.Context, output any, args ...string) (PBResult, error) {
	jsonArgs := append(append([]string{}, args...), "-o", "json")
	result, err := client.Run(ctx, jsonArgs...)
	if err != nil {
		return result, err
	}

	if err := json.Unmarshal([]byte(result.Stdout), output); err != nil {
		return result, fmt.Errorf("decode pb JSON output: %w", err)
	}

	return result, nil
}
