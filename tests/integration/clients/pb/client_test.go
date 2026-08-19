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

package pb

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPBClientRun(t *testing.T) {
	// Verifies PB command output, errors, JSON, and timeouts.
	t.Setenv("QUEST_PB_HELPER_PROCESS", "1")

	client := PBClient{
		Binary:  os.Args[0],
		Timeout: time.Second,
	}
	command := []string{"-test.run=TestPBClientHelperProcess", "--"}

	t.Run("captures output", func(t *testing.T) {
		result, err := client.Run(context.Background(), append(command, "success")...)
		if err != nil {
			t.Fatalf("run command: %v", err)
		}
		if result.ExitCode != 0 {
			t.Fatalf("expected exit code 0, got %d", result.ExitCode)
		}
		if result.Stdout != `{"name":"pstats"}` {
			t.Fatalf("unexpected stdout: %q", result.Stdout)
		}
		if result.Stderr != "warning" {
			t.Fatalf("unexpected stderr: %q", result.Stderr)
		}
	})

	t.Run("returns exit code", func(t *testing.T) {
		result, err := client.Run(context.Background(), append(command, "failure")...)
		if err == nil {
			t.Fatal("expected command to fail")
		}
		if result.ExitCode != 7 {
			t.Fatalf("expected exit code 7, got %d", result.ExitCode)
		}
		if result.Stderr != "failed" {
			t.Fatalf("unexpected stderr: %q", result.Stderr)
		}
	})

	t.Run("decodes JSON", func(t *testing.T) {
		var output struct {
			Name string `json:"name"`
		}
		result, err := client.RunJSON(context.Background(), &output, append(command, "json")...)
		if err != nil {
			t.Fatalf("run JSON command: %v (stderr: %s)", err, result.Stderr)
		}
		if output.Name != "pstats" {
			t.Fatalf("unexpected decoded name: %q", output.Name)
		}
	})

	t.Run("enforces timeout", func(t *testing.T) {
		timeoutClient := client
		timeoutClient.Timeout = 50 * time.Millisecond

		result, err := timeoutClient.Run(context.Background(), append(command, "timeout")...)
		if err == nil || !strings.Contains(err.Error(), "timed out") {
			t.Fatalf("expected timeout error, got %v", err)
		}
		if result.ExitCode != -1 {
			t.Fatalf("expected exit code -1, got %d", result.ExitCode)
		}
	})
}

func TestPBClientHelperProcess(t *testing.T) {
	// Provides controlled command results for PB client tests.
	if os.Getenv("QUEST_PB_HELPER_PROCESS") != "1" {
		return
	}

	separator := -1
	for index, arg := range os.Args {
		if arg == "--" {
			separator = index
			break
		}
	}
	if separator == -1 || separator+1 >= len(os.Args) {
		os.Exit(2)
	}

	switch os.Args[separator+1] {
	case "success", "json":
		fmt.Fprint(os.Stdout, `{"name":"pstats"}`)
		if os.Args[separator+1] == "success" {
			fmt.Fprint(os.Stderr, "warning")
		}
		os.Exit(0)
	case "failure":
		fmt.Fprint(os.Stderr, "failed")
		os.Exit(7)
	case "timeout":
		time.Sleep(2 * time.Second)
	default:
		os.Exit(2)
	}
}

func TestPasswordFromPBUserAddOutput(t *testing.T) {
	// Verifies password parsing from PB user creation output.
	output := "Added user: alice\nPassword is: generated-password\nRole(s) assigned: reader\n"
	password, err := PasswordFromUserAddOutput(output)
	if err != nil {
		t.Fatalf("extract password: %v", err)
	}
	if password != "generated-password" {
		t.Fatalf("unexpected password: %q", password)
	}

	if _, err := PasswordFromUserAddOutput("Added user: alice\n"); err == nil {
		t.Fatal("expected missing password to fail")
	}
}
