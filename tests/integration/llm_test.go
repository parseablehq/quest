// Copyright (c) 2023 Cloudnatively Services Pvt Ltd
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
	"testing"

	"github.com/stretchr/testify/require"
)

type testLLMConfig struct {
	Provider          string   `json:"provider"`
	Title             string   `json:"title"`
	APIKey            string   `json:"apiKey"`
	URL               string   `json:"url"`
	ID                string   `json:"id"`
	IsDefault         bool     `json:"isDefault"`
	ModelsList        []string `json:"modelsList"`
	DefaultModel      string   `json:"defaultModel"`
	ResponsesEndpoint string   `json:"responsesEndpoint"`
}

type testLLMConfigList struct {
	LLMConfigs []testLLMConfig `json:"llmConfigs"`
}

type testLLMModelList struct {
	ID       string   `json:"id"`
	Name     string   `json:"name"`
	Default  bool     `json:"default"`
	Provider string   `json:"provider"`
	Models   []string `json:"models"`
}

func llmReadRequest(t *testing.T, path string) []byte {
	t.Helper()
	req, err := NewGlob.QueryClient.NewRequestAtPath("GET", path, nil)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	body := []byte(readAsString(response.Body))
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	return body
}

func TestEnterpriseLLMConfigurationAPIs(t *testing.T) {
	// Verifies the LLM read APIs without invoking an external LLM provider.
	if NewGlob.Edition != "enterprise" {
		t.Skip("LLM configuration is only available in Enterprise")
	}
	t.Parallel()

	t.Run("ListConfigs", func(t *testing.T) {
		body := llmReadRequest(t, "api/prism/v1/llm")
		var result testLLMConfigList
		require.NoError(t, json.Unmarshal(body, &result))
		require.NotNil(t, result.LLMConfigs)
	})

	t.Run("ListModels", func(t *testing.T) {
		body := llmReadRequest(t, "api/prism/v1/llm/models")
		var result []testLLMModelList
		require.NoError(t, json.Unmarshal(body, &result))
		require.NotNil(t, result)
	})
}
