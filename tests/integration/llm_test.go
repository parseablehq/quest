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

type testLLMConfig struct {
	Provider          string   `json:"provider"`
	Title             string   `json:"title"`
	APIKey            string   `json:"apiKey"`
	URL               string   `json:"url"`
	ID                string   `json:"id"`
	IsDefault         bool     `json:"isDefault"`
	ModelsList        []string `json:"modelsList"`
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

func llmRequest(t *testing.T, method, path string, payload any, expectedStatus int) []byte {
	t.Helper()
	var body *bytes.Reader
	if payload == nil {
		body = bytes.NewReader(nil)
	} else {
		encoded, err := json.Marshal(payload)
		require.NoError(t, err)
		body = bytes.NewReader(encoded)
	}

	req, err := NewGlob.QueryClient.NewRequestAtPath(method, path, body)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	responseBody := []byte(readAsString(response.Body))
	require.Equalf(t, expectedStatus, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, responseBody)
	return responseBody
}

func decodeTestLLMConfig(t *testing.T, body []byte) testLLMConfig {
	t.Helper()
	var config testLLMConfig
	require.NoError(t, json.Unmarshal(body, &config))
	return config
}

func listTestLLMConfigs(t *testing.T) []testLLMConfig {
	t.Helper()
	body := llmRequest(t, "GET", "api/prism/v1/llm", nil, 200)
	var result testLLMConfigList
	require.NoError(t, json.Unmarshal(body, &result))
	return result.LLMConfigs
}

func requireTestLLMConfig(t *testing.T, configs []testLLMConfig, id string) testLLMConfig {
	t.Helper()
	for _, config := range configs {
		if config.ID == id {
			return config
		}
	}
	t.Fatalf("LLM config %q was not returned", id)
	return testLLMConfig{}
}

func requireTestLLMModelList(t *testing.T, configs []testLLMModelList, id string) testLLMModelList {
	t.Helper()
	for _, config := range configs {
		if config.ID == id {
			return config
		}
	}
	t.Fatalf("LLM model config %q was not returned", id)
	return testLLMModelList{}
}

func TestEnterpriseLLMConfigLifecycle(t *testing.T) {
	// Verifies LLM config persistence, secret masking, models, defaults, and deletion rules.
	if NewGlob.Edition != "enterprise" {
		t.Skip("LLM configuration is only available in Enterprise")
	}
	t.Parallel()

	const maskedAPIKey = "qu********"
	basePath := "api/prism/v1/llm"
	titleOne := NewGlob.Stream + " llm one"
	titleTwo := NewGlob.Stream + " llm two"
	modelOne := NewGlob.Stream + "-model-one"
	modelTwo := NewGlob.Stream + "-model-two"
	secretOne := "quest-secret-key-one"
	secretTwo := "quest-secret-key-two"

	createPayload := func(title, secret, model string) map[string]any {
		return map[string]any{
			"provider":          "Custom",
			"title":             title,
			"apiKey":            secret,
			"url":               "http://unused-llm.test",
			"modelsList":        []string{model},
			"responsesEndpoint": "responses",
		}
	}

	createdOneBody := llmRequest(t, "POST", basePath, createPayload(titleOne, secretOne, modelOne), 200)
	require.NotContains(t, string(createdOneBody), secretOne)
	createdOne := decodeTestLLMConfig(t, createdOneBody)
	require.NotEmpty(t, createdOne.ID)
	require.Equal(t, "Custom", createdOne.Provider)
	require.Equal(t, titleOne, createdOne.Title)
	require.Equal(t, maskedAPIKey, createdOne.APIKey)
	require.Equal(t, []string{modelOne}, createdOne.ModelsList)
	require.Equal(t, "responses", createdOne.ResponsesEndpoint)

	createdTwoBody := llmRequest(t, "POST", basePath, createPayload(titleTwo, secretTwo, modelTwo), 200)
	require.NotContains(t, string(createdTwoBody), secretTwo)
	createdTwo := decodeTestLLMConfig(t, createdTwoBody)
	require.NotEmpty(t, createdTwo.ID)
	require.NotEqual(t, createdOne.ID, createdTwo.ID)
	require.Equal(t, maskedAPIKey, createdTwo.APIKey)

	t.Run("ListConfigs", func(t *testing.T) {
		configs := listTestLLMConfigs(t)
		listedOne := requireTestLLMConfig(t, configs, createdOne.ID)
		listedTwo := requireTestLLMConfig(t, configs, createdTwo.ID)
		require.Equal(t, titleOne, listedOne.Title)
		require.Equal(t, titleTwo, listedTwo.Title)
		require.Equal(t, maskedAPIKey, listedOne.APIKey)
		require.Equal(t, maskedAPIKey, listedTwo.APIKey)
		encoded, err := json.Marshal(configs)
		require.NoError(t, err)
		require.NotContains(t, string(encoded), secretOne)
		require.NotContains(t, string(encoded), secretTwo)
	})

	t.Run("ListModels", func(t *testing.T) {
		body := llmRequest(t, "GET", basePath+"/models", nil, 200)
		var models []testLLMModelList
		require.NoError(t, json.Unmarshal(body, &models))
		listedOne := requireTestLLMModelList(t, models, createdOne.ID)
		listedTwo := requireTestLLMModelList(t, models, createdTwo.ID)
		require.Equal(t, titleOne, listedOne.Name)
		require.Equal(t, titleTwo, listedTwo.Name)
		require.Equal(t, "Custom", listedOne.Provider)
		require.Equal(t, []string{modelOne}, listedOne.Models)
		require.Equal(t, []string{modelTwo}, listedTwo.Models)
	})

	t.Run("ProtectAndSwitchDefault", func(t *testing.T) {
		firstDefault := decodeTestLLMConfig(t, llmRequest(t, "PUT", basePath+"/"+createdOne.ID+"/default", nil, 200))
		require.True(t, firstDefault.IsDefault)

		deleteBody := llmRequest(t, "DELETE", basePath+"/"+createdOne.ID, nil, 500)
		require.Contains(t, string(deleteBody), "Cannot delete the default LLM configuration")

		secondDefault := decodeTestLLMConfig(t, llmRequest(t, "PUT", basePath+"/"+createdTwo.ID+"/default", nil, 200))
		require.True(t, secondDefault.IsDefault)
		configs := listTestLLMConfigs(t)
		require.False(t, requireTestLLMConfig(t, configs, createdOne.ID).IsDefault)
		require.True(t, requireTestLLMConfig(t, configs, createdTwo.ID).IsDefault)
	})

	t.Run("DeleteConfigs", func(t *testing.T) {
		deletedOne := decodeTestLLMConfig(t, llmRequest(t, "DELETE", basePath+"/"+createdOne.ID, nil, 200))
		require.Equal(t, createdOne.ID, deletedOne.ID)
		deletedTwo := decodeTestLLMConfig(t, llmRequest(t, "DELETE", basePath+"/"+createdTwo.ID, nil, 200))
		require.Equal(t, createdTwo.ID, deletedTwo.ID)

		remaining := listTestLLMConfigs(t)
		for _, config := range remaining {
			require.NotEqual(t, createdOne.ID, config.ID)
			require.NotEqual(t, createdTwo.ID, config.ID)
		}
	})
}
