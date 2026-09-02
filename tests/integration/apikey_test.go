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

type testAPIKeyResponse struct {
	KeyID      string   `json:"keyId"`
	APIKey     string   `json:"apiKey"`
	KeyName    string   `json:"keyName"`
	Roles      []string `json:"roles"`
	CreatedBy  string   `json:"createdBy"`
	CreatedAt  string   `json:"createdAt"`
	ModifiedAt string   `json:"modifiedAt"`
}

type testAPIKeyValidationResponse struct {
	Valid bool `json:"valid"`
}

type testAPIKeyDeleteResponse struct {
	KeyID   string `json:"keyId"`
	KeyName string `json:"keyName"`
	Message string `json:"message"`
}

func deleteTestAPIKey(t *testing.T, keyID, keyName string) testAPIKeyDeleteResponse {
	t.Helper()
	payload, err := json.Marshal(map[string]string{
		"keyName": keyName,
		"keyType": "",
	})
	require.NoError(t, err)
	req, err := NewGlob.QueryClient.NewRequestAtPath(
		"DELETE",
		"api/prism/v1/apikeys/"+keyID,
		bytes.NewReader(payload),
	)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	result, err := readJsonBody[testAPIKeyDeleteResponse](response.Body)
	require.NoError(t, err)
	return result
}

func validateTestAPIKey(t *testing.T, apiKey string) bool {
	t.Helper()
	payload, err := json.Marshal(map[string]string{"apiKey": apiKey})
	require.NoError(t, err)
	req, err := NewGlob.QueryClient.NewRequestAtPath(
		"POST",
		"api/prism/v1/apikeys/validate",
		bytes.NewReader(payload),
	)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	result, err := readJsonBody[testAPIKeyValidationResponse](response.Body)
	require.NoError(t, err)
	return result.Valid
}

func TestSmokeAPIKeyLifecycle(t *testing.T) {
	// Verifies create, list, get, validate, and delete for a self-hosted API key.
	t.Parallel()
	rbacMu.Lock()
	t.Cleanup(rbacMu.Unlock)

	role := NewGlob.Stream + "apikeyrole"
	keyName := NewGlob.Stream + "apikey"
	CreateRole(t, NewGlob.QueryClient, role, dummyRole)
	t.Cleanup(func() {
		DeleteRole(t, NewGlob.PBClient, role)
	})

	payload, err := json.Marshal(map[string]any{
		"keyName": keyName,
		"roles":   []string{role},
	})
	require.NoError(t, err)
	req, err := NewGlob.QueryClient.NewRequestAtPath(
		"POST",
		"api/prism/v1/apikeys",
		bytes.NewReader(payload),
	)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	created, err := readJsonBody[testAPIKeyResponse](response.Body)
	require.NoError(t, err)
	require.NotEmpty(t, created.KeyID)
	require.NotEmpty(t, created.APIKey)
	require.Equal(t, keyName, created.KeyName)
	require.ElementsMatch(t, []string{role}, created.Roles)
	require.NotEmpty(t, created.CreatedBy)
	require.NotEmpty(t, created.CreatedAt)
	require.NotEmpty(t, created.ModifiedAt)

	keyDeleted := false
	t.Cleanup(func() {
		if !keyDeleted {
			deleteTestAPIKey(t, created.KeyID, keyName)
		}
	})

	t.Run("ListAPIKeys", func(t *testing.T) {
		req, err := NewGlob.QueryClient.NewRequestAtPath("GET", "api/prism/v1/apikeys", nil)
		require.NoError(t, err)
		response, err := NewGlob.QueryClient.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
		keys, err := readJsonBody[[]testAPIKeyResponse](response.Body)
		require.NoError(t, err)

		var listed *testAPIKeyResponse
		for index := range keys {
			if keys[index].KeyID == created.KeyID {
				listed = &keys[index]
				break
			}
		}
		require.NotNil(t, listed)
		require.Equal(t, keyName, listed.KeyName)
		require.ElementsMatch(t, []string{role}, listed.Roles)
		require.Equal(t, "****"+created.APIKey[len(created.APIKey)-4:], listed.APIKey)
	})

	t.Run("GetAPIKey", func(t *testing.T) {
		req, err := NewGlob.QueryClient.NewRequestAtPath("GET", "api/prism/v1/apikeys/"+created.KeyID, nil)
		require.NoError(t, err)
		response, err := NewGlob.QueryClient.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
		key, err := readJsonBody[testAPIKeyResponse](response.Body)
		require.NoError(t, err)
		require.Equal(t, created.KeyID, key.KeyID)
		require.Equal(t, created.APIKey, key.APIKey)
		require.Equal(t, keyName, key.KeyName)
		require.ElementsMatch(t, []string{role}, key.Roles)
	})

	t.Run("ValidateAPIKey", func(t *testing.T) {
		require.True(t, validateTestAPIKey(t, created.APIKey))
	})

	t.Run("DeleteAPIKey", func(t *testing.T) {
		deleted := deleteTestAPIKey(t, created.KeyID, keyName)
		keyDeleted = true
		require.Equal(t, created.KeyID, deleted.KeyID)
		require.Equal(t, keyName, deleted.KeyName)
		require.Equal(t, "API key deleted successfully", deleted.Message)
	})

	t.Run("ValidateDeletedAPIKey", func(t *testing.T) {
		require.False(t, validateTestAPIKey(t, created.APIKey))
	})
}
