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
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	httpclient "quest/tests/integration/clients/http"
)

// RBAC tests mutate shared server-wide user and role state. They are still
// scheduled as parallel tests, but those mutations must not overlap.
var rbacMu sync.Mutex

type prismUserResponse struct {
	ID       string                     `json:"id"`
	Username string                     `json:"username"`
	Email    *string                    `json:"email"`
	Roles    map[string]json.RawMessage `json:"roles"`
}

func getPrismUser(t *testing.T, user string) prismUserResponse {
	t.Helper()
	req, err := NewGlob.QueryClient.NewRequest("GET", "users/"+user, nil)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	result, err := readJsonBody[prismUserResponse](response.Body)
	require.NoError(t, err)
	return result
}

func getDefaultRole(t *testing.T) *string {
	t.Helper()
	req, err := NewGlob.QueryClient.NewRequest("GET", "role/default", nil)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	role, err := readJsonBody[*string](response.Body)
	require.NoError(t, err)
	return role
}

func setDefaultRole(t *testing.T, role string) {
	t.Helper()
	payload, err := json.Marshal(role)
	require.NoError(t, err)
	req, err := NewGlob.QueryClient.NewRequest("PUT", "role/default", bytes.NewReader(payload))
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
}

func TestSmoke_AllUsersAPI(t *testing.T) {
	// Verifies the UI user APIs together with creation, password, and deletion.
	t.Skip("temporarily disabled due to a Parseable RBAC server issue")
	t.Parallel()
	rbacMu.Lock()
	t.Cleanup(rbacMu.Unlock)

	role := NewGlob.Stream + "allusersrole"
	addedRole := NewGlob.Stream + "allusersaddedrole"
	user := NewGlob.Stream + "allusers"
	CreateRole(t, NewGlob.QueryClient, role, dummyRole)
	t.Cleanup(func() {
		DeleteRole(t, NewGlob.PBClient, role)
	})
	AssertRole(t, NewGlob.QueryClient, role, dummyRole)
	CreateRole(t, NewGlob.QueryClient, addedRole, dummyRole)
	t.Cleanup(func() {
		DeleteRole(t, NewGlob.PBClient, addedRole)
	})
	AssertRole(t, NewGlob.QueryClient, addedRole, dummyRole)

	CreateUserWithRole(t, NewGlob.PBClient, user, []string{role})
	t.Cleanup(func() {
		DeleteUser(t, NewGlob.PBClient, user)
	})
	AssertUserRole(t, NewGlob.QueryClient, user, role, dummyRole)

	t.Run("ListUsers", func(t *testing.T) {
		req, err := NewGlob.QueryClient.NewRequest("GET", "users", nil)
		require.NoError(t, err)
		response, err := NewGlob.QueryClient.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
		users, err := readJsonBody[[]prismUserResponse](response.Body)
		require.NoError(t, err)
		var listedUser *prismUserResponse
		for index := range users {
			if users[index].ID == user {
				listedUser = &users[index]
				break
			}
		}
		require.NotNil(t, listedUser)
		require.Equal(t, user, listedUser.Username)
		require.Contains(t, listedUser.Roles, role)
	})

	t.Run("GetUser", func(t *testing.T) {
		result := getPrismUser(t, user)
		require.Equal(t, user, result.ID)
		require.Equal(t, user, result.Username)
		require.Contains(t, result.Roles, role)
	})

	t.Run("UpdateUserEmail", func(t *testing.T) {
		email := user + "@example.com"
		payload, err := json.Marshal(map[string]string{"email": email})
		require.NoError(t, err)
		req, err := NewGlob.QueryClient.NewRequest("PATCH", "user/"+user, bytes.NewReader(payload))
		require.NoError(t, err)
		response, err := NewGlob.QueryClient.Do(req)
		require.NoError(t, err)
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
		require.NoError(t, response.Body.Close())

		updated := getPrismUser(t, user)
		require.NotNil(t, updated.Email)
		require.Equal(t, email, *updated.Email)
	})

	t.Run("AddUserRole", func(t *testing.T) {
		payload, err := json.Marshal([]string{addedRole})
		require.NoError(t, err)
		req, err := NewGlob.QueryClient.NewRequest("PATCH", "user/"+user+"/role/add", bytes.NewReader(payload))
		require.NoError(t, err)
		response, err := NewGlob.QueryClient.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
		message, err := readJsonBody[string](response.Body)
		require.NoError(t, err)
		require.Equal(t, "Roles updated successfully for "+user, message)

		updated := getPrismUser(t, user)
		require.Contains(t, updated.Roles, role)
		require.Contains(t, updated.Roles, addedRole)
	})

	t.Run("RemoveUserRole", func(t *testing.T) {
		payload, err := json.Marshal([]string{addedRole})
		require.NoError(t, err)
		req, err := NewGlob.QueryClient.NewRequest("PATCH", "user/"+user+"/role/remove", bytes.NewReader(payload))
		require.NoError(t, err)
		response, err := NewGlob.QueryClient.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
		message, err := readJsonBody[string](response.Body)
		require.NoError(t, err)
		require.Equal(t, "Roles updated successfully for "+user, message)

		updated := getPrismUser(t, user)
		require.Contains(t, updated.Roles, role)
		require.NotContains(t, updated.Roles, addedRole)
	})

	RegenPassword(t, NewGlob.QueryClient, user)
}

func TestSmokeRoleUIEndpoints(t *testing.T) {
	// Verifies role listing and default-role APIs without leaking global state.
	t.Skip("temporarily disabled due to a Parseable RBAC server issue")
	t.Parallel()
	rbacMu.Lock()
	t.Cleanup(rbacMu.Unlock)

	role := NewGlob.Stream + "defaultrole"
	CreateRole(t, NewGlob.QueryClient, role, dummyRole)
	t.Cleanup(func() {
		DeleteRole(t, NewGlob.PBClient, role)
	})

	t.Run("ListRoles", func(t *testing.T) {
		req, err := NewGlob.QueryClient.NewRequest("GET", "roles", nil)
		require.NoError(t, err)
		response, err := NewGlob.QueryClient.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
		roles, err := readJsonBody[map[string]json.RawMessage](response.Body)
		require.NoError(t, err)
		require.Contains(t, roles, role)
	})

	originalDefault := getDefaultRole(t)
	t.Run("GetDefaultRole", func(t *testing.T) {
		currentDefault := getDefaultRole(t)
		require.Equal(t, originalDefault, currentDefault)
	})

	t.Run("SetDefaultRole", func(t *testing.T) {
		if originalDefault == nil {
			t.Skip("Parseable has no default role and the API cannot clear a default role after this test")
		}

		setDefaultRole(t, role)
		t.Cleanup(func() {
			setDefaultRole(t, *originalDefault)
		})
		updatedDefault := getDefaultRole(t)
		require.NotNil(t, updatedDefault)
		require.Equal(t, role, *updatedDefault)
	})
}

func TestSmoke_NewUserWithRole(t *testing.T) {
	// Verifies that a new user can be created with a role.
	t.Skip("temporarily disabled due to a Parseable RBAC server issue")
	t.Parallel()
	rbacMu.Lock()
	defer rbacMu.Unlock()

	role := NewGlob.Stream + "newuserrole"
	user := NewGlob.Stream + "newuser"

	CreateRole(t, NewGlob.QueryClient, role, dummyRole)
	AssertRole(t, NewGlob.QueryClient, role, dummyRole)
	CreateUserWithRole(t, NewGlob.PBClient, user, []string{role})
	AssertUserRole(t, NewGlob.QueryClient, user, role, dummyRole)
	DeleteUser(t, NewGlob.PBClient, user)
	DeleteRole(t, NewGlob.PBClient, role)
}

func TestSmokeRbacBasic(t *testing.T) {
	// Verifies that a user's role controls basic API access.
	t.Skip("temporarily disabled due to a Parseable RBAC server issue")
	t.Parallel()
	rbacMu.Lock()
	defer rbacMu.Unlock()

	stream := NewGlob.Stream + "rbacbasic"
	role := NewGlob.Stream + "rbacbasicrole"
	user := NewGlob.Stream + "rbacbasicuser"
	CreateStream(t, NewGlob.PBClient, stream)
	CreateRole(t, NewGlob.QueryClient, role, dummyRole)
	AssertRole(t, NewGlob.QueryClient, role, dummyRole)
	CreateUserWithRole(t, NewGlob.PBClient, user, []string{role})
	userClient := NewGlob.QueryClient
	userClient.Username = user
	userClient.Password = RegenPassword(t, NewGlob.QueryClient, user)
	checkAPIAccess(t, userClient, NewGlob.QueryClient, stream, "editor")
	DeleteUser(t, NewGlob.PBClient, user)
	DeleteRole(t, NewGlob.PBClient, role)
}

func TestSmokeRoles(t *testing.T) {
	// Verifies API access for ingestor, reader, writer, and editor roles.
	t.Skip("temporarily disabled due to a Parseable RBAC server issue")
	t.Parallel()
	rbacMu.Lock()
	defer rbacMu.Unlock()

	stream := NewGlob.Stream + "roles"
	editorDeleteStream := NewGlob.Stream + "roleseditordelete"
	CreateStream(t, NewGlob.PBClient, stream)
	CreateStream(t, NewGlob.PBClient, editorDeleteStream)
	cases := []struct {
		roleName string
		body     string
	}{
		{
			roleName: NewGlob.Stream + "ingestor",
			body:     Roleingestor(stream),
		},
		{
			roleName: NewGlob.Stream + "reader",
			body:     RoleReader(stream),
		},
		{
			roleName: NewGlob.Stream + "writer",
			body:     RoleWriter(stream),
		},
		{
			roleName: NewGlob.Stream + "editor",
			body:     RoleEditor,
		},
	}

	for _, tc := range cases {
		t.Run(tc.roleName, func(t *testing.T) {
			CreateRole(t, NewGlob.QueryClient, tc.roleName, tc.body)
			AssertRole(t, NewGlob.QueryClient, tc.roleName, tc.body)
			username := tc.roleName + "_user"
			password := CreateUserWithRole(t, NewGlob.PBClient, username, []string{tc.roleName})
			var ingestClient httpclient.HTTPClient
			queryClient := NewGlob.QueryClient
			queryClient.Username = username
			queryClient.Password = password
			if NewGlob.IngestorUrl.String() != "" {
				ingestClient = NewGlob.IngestorClient
				ingestClient.Username = username
				ingestClient.Password = password
			} else {
				ingestClient = NewGlob.QueryClient
				ingestClient.Username = username
				ingestClient.Password = password
			}

			roleKind := strings.TrimPrefix(tc.roleName, NewGlob.Stream)
			accessStream := stream
			if roleKind == "editor" {
				accessStream = editorDeleteStream
			}
			checkAPIAccess(t, queryClient, ingestClient, accessStream, roleKind)
			DeleteUser(t, NewGlob.PBClient, username)
			DeleteRole(t, NewGlob.PBClient, tc.roleName)
		})
	}
}
