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
	"bytes"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

type testUserGroupUser struct {
	UserID   string `json:"userid"`
	Username string `json:"username"`
	Method   string `json:"method"`
}

type testUserGroupResponse struct {
	Name  string              `json:"name"`
	Roles []string            `json:"roles"`
	Users []testUserGroupUser `json:"users"`
}

type testUserGroupUserIdentifier struct {
	UserID string `json:"userid"`
	Method string `json:"method"`
}

type testUserGroupMembers struct {
	Roles []string                      `json:"roles"`
	Users []testUserGroupUserIdentifier `json:"users"`
}

type testUserGroupRoleResponse struct {
	Roles      map[string]json.RawMessage            `json:"roles"`
	GroupRoles map[string]map[string]json.RawMessage `json:"group_roles"`
}

func userGroupRequest(t *testing.T, method, path string, payload any, expectedStatus int) *testUserGroupResponse {
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
	responseBody := readAsString(response.Body)
	require.Equalf(t, expectedStatus, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, responseBody)
	if expectedStatus != 200 {
		return nil
	}

	var group testUserGroupResponse
	err = json.Unmarshal([]byte(responseBody), &group)
	require.NoError(t, err)
	return &group
}

func listTestUserGroups(t *testing.T) []testUserGroupResponse {
	t.Helper()
	req, err := NewGlob.QueryClient.NewRequestAtPath("GET", "api/prism/v1/usergroup", nil)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	groups, err := readJsonBody[[]testUserGroupResponse](response.Body)
	require.NoError(t, err)
	return groups
}

func requireTestUserGroupListed(t *testing.T, groups []testUserGroupResponse, name string) testUserGroupResponse {
	t.Helper()
	for _, group := range groups {
		if group.Name == name {
			return group
		}
	}
	t.Fatalf("user group %q was not returned", name)
	return testUserGroupResponse{}
}

func requireTestUserGroupMembers(t *testing.T, group testUserGroupResponse, role string, users ...string) {
	t.Helper()
	require.ElementsMatch(t, []string{role}, group.Roles)
	require.Len(t, group.Users, len(users))
	groupUsers := make(map[string]testUserGroupUser, len(group.Users))
	for _, user := range group.Users {
		groupUsers[user.UserID] = user
	}
	for _, expectedUser := range users {
		user, exists := groupUsers[expectedUser]
		require.Truef(t, exists, "user %q was not returned in group %q", expectedUser, group.Name)
		require.Equal(t, expectedUser, user.Username)
		require.Equal(t, "native", user.Method)
	}
}

func getTestUserGroupRoles(t *testing.T, user string) testUserGroupRoleResponse {
	t.Helper()
	req, err := NewGlob.QueryClient.NewRequest("GET", "user/"+url.PathEscape(user)+"/role", nil)
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s", response.Status)
	result, err := readJsonBody[testUserGroupRoleResponse](response.Body)
	require.NoError(t, err)
	return result
}

func cleanupTestUserGroup(group string, members testUserGroupMembers) {
	encoded, err := json.Marshal(members)
	if err == nil {
		req, requestErr := NewGlob.QueryClient.NewRequestAtPath(
			"PATCH",
			"api/prism/v1/usergroup/"+url.PathEscape(group)+"/remove",
			bytes.NewReader(encoded),
		)
		if requestErr == nil {
			if response, responseErr := NewGlob.QueryClient.Do(req); responseErr == nil {
				response.Body.Close()
			}
		}
	}

	req, err := NewGlob.QueryClient.NewRequestAtPath(
		"DELETE",
		"api/prism/v1/usergroup/"+url.PathEscape(group),
		nil,
	)
	if err == nil {
		if response, responseErr := NewGlob.QueryClient.Do(req); responseErr == nil {
			response.Body.Close()
		}
	}
}

func TestEnterpriseUserGroupLifecycle(t *testing.T) {
	// Verifies Enterprise user-group create, list, get, membership, and delete APIs.
	if NewGlob.Edition != "enterprise" {
		t.Skip("user groups are only available in Enterprise")
	}
	t.Parallel()
	rbacMu.Lock()
	t.Cleanup(rbacMu.Unlock)

	groupName := NewGlob.Stream + "usergroup"
	roleName := NewGlob.Stream + "usergrouprole"
	baseRoleName := NewGlob.Stream + "usergroupbaserole"
	usernameOne := NewGlob.Stream + "usergroupuserone"
	usernameTwo := NewGlob.Stream + "usergroupusertwo"
	memberPayload := testUserGroupMembers{
		Roles: []string{roleName},
		Users: []testUserGroupUserIdentifier{
			{UserID: usernameOne, Method: "native"},
			{UserID: usernameTwo, Method: "native"},
		},
	}

	CreateRole(t, NewGlob.QueryClient, roleName, dummyRole)
	t.Cleanup(func() {
		DeleteRole(t, NewGlob.PBClient, roleName)
	})
	CreateRole(t, NewGlob.QueryClient, baseRoleName, dummyRole)
	t.Cleanup(func() {
		DeleteRole(t, NewGlob.PBClient, baseRoleName)
	})
	CreateUserWithRole(t, NewGlob.PBClient, usernameOne, []string{baseRoleName})
	t.Cleanup(func() {
		DeleteUser(t, NewGlob.PBClient, usernameOne)
	})
	CreateUserWithRole(t, NewGlob.PBClient, usernameTwo, []string{baseRoleName})
	t.Cleanup(func() {
		DeleteUser(t, NewGlob.PBClient, usernameTwo)
	})

	created := userGroupRequest(t, "POST", "api/prism/v1/usergroup", map[string]any{
		"name":  groupName,
		"roles": []string{},
		"users": []testUserGroupUserIdentifier{},
	}, 200)
	require.Equal(t, groupName, created.Name)
	require.Empty(t, created.Roles)
	require.Empty(t, created.Users)

	groupDeleted := false
	t.Cleanup(func() {
		if !groupDeleted {
			cleanupTestUserGroup(groupName, memberPayload)
		}
	})

	t.Run("ListGroup", func(t *testing.T) {
		listed := requireTestUserGroupListed(t, listTestUserGroups(t), groupName)
		require.Empty(t, listed.Roles)
		require.Empty(t, listed.Users)
	})

	t.Run("GetGroup", func(t *testing.T) {
		group := userGroupRequest(t, "GET", "api/prism/v1/usergroup/"+url.PathEscape(groupName), nil, 200)
		require.Equal(t, groupName, group.Name)
		require.Empty(t, group.Roles)
		require.Empty(t, group.Users)
	})

	t.Run("AddUserAndRole", func(t *testing.T) {
		group := userGroupRequest(
			t,
			"PATCH",
			"api/prism/v1/usergroup/"+url.PathEscape(groupName)+"/add",
			memberPayload,
			200,
		)
		require.Equal(t, groupName, group.Name)
		requireTestUserGroupMembers(t, *group, roleName, usernameOne, usernameTwo)

		persisted := userGroupRequest(t, "GET", "api/prism/v1/usergroup/"+url.PathEscape(groupName), nil, 200)
		requireTestUserGroupMembers(t, *persisted, roleName, usernameOne, usernameTwo)

		for _, username := range []string{usernameOne, usernameTwo} {
			inheritedRoles := getTestUserGroupRoles(t, username)
			require.Contains(t, inheritedRoles.Roles, baseRoleName)
			require.NotContains(t, inheritedRoles.Roles, roleName)
			require.Contains(t, inheritedRoles.GroupRoles, groupName)
			require.Contains(t, inheritedRoles.GroupRoles[groupName], roleName)
		}
	})

	t.Run("RejectNonEmptyGroupDeletion", func(t *testing.T) {
		userGroupRequest(t, "DELETE", "api/prism/v1/usergroup/"+url.PathEscape(groupName), nil, 400)
		persisted := userGroupRequest(t, "GET", "api/prism/v1/usergroup/"+url.PathEscape(groupName), nil, 200)
		requireTestUserGroupMembers(t, *persisted, roleName, usernameOne, usernameTwo)
	})

	t.Run("RemoveUserAndRole", func(t *testing.T) {
		group := userGroupRequest(
			t,
			"PATCH",
			"api/prism/v1/usergroup/"+url.PathEscape(groupName)+"/remove",
			memberPayload,
			200,
		)
		require.Equal(t, groupName, group.Name)
		require.Empty(t, group.Roles)
		require.Empty(t, group.Users)

		persisted := userGroupRequest(t, "GET", "api/prism/v1/usergroup/"+url.PathEscape(groupName), nil, 200)
		require.Empty(t, persisted.Roles)
		require.Empty(t, persisted.Users)

		for _, username := range []string{usernameOne, usernameTwo} {
			inheritedRoles := getTestUserGroupRoles(t, username)
			require.Contains(t, inheritedRoles.Roles, baseRoleName)
			require.NotContains(t, inheritedRoles.Roles, roleName)
			require.NotContains(t, inheritedRoles.GroupRoles, groupName)
		}
	})

	t.Run("DeleteGroup", func(t *testing.T) {
		deleted := userGroupRequest(t, "DELETE", "api/prism/v1/usergroup/"+url.PathEscape(groupName), nil, 200)
		groupDeleted = true
		require.Equal(t, groupName, deleted.Name)
		require.Empty(t, deleted.Roles)
		require.Empty(t, deleted.Users)

		userGroupRequest(t, "GET", "api/prism/v1/usergroup/"+url.PathEscape(groupName), nil, 400)
		for _, group := range listTestUserGroups(t) {
			require.NotEqual(t, groupName, group.Name)
		}
	})
}
