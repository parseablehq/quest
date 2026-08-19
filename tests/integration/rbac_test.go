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
	"strings"
	"sync"
	"testing"

	httpclient "quest/tests/integration/clients/http"
)

// RBAC tests mutate shared server-wide user and role state. They are still
// scheduled as parallel tests, but those mutations must not overlap.
var rbacMu sync.Mutex

func TestSmoke_AllUsersAPI(t *testing.T) {
	// Verifies the user creation, role, password, and deletion flow.
	t.Parallel()
	rbacMu.Lock()
	defer rbacMu.Unlock()

	role := NewGlob.Stream + "allusersrole"
	user := NewGlob.Stream + "allusers"
	CreateRole(t, NewGlob.QueryClient, role, dummyRole)
	AssertRole(t, NewGlob.QueryClient, role, dummyRole)

	CreateUserWithRole(t, NewGlob.PBClient, user, []string{role})
	AssertUserRole(t, NewGlob.QueryClient, user, role, dummyRole)
	RegenPassword(t, NewGlob.QueryClient, user)
	DeleteUser(t, NewGlob.PBClient, user)
	DeleteRole(t, NewGlob.PBClient, role)
}

func TestSmoke_NewUserWithRole(t *testing.T) {
	// Verifies that a new user can be created with a role.
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
