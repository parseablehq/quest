// Copyright (c) 2023 Cloudnatively Services Pvt Ltd
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package httpclient

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewRequestPreservesQueryParameters(t *testing.T) {
	baseURL, err := url.Parse("http://parseable:8000")
	require.NoError(t, err)
	client := DefaultClient(*baseURL, "admin", "admin")

	req, err := client.NewRequest("GET", "/alerts?metric_name=quest%20metric&tags=quest-test", nil)
	require.NoError(t, err)
	require.Equal(t, "/api/v1/alerts", req.URL.Path)
	require.Equal(t, "quest metric", req.URL.Query().Get("metric_name"))
	require.Equal(t, "quest-test", req.URL.Query().Get("tags"))
}

func TestNewRequestAtPathPreservesQueryParameters(t *testing.T) {
	baseURL, err := url.Parse("http://parseable:8000")
	require.NoError(t, err)
	client := DefaultClient(*baseURL, "admin", "admin")

	req, err := client.NewRequestAtPath("GET", "api/prism/v1/datasets?limit=5", nil)
	require.NoError(t, err)
	require.Equal(t, "/api/prism/v1/datasets", req.URL.Path)
	require.Equal(t, "5", req.URL.Query().Get("limit"))
}
