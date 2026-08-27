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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	agentObservabilityModel = "gpt-4o-mini"
	agentObservabilityTool  = "search_orders"
	agentObservabilityUser  = "quest-user"
)

type agentValidationResponse struct {
	Datasets []struct {
		Dataset       string   `json:"dataset"`
		LogSource     string   `json:"logSource"`
		Valid         bool     `json:"valid"`
		MissingFields []string `json:"missingFields"`
		Message       string   `json:"message"`
	} `json:"datasets"`
	Errors []string `json:"errors"`
}

type agentMetric struct {
	Current float64 `json:"current"`
}

type agentOverviewResponse struct {
	Section1 struct {
		InvocationCount agentMetric `json:"invocationCount"`
		ErrorCount      agentMetric `json:"errorCount"`
		TotalTokens     agentMetric `json:"totalTokens"`
		TotalLLMCalls   agentMetric `json:"totalLlmCalls"`
		TotalToolCalls  agentMetric `json:"totalToolCalls"`
	} `json:"section1"`
	Section2 struct {
		ToolUsage []struct {
			ToolName string  `json:"toolName"`
			Calls    float64 `json:"calls"`
		} `json:"toolUsagePercentage"`
		TokenUsage []struct {
			User         string  `json:"user"`
			Model        string  `json:"model"`
			InputTokens  float64 `json:"inputTokens"`
			OutputTokens float64 `json:"outputTokens"`
		} `json:"tokenUsageByUserAndModel"`
	} `json:"section2"`
}

type agentModelsResponse struct {
	Cards struct {
		InputTokens   agentMetric `json:"inputTokens"`
		OutputTokens  agentMetric `json:"outputTokens"`
		MostUsedModel struct {
			Name string `json:"name"`
		} `json:"mostUsedModel"`
	} `json:"cards"`
	Charts struct {
		TotalTokensByModel []struct {
			Name  string  `json:"name"`
			Value float64 `json:"value"`
		} `json:"totalTokensByModel"`
	} `json:"charts"`
}

type agentToolsResponse struct {
	Cards struct {
		TotalToolCalls agentMetric `json:"totalToolCalls"`
		Errors         agentMetric `json:"errors"`
		MostUsedTool   struct {
			Name string `json:"name"`
		} `json:"mostUsedTool"`
	} `json:"cards"`
	Charts struct {
		ToolUsage []struct {
			ToolName string  `json:"toolName"`
			Calls    float64 `json:"calls"`
		} `json:"toolUsage"`
		ToolFailures struct {
			Points []struct {
				TraceID  string `json:"traceId"`
				ToolName string `json:"toolName"`
			} `json:"points"`
		} `json:"toolFailures"`
	} `json:"charts"`
}

type agentListingRecord struct {
	TraceID         string  `json:"traceId"`
	Models          string  `json:"models"`
	Model           string  `json:"model"`
	InputTokens     float64 `json:"inputTokens"`
	OutputTokens    float64 `json:"outputTokens"`
	InvocationError bool    `json:"invocationError"`
	ToolCalls       float64 `json:"toolCalls"`
}

type agentListingResponse struct {
	Total   int                  `json:"total"`
	Limit   int                  `json:"limit"`
	Offset  int                  `json:"offset"`
	Records []agentListingRecord `json:"records"`
}

type agentDetailResponse struct {
	TraceID  string `json:"traceId"`
	Messages []struct {
		Role string `json:"role"`
	} `json:"messages"`
	Spans []struct {
		SpanID        string  `json:"spanId"`
		ParentSpanID  string  `json:"parentSpanId"`
		ServiceName   string  `json:"serviceName"`
		SpanName      string  `json:"spanName"`
		TraceID       string  `json:"traceId"`
		OperationName string  `json:"operationName"`
		ToolName      string  `json:"toolName"`
		ToolCallID    string  `json:"toolCallId"`
		Model         string  `json:"model"`
		InputTokens   float64 `json:"inputTokens"`
		OutputTokens  float64 `json:"outputTokens"`
		HasError      bool    `json:"hasError"`
		Level         float64 `json:"level"`
	} `json:"spans"`
	Events []struct {
		EventName string `json:"eventName"`
	} `json:"events"`
}

func agentObservabilityRequest(t *testing.T, path string, payload any, output any) {
	t.Helper()
	encoded, err := json.Marshal(payload)
	require.NoError(t, err)
	req, err := NewGlob.QueryClient.NewRequestAtPath("POST", path, bytes.NewReader(encoded))
	require.NoError(t, err)
	response, err := NewGlob.QueryClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	body := readAsString(response.Body)
	require.Equalf(t, 200, response.StatusCode, "Server returned http code: %s and response: %s", response.Status, body)
	require.NoError(t, json.Unmarshal([]byte(body), output))
}

func createAgentObservabilityDataset(t *testing.T, dataset string) {
	t.Helper()
	CreateStreamWithHeader(t, NewGlob.QueryClient, dataset, map[string]string{
		"X-P-Telemetry-Type": "traces",
		"X-P-Log-Source":     "otel-traces",
		"X-P-Dataset-Tags":   "agent-observability",
	})
	t.Cleanup(func() {
		DeleteStream(t, NewGlob.PBClient, dataset)
	})
}

func ingestAgentObservabilityFixture(t *testing.T, dataset, traceID string) {
	t.Helper()
	client := NewGlob.QueryClient
	if NewGlob.IngestorUrl.String() != "" {
		client = NewGlob.IngestorClient
	}

	start := time.Now().UTC().Add(-3 * time.Second)
	rootSpanID := "aaaaaaaaaaaaaaaa"
	chatSpanID := "bbbbbbbbbbbbbbbb"
	toolSpanID := "cccccccccccccccc"
	inputMessages := `[{"role":"user","parts":[{"type":"text","content":"Find order 123"}]}]`
	outputMessages := `[{"role":"assistant","parts":[{"type":"text","content":"Order 123 is shipped"}]}]`

	rootSpan := map[string]any{
		"traceId": traceID, "spanId": rootSpanID, "name": "quest-agent-run", "kind": 1,
		"startTimeUnixNano": fmt.Sprint(start.UnixNano()),
		"endTimeUnixNano":   fmt.Sprint(start.Add(2500 * time.Millisecond).UnixNano()),
		"status":            map[string]any{"code": 2, "message": "quest invocation failed"},
		"attributes": []any{
			otlpStringAttribute("gen_ai.operation.name", "invoke_agent"),
			otlpStringAttribute("gen_ai.agent.name", "quest-agent"),
			otlpStringAttribute("gen_ai.agent.id", "quest-agent-1"),
			otlpStringAttribute("gen_ai.conversation.id", "quest-conversation-1"),
			otlpStringAttribute("user.name", agentObservabilityUser),
			otlpStringAttribute("team.name", "quest-team"),
			otlpStringAttribute("error.message", "quest invocation failed"),
		},
	}
	chatSpan := map[string]any{
		"traceId": traceID, "spanId": chatSpanID, "parentSpanId": rootSpanID, "name": "quest-agent-chat", "kind": 3,
		"startTimeUnixNano": fmt.Sprint(start.Add(200 * time.Millisecond).UnixNano()),
		"endTimeUnixNano":   fmt.Sprint(start.Add(1500 * time.Millisecond).UnixNano()),
		"status":            map[string]int{"code": 1},
		"attributes": []any{
			otlpStringAttribute("gen_ai.operation.name", "chat"),
			otlpStringAttribute("gen_ai.provider.name", "openai"),
			otlpStringAttribute("gen_ai.request.model", agentObservabilityModel),
			otlpStringAttribute("gen_ai.response.model", agentObservabilityModel),
			otlpIntAttribute("gen_ai.usage.input_tokens", 20),
			otlpIntAttribute("gen_ai.usage.output_tokens", 10),
			otlpIntAttribute("gen_ai.usage.cached_tokens", 2),
			otlpStringAttribute("gen_ai.input.messages", inputMessages),
			otlpStringAttribute("gen_ai.output.messages", outputMessages),
		},
	}
	toolSpan := map[string]any{
		"traceId": traceID, "spanId": toolSpanID, "parentSpanId": rootSpanID, "name": "quest-search-orders", "kind": 3,
		"startTimeUnixNano": fmt.Sprint(start.Add(1600 * time.Millisecond).UnixNano()),
		"endTimeUnixNano":   fmt.Sprint(start.Add(2200 * time.Millisecond).UnixNano()),
		"status":            map[string]any{"code": 2, "message": "quest tool failure"},
		"attributes": []any{
			otlpStringAttribute("gen_ai.operation.name", "execute_tool"),
			otlpStringAttribute("gen_ai.tool.name", agentObservabilityTool),
			otlpStringAttribute("gen_ai.tool.call.id", "quest-tool-call-1"),
			otlpStringAttribute("gen_ai.tool.call.arguments", `{"order_id":"123"}`),
			otlpStringAttribute("gen_ai.tool.call.result", `{"status":"shipped"}`),
			otlpStringAttribute("error.message", "quest tool failure"),
		},
	}

	payload := map[string]any{"resourceSpans": []any{map[string]any{
		"resource": map[string]any{"attributes": []any{
			otlpStringAttribute("service.name", "quest-agent-service"),
		}},
		"scopeSpans": []any{map[string]any{
			"scope": map[string]string{"name": "quest-agent-observability"},
			"spans": []any{rootSpan, chatSpan, toolSpan},
		}},
	}}}
	encoded, err := json.Marshal(payload)
	require.NoError(t, err)
	req, err := client.NewRequestAtPath("POST", "/v1/traces", bytes.NewReader(encoded))
	require.NoError(t, err)
	req.Header.Set("X-P-Stream", dataset)
	req.Header.Set("X-P-Log-Source", "otel-traces")
	response, err := client.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equalf(t, 200, response.StatusCode, "OTLP agent fixture failed: %s", readAsString(response.Body))
}

func waitForValidAgentDataset(t *testing.T, dataset string) {
	t.Helper()
	var lastBody string
	deadline := time.Now().Add(traceVisibilityTimeout)
	for {
		valid := func() bool {
			infoPath := fmt.Sprintf("api/prism/v1/logstream/%s/info", dataset)
			infoRequest, err := NewGlob.QueryClient.NewRequestAtPath("GET", infoPath, nil)
			if err != nil {
				lastBody = err.Error()
				return false
			}
			infoResponse, err := NewGlob.QueryClient.Do(infoRequest)
			if err != nil {
				lastBody = err.Error()
				return false
			}
			infoBody := readAsString(infoResponse.Body)
			infoResponse.Body.Close()
			if infoResponse.StatusCode != 200 {
				lastBody = fmt.Sprintf("dataset info status=%d body=%s", infoResponse.StatusCode, infoBody)
				return false
			}

			req, err := NewGlob.QueryClient.NewRequestAtPath("GET", "api/prism/v1/agent-observability/validate", nil)
			if err != nil {
				lastBody = err.Error()
				return false
			}
			response, err := NewGlob.QueryClient.Do(req)
			if err != nil {
				lastBody = err.Error()
				return false
			}
			lastBody = readAsString(response.Body)
			response.Body.Close()
			if response.StatusCode != 200 {
				return false
			}
			var result agentValidationResponse
			if json.Unmarshal([]byte(lastBody), &result) != nil {
				return false
			}
			for _, candidate := range result.Datasets {
				if candidate.Dataset == dataset {
					return candidate.LogSource == "otel-traces" && candidate.Valid && len(candidate.MissingFields) == 0
				}
			}
			return false
		}()
		if valid {
			return
		}
		if time.Now().After(deadline) {
			require.FailNowf(t, "agent dataset did not become valid", "dataset=%s last response=%s", dataset, lastBody)
		}
		time.Sleep(5 * time.Second)
	}
}

func waitForAgentListing(t *testing.T, payload map[string]any, traceID string) agentListingResponse {
	t.Helper()
	var result agentListingResponse
	var lastBody string
	deadline := time.Now().Add(traceVisibilityTimeout)
	for {
		found := func() bool {
			encoded, err := json.Marshal(payload)
			if err != nil {
				lastBody = err.Error()
				return false
			}
			req, err := NewGlob.QueryClient.NewRequestAtPath("POST", "api/prism/v1/agent-observability/listing", bytes.NewReader(encoded))
			if err != nil {
				lastBody = err.Error()
				return false
			}
			response, err := NewGlob.QueryClient.Do(req)
			if err != nil {
				lastBody = err.Error()
				return false
			}
			lastBody = readAsString(response.Body)
			response.Body.Close()
			if response.StatusCode != 200 || json.Unmarshal([]byte(lastBody), &result) != nil {
				return false
			}
			for _, record := range result.Records {
				if record.TraceID == traceID {
					return true
				}
			}
			return false
		}()
		if found {
			return result
		}
		if time.Now().After(deadline) {
			require.FailNowf(t, "agent trace did not become queryable", "trace=%s last response=%s", traceID, lastBody)
		}
		time.Sleep(5 * time.Second)
	}
}

func TestEnterpriseAgentObservability(t *testing.T) {
	// Verifies tagged GenAI traces across all Agent Observability feature APIs.
	if NewGlob.Edition != "enterprise" {
		t.Skip("Agent Observability is only available in Enterprise")
	}
	t.Parallel()

	dataset := NewGlob.Stream + "agentobservability"
	traceID := fmt.Sprintf("%032x", time.Now().UnixNano())
	createAgentObservabilityDataset(t, dataset)
	ingestAgentObservabilityFixture(t, dataset, traceID)
	waitForValidAgentDataset(t, dataset)

	window := map[string]any{
		"dataset":   dataset,
		"startTime": time.Now().UTC().Add(-15 * time.Minute).Format(time.RFC3339Nano),
		"endTime":   time.Now().UTC().Add(5 * time.Minute).Format(time.RFC3339Nano),
	}
	listingPayload := map[string]any{
		"dataset":   window["dataset"],
		"startTime": window["startTime"],
		"endTime":   window["endTime"],
		"limit":     10,
		"offset":    0,
		"provider":  "openai",
	}
	listing := waitForAgentListing(t, listingPayload, traceID)
	require.GreaterOrEqual(t, listing.Total, 1)
	require.Equal(t, 10, listing.Limit)
	require.Equal(t, 0, listing.Offset)
	var listedTrace *agentListingRecord
	for index := range listing.Records {
		if listing.Records[index].TraceID == traceID {
			listedTrace = &listing.Records[index]
			break
		}
	}
	require.NotNil(t, listedTrace)
	require.True(t, strings.Contains(listedTrace.Models, agentObservabilityModel) || listedTrace.Model == agentObservabilityModel)
	require.GreaterOrEqual(t, listedTrace.InputTokens, float64(20))
	require.GreaterOrEqual(t, listedTrace.OutputTokens, float64(10))
	require.True(t, listedTrace.InvocationError)
	require.GreaterOrEqual(t, listedTrace.ToolCalls, float64(1))

	t.Run("FeatureResponses", func(t *testing.T) {
		t.Run("Overview", func(t *testing.T) {
			t.Parallel()
			payload := map[string]any{
				"dataset": window["dataset"], "startTime": window["startTime"], "endTime": window["endTime"],
				"provider": "openai", "numBins": 10,
			}
			var result agentOverviewResponse
			agentObservabilityRequest(t, "api/prism/v1/agent-observability/overview", payload, &result)
			require.GreaterOrEqual(t, result.Section1.InvocationCount.Current, float64(1))
			require.GreaterOrEqual(t, result.Section1.ErrorCount.Current, float64(1))
			require.GreaterOrEqual(t, result.Section1.TotalTokens.Current, float64(30))
			require.GreaterOrEqual(t, result.Section1.TotalLLMCalls.Current, float64(1))
			require.GreaterOrEqual(t, result.Section1.TotalToolCalls.Current, float64(1))
			require.Contains(t, result.Section2.ToolUsage, struct {
				ToolName string  `json:"toolName"`
				Calls    float64 `json:"calls"`
			}{ToolName: agentObservabilityTool, Calls: 1})
			require.NotEmpty(t, result.Section2.TokenUsage)
			require.Equal(t, agentObservabilityUser, result.Section2.TokenUsage[0].User)
			require.Equal(t, agentObservabilityModel, result.Section2.TokenUsage[0].Model)
		})

		t.Run("Models", func(t *testing.T) {
			t.Parallel()
			payload := map[string]any{
				"dataset": window["dataset"], "startTime": window["startTime"], "endTime": window["endTime"],
				"provider": "openai",
			}
			var result agentModelsResponse
			agentObservabilityRequest(t, "api/prism/v1/agent-observability/models", payload, &result)
			require.GreaterOrEqual(t, result.Cards.InputTokens.Current, float64(20))
			require.GreaterOrEqual(t, result.Cards.OutputTokens.Current, float64(10))
			require.Equal(t, agentObservabilityModel, result.Cards.MostUsedModel.Name)
			var modelFound bool
			for _, model := range result.Charts.TotalTokensByModel {
				if model.Name == agentObservabilityModel && model.Value >= 30 {
					modelFound = true
				}
			}
			require.True(t, modelFound, "model totals did not include the ingested model")
		})

		t.Run("Tools", func(t *testing.T) {
			t.Parallel()
			var result agentToolsResponse
			agentObservabilityRequest(t, "api/prism/v1/agent-observability/tools", window, &result)
			require.GreaterOrEqual(t, result.Cards.TotalToolCalls.Current, float64(1))
			require.GreaterOrEqual(t, result.Cards.Errors.Current, float64(1))
			require.Equal(t, agentObservabilityTool, result.Cards.MostUsedTool.Name)
			var toolFound bool
			for _, tool := range result.Charts.ToolUsage {
				if tool.ToolName == agentObservabilityTool && tool.Calls >= 1 {
					toolFound = true
				}
			}
			require.True(t, toolFound, "tool usage did not include the ingested tool")
			var failureFound bool
			for _, failure := range result.Charts.ToolFailures.Points {
				if failure.TraceID == traceID && failure.ToolName == agentObservabilityTool {
					failureFound = true
				}
			}
			require.True(t, failureFound, "tool failures did not include the ingested failed tool span")
		})
	})

	t.Run("Detail", func(t *testing.T) {
		payload := map[string]any{
			"dataset": window["dataset"], "traceId": traceID,
			"startTime": window["startTime"], "endTime": window["endTime"], "provider": "openai",
		}
		var result agentDetailResponse
		agentObservabilityRequest(t, "api/prism/v1/agent-observability/detail", payload, &result)
		require.Equal(t, traceID, result.TraceID)
		require.Len(t, result.Spans, 3)
		operations := make(map[string]bool)
		for _, span := range result.Spans {
			require.Equal(t, traceID, span.TraceID)
			require.Equal(t, "quest-agent-service", span.ServiceName)
			operations[span.OperationName] = true
			if span.OperationName == "chat" {
				require.Equal(t, agentObservabilityModel, span.Model)
				require.GreaterOrEqual(t, span.InputTokens, float64(20))
				require.GreaterOrEqual(t, span.OutputTokens, float64(10))
			}
			if span.OperationName == "execute_tool" {
				require.Equal(t, agentObservabilityTool, span.ToolName)
				require.Equal(t, "quest-tool-call-1", span.ToolCallID)
				require.True(t, span.HasError)
			}
		}
		require.True(t, operations["invoke_agent"])
		require.True(t, operations["chat"])
		require.True(t, operations["execute_tool"])
		require.NotEmpty(t, result.Messages)
		roles := make(map[string]bool)
		for _, message := range result.Messages {
			roles[message.Role] = true
		}
		require.True(t, roles["user"])
		require.True(t, roles["assistant"])
		eventNames := make(map[string]bool)
		for _, event := range result.Events {
			eventNames[event.EventName] = true
		}
		require.True(t, eventNames["gen_ai.tool.call"])
		require.True(t, eventNames["gen_ai.tool.output"])
	})
}
