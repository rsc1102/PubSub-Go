package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"pubsub-go/internal/services"

	"github.com/gin-gonic/gin"
)

func TestCreateTopicEndpoint(t *testing.T) {
	router := newTestRouter()

	resp := performJSONRequest(t, router, http.MethodPost, "/topics", map[string]string{
		"topic": "orders",
	})

	if resp.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d", http.StatusCreated, resp.Code)
	}
}

func TestCreateTopicEndpointRejectsWhitespaceOnlyName(t *testing.T) {
	router := newTestRouter()

	resp := performJSONRequest(t, router, http.MethodPost, "/topics", map[string]string{
		"topic": "   ",
	})

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, resp.Code)
	}
}

func TestGetTopicsEndpoint(t *testing.T) {
	router := newTestRouter()
	ps = services.NewPubSub()
	mustCreateTopicHTTP(t, "orders")
	mustCreateTopicHTTP(t, "payments")

	resp := performJSONRequest(t, router, http.MethodGet, "/topics", nil)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, resp.Code)
	}

	var body struct {
		Topics []string `json:"topics"`
	}
	decodeResponse(t, resp, &body)
	if len(body.Topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(body.Topics))
	}
}

func TestCreateSubscriptionEndpointRequiresTopic(t *testing.T) {
	router := newTestRouter()

	resp := performJSONRequest(t, router, http.MethodPost, "/subscriptions", map[string]string{
		"topic":        "missing",
		"subscription": "alpha",
	})

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, resp.Code)
	}
}

func TestPublishAndConsumeEndpoints(t *testing.T) {
	router := newTestRouter()
	ps = services.NewPubSub()
	mustCreateTopicHTTP(t, "orders")
	mustCreateSubscriptionHTTP(t, "orders", "alpha")

	publishResp := performJSONRequest(t, router, http.MethodPost, "/publish", map[string]string{
		"topic":   "orders",
		"content": "created",
	})
	if publishResp.Code != http.StatusCreated {
		t.Fatalf("expected publish status %d, got %d", http.StatusCreated, publishResp.Code)
	}

	consumeResp := performJSONRequest(t, router, http.MethodPost, "/consume", map[string]string{
		"topic":        "orders",
		"subscription": "alpha",
	})
	if consumeResp.Code != http.StatusOK {
		t.Fatalf("expected consume status %d, got %d", http.StatusOK, consumeResp.Code)
	}

	var body struct {
		Message string `json:"message"`
	}
	decodeResponse(t, consumeResp, &body)
	if body.Message != "created" {
		t.Fatalf("expected consumed message %q, got %q", "created", body.Message)
	}
}

func TestPublishEndpointRequiresExistingTopic(t *testing.T) {
	router := newTestRouter()

	resp := performJSONRequest(t, router, http.MethodPost, "/publish", map[string]string{
		"topic":   "missing",
		"content": "created",
	})

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, resp.Code)
	}
}

func TestCreateSubscriptionEndpointRejectsDuplicateName(t *testing.T) {
	router := newTestRouter()
	ps = services.NewPubSub()
	mustCreateTopicHTTP(t, "orders")
	mustCreateSubscriptionHTTP(t, "orders", "alpha")

	resp := performJSONRequest(t, router, http.MethodPost, "/subscriptions", map[string]string{
		"topic":        "orders",
		"subscription": "alpha",
	})

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, resp.Code)
	}
}

func TestCreateSubscriptionEndpointRejectsMissingSubscriptionName(t *testing.T) {
	router := newTestRouter()
	ps = services.NewPubSub()
	mustCreateTopicHTTP(t, "orders")

	resp := performJSONRequest(t, router, http.MethodPost, "/subscriptions", map[string]string{
		"topic": "orders",
	})

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, resp.Code)
	}
}

func TestCreateSubscriptionEndpointRejectsWhitespaceOnlySubscriptionName(t *testing.T) {
	router := newTestRouter()
	ps = services.NewPubSub()
	mustCreateTopicHTTP(t, "orders")

	resp := performJSONRequest(t, router, http.MethodPost, "/subscriptions", map[string]string{
		"topic":        "orders",
		"subscription": "   ",
	})

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, resp.Code)
	}
}

func TestConsumeEndpointReturnsBadRequestForEmptyQueue(t *testing.T) {
	router := newTestRouter()
	ps = services.NewPubSub()
	mustCreateTopicHTTP(t, "orders")
	mustCreateSubscriptionHTTP(t, "orders", "alpha")

	resp := performJSONRequest(t, router, http.MethodPost, "/consume", map[string]string{
		"topic":        "orders",
		"subscription": "alpha",
	})

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, resp.Code)
	}
}

func TestDeleteSubscriptionEndpoint(t *testing.T) {
	router := newTestRouter()
	ps = services.NewPubSub()
	mustCreateTopicHTTP(t, "orders")
	mustCreateSubscriptionHTTP(t, "orders", "alpha")

	resp := performJSONRequest(t, router, http.MethodDelete, "/subscriptions", map[string]string{
		"topic":        "orders",
		"subscription": "alpha",
	})

	if resp.Code != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d", http.StatusNoContent, resp.Code)
	}
}

func newTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	ps = services.NewPubSub()

	router := gin.New()
	router.GET("/ping", HealthCheck)
	router.POST("/topics", CreateTopic)
	router.DELETE("/topics", DeleteTopic)
	router.GET("/topics", GetTopics)
	router.POST("/subscriptions", CreateSubscription)
	router.DELETE("/subscriptions", DeleteSubscription)
	router.GET("/subscriptions", GetSubscriptions)
	router.POST("/publish", PublishMessage)
	router.POST("/consume", ConsumeMessage)
	return router
}

func performJSONRequest(t *testing.T, router *gin.Engine, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var payload bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&payload).Encode(body); err != nil {
			t.Fatalf("encoding request body failed: %v", err)
		}
	}

	req := httptest.NewRequest(method, path, &payload)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	return recorder
}

func decodeResponse(t *testing.T, recorder *httptest.ResponseRecorder, target any) {
	t.Helper()

	if err := json.Unmarshal(recorder.Body.Bytes(), target); err != nil {
		t.Fatalf("decoding response failed: %v", err)
	}
}

func mustCreateTopicHTTP(t *testing.T, topic string) {
	t.Helper()

	if err := ps.CreateTopic(topic); err != nil {
		t.Fatalf("CreateTopic(%q) returned error: %v", topic, err)
	}
}

func mustCreateSubscriptionHTTP(t *testing.T, topic, subscription string) {
	t.Helper()

	if err := ps.Subscribe(topic, subscription); err != nil {
		t.Fatalf("Subscribe(%q, %q) returned error: %v", topic, subscription, err)
	}
}
