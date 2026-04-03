package handlers

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

func TestPublishAndStreamEndpoints(t *testing.T) {
	router := newTestRouter()
	ps = services.NewPubSub()
	mustCreateTopicHTTP(t, "orders")
	mustCreateSubscriptionHTTP(t, "orders", "alpha")

	server := httptest.NewServer(router)
	defer server.Close()

	streamResp, err := http.Get(server.URL + "/stream?topic=orders&subscription=alpha")
	if err != nil {
		t.Fatalf("opening stream failed: %v", err)
	}
	defer streamResp.Body.Close()

	if streamResp.StatusCode != http.StatusOK {
		t.Fatalf("expected stream status %d, got %d", http.StatusOK, streamResp.StatusCode)
	}

	publishResp := performJSONRequest(t, router, http.MethodPost, "/publish", map[string]string{
		"topic":   "orders",
		"content": "created",
	})
	if publishResp.Code != http.StatusCreated {
		t.Fatalf("expected publish status %d, got %d", http.StatusCreated, publishResp.Code)
	}

	rawEvent, err := readSSEEvent(streamResp.Body, 2*time.Second)
	if err != nil {
		t.Fatalf("reading SSE event failed: %v", err)
	}

	var event struct {
		Topic        string `json:"topic"`
		Subscription string `json:"subscription"`
		Content      string `json:"content"`
	}
	if err := json.Unmarshal([]byte(rawEvent), &event); err != nil {
		t.Fatalf("decoding SSE payload failed: %v", err)
	}
	if event.Content != "created" {
		t.Fatalf("expected streamed message %q, got %q", "created", event.Content)
	}
	if event.Topic != "orders" || event.Subscription != "alpha" {
		t.Fatalf("unexpected stream metadata: %+v", event)
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

func TestPublishEndpointRejectsTopicWithoutSubscriptions(t *testing.T) {
	router := newTestRouter()
	ps = services.NewPubSub()
	mustCreateTopicHTTP(t, "orders")

	resp := performJSONRequest(t, router, http.MethodPost, "/publish", map[string]string{
		"topic":   "orders",
		"content": "created",
	})

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, resp.Code)
	}
}

func TestPublishEndpointIgnoresFullSubscriberQueues(t *testing.T) {
	router := newTestRouter()
	ps = services.NewPubSub(1)
	mustCreateTopicHTTP(t, "orders")
	mustCreateSubscriptionHTTP(t, "orders", "alpha")
	mustCreateSubscriptionHTTP(t, "orders", "beta")

	firstResp := performJSONRequest(t, router, http.MethodPost, "/publish", map[string]string{
		"topic":   "orders",
		"content": "created",
	})
	if firstResp.Code != http.StatusCreated {
		t.Fatalf("expected first publish status %d, got %d", http.StatusCreated, firstResp.Code)
	}

	stream, err := ps.SubscriptionStream("orders", "beta")
	if err != nil {
		t.Fatalf("SubscriptionStream returned error: %v", err)
	}
	if _, ok := <-stream; !ok {
		t.Fatal("expected beta stream to remain open")
	}

	secondResp := performJSONRequest(t, router, http.MethodPost, "/publish", map[string]string{
		"topic":   "orders",
		"content": "created-again",
	})

	if secondResp.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d", http.StatusCreated, secondResp.Code)
	}

	alphaStream, err := ps.SubscriptionStream("orders", "alpha")
	if err != nil {
		t.Fatalf("SubscriptionStream(alpha) returned error: %v", err)
	}
	select {
	case msg := <-alphaStream:
		if msg != "created" {
			t.Fatalf("expected alpha to keep only the original message, got %q", msg)
		}
	default:
		t.Fatal("expected alpha to still have the original queued message")
	}
	select {
	case msg := <-alphaStream:
		t.Fatalf("expected alpha to drop the second message, got %q", msg)
	default:
	}

	betaStream, err := ps.SubscriptionStream("orders", "beta")
	if err != nil {
		t.Fatalf("SubscriptionStream(beta) returned error: %v", err)
	}
	select {
	case msg := <-betaStream:
		if msg != "created-again" {
			t.Fatalf("expected beta to receive %q, got %q", "created-again", msg)
		}
	default:
		t.Fatal("expected beta to receive the second published message")
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

func TestStreamEndpointRequiresExistingSubscription(t *testing.T) {
	router := newTestRouter()
	ps = services.NewPubSub()
	mustCreateTopicHTTP(t, "orders")

	req := httptest.NewRequest(http.MethodGet, "/stream?topic=orders&subscription=alpha", nil)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, resp.Code)
	}
}

func TestStreamEndpointRejectsMissingQueryParameters(t *testing.T) {
	router := newTestRouter()

	req := httptest.NewRequest(http.MethodGet, "/stream", nil)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, resp.Code)
	}
}

func TestStreamEndpointEmitsErrorEventWhenSubscriptionCloses(t *testing.T) {
	router := newTestRouter()
	ps = services.NewPubSub()
	mustCreateTopicHTTP(t, "orders")
	mustCreateSubscriptionHTTP(t, "orders", "alpha")

	server := httptest.NewServer(router)
	defer server.Close()

	streamResp, err := http.Get(server.URL + "/stream?topic=orders&subscription=alpha")
	if err != nil {
		t.Fatalf("opening stream failed: %v", err)
	}
	defer streamResp.Body.Close()

	if streamResp.StatusCode != http.StatusOK {
		t.Fatalf("expected stream status %d, got %d", http.StatusOK, streamResp.StatusCode)
	}

	if err := ps.Unsubscribe("orders", "alpha"); err != nil {
		t.Fatalf("Unsubscribe returned error: %v", err)
	}

	eventName, rawEvent, err := readSSEFrame(streamResp.Body, 2*time.Second)
	if err != nil {
		t.Fatalf("reading SSE error event failed: %v", err)
	}
	if eventName != "error" {
		t.Fatalf("expected SSE event %q, got %q", "error", eventName)
	}

	var event struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(rawEvent), &event); err != nil {
		t.Fatalf("decoding SSE error payload failed: %v", err)
	}
	if event.Message != services.ErrSubscriptionClosed.Error() {
		t.Fatalf("expected error message %q, got %q", services.ErrSubscriptionClosed.Error(), event.Message)
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
	router.GET("/stream", StreamMessages)
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

func readSSEEvent(body io.Reader, timeout time.Duration) (string, error) {
	_, data, err := readSSEFrame(body, timeout)
	return data, err
}

func readSSEFrame(body io.Reader, timeout time.Duration) (string, string, error) {
	reader := bufio.NewReader(body)
	deadline := time.Now().Add(timeout)
	eventName := ""
	data := ""

	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if err != nil {
			return "", "", err
		}

		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if data != "" {
				return eventName, data, nil
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		if strings.HasPrefix(line, "event: ") {
			eventName = strings.TrimSpace(strings.TrimPrefix(line, "event: "))
			continue
		}
		if strings.HasPrefix(line, "data: ") {
			data = strings.TrimSpace(strings.TrimPrefix(line, "data: "))
		}
	}

	return "", "", fmt.Errorf("timed out waiting for SSE event")
}
