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

func BenchmarkCreateTopicEndpoint(b *testing.B) {
	router := newBenchmarkRouter()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body := mustMarshalBenchmarkJSON(b, map[string]string{
			"topic": fmt.Sprintf("orders-%d", i),
		})
		recorder := performBenchmarkRequest(router, http.MethodPost, "/topics", body)
		if recorder.Code != http.StatusCreated {
			b.Fatalf("expected status %d, got %d", http.StatusCreated, recorder.Code)
		}
	}
}

func BenchmarkCreateSubscriptionEndpoint(b *testing.B) {
	router := newBenchmarkRouter()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic := fmt.Sprintf("orders-%d", i)
		mustCreateTopicBenchmarkHTTP(b, topic)

		body := mustMarshalBenchmarkJSON(b, map[string]string{
			"topic":        topic,
			"subscription": "alpha",
		})
		recorder := performBenchmarkRequest(router, http.MethodPost, "/subscriptions", body)
		if recorder.Code != http.StatusCreated {
			b.Fatalf("expected status %d, got %d", http.StatusCreated, recorder.Code)
		}
	}
}

func BenchmarkPublishEndpoint(b *testing.B) {
	router := newBenchmarkRouter()
	mustCreateTopicBenchmarkHTTP(b, "orders")
	for i := 0; i < 100; i++ {
		mustCreateSubscriptionBenchmarkHTTP(b, "orders", fmt.Sprintf("sub-%d", i))
	}

	body := mustMarshalBenchmarkJSON(b, map[string]string{
		"topic":   "orders",
		"content": "created",
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		recorder := performBenchmarkRequest(router, http.MethodPost, "/publish", body)
		if recorder.Code != http.StatusCreated {
			b.Fatalf("expected status %d, got %d", http.StatusCreated, recorder.Code)
		}
		drainBenchmarkSubscriptions(b, "orders", 100)
	}
}

func BenchmarkStreamEndpointFirstEvent(b *testing.B) {
	router := newBenchmarkRouter()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ps = services.NewPubSub()
		topic := fmt.Sprintf("orders-%d", i)
		subscription := "alpha"
		mustCreateTopicBenchmarkHTTP(b, topic)
		mustCreateSubscriptionBenchmarkHTTP(b, topic, subscription)

		server := httptest.NewServer(router)
		streamResp, err := http.Get(server.URL + "/stream?topic=" + topic + "&subscription=" + subscription)
		if err != nil {
			b.Fatalf("opening stream failed: %v", err)
		}

		if err := ps.Publish(topic, "created"); err != nil {
			b.Fatalf("Publish returned error: %v", err)
		}

		if _, err := readBenchmarkSSEEvent(streamResp.Body, 2*time.Second); err != nil {
			b.Fatalf("reading SSE event failed: %v", err)
		}

		streamResp.Body.Close()
		server.Close()
	}
}

func newBenchmarkRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	ps = services.NewPubSub()

	router := gin.New()
	router.POST("/topics", CreateTopic)
	router.POST("/subscriptions", CreateSubscription)
	router.POST("/publish", PublishMessage)
	router.GET("/stream", StreamMessages)
	return router
}

func performBenchmarkRequest(router *gin.Engine, method, path string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	return recorder
}

func mustMarshalBenchmarkJSON(b *testing.B, payload any) []byte {
	b.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		b.Fatalf("Marshal returned error: %v", err)
	}
	return body
}

func mustCreateTopicBenchmarkHTTP(b *testing.B, topic string) {
	b.Helper()

	if err := ps.CreateTopic(topic); err != nil {
		b.Fatalf("CreateTopic(%q) returned error: %v", topic, err)
	}
}

func mustCreateSubscriptionBenchmarkHTTP(b *testing.B, topic, subscription string) {
	b.Helper()

	if err := ps.Subscribe(topic, subscription); err != nil {
		b.Fatalf("Subscribe(%q, %q) returned error: %v", topic, subscription, err)
	}
}

func drainBenchmarkSubscriptions(b *testing.B, topic string, subscriberCount int) {
	b.Helper()

	for i := 0; i < subscriberCount; i++ {
		subscription := fmt.Sprintf("sub-%d", i)
		stream, err := ps.SubscriptionStream(topic, subscription)
		if err != nil {
			b.Fatalf("SubscriptionStream(%q, %q) returned error: %v", topic, subscription, err)
		}
		if _, ok := <-stream; !ok {
			b.Fatalf("subscription stream %q closed unexpectedly", subscription)
		}
	}
}

func readBenchmarkSSEEvent(body io.Reader, timeout time.Duration) (string, error) {
	reader := bufio.NewReader(body)
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if err != nil {
			return "", err
		}

		if strings.HasPrefix(line, "data: ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "data: ")), nil
		}
	}

	return "", fmt.Errorf("timed out waiting for SSE event")
}
