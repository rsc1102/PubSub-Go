package services

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
)

func BenchmarkPublish(b *testing.B) {
	for _, subscriberCount := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("subscribers=%d", subscriberCount), func(b *testing.B) {
			ps := NewPubSub()
			mustCreateTopicBenchmark(b, ps, "orders")
			for i := 0; i < subscriberCount; i++ {
				mustSubscribeBenchmark(b, ps, "orders", fmt.Sprintf("sub-%d", i))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := ps.Publish("orders", "created"); err != nil {
					b.Fatalf("Publish returned error: %v", err)
				}
				drainAllSubscriptions(b, ps, "orders", subscriberCount)
			}
		})
	}
}

func BenchmarkConsume(b *testing.B) {
	ps := NewPubSub()
	mustCreateTopicBenchmark(b, ps, "orders")
	mustSubscribeBenchmark(b, ps, "orders", "alpha")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ps.Publish("orders", "created"); err != nil {
			b.Fatalf("Publish returned error: %v", err)
		}
		drainNextMessageBenchmark(b, ps, "orders", "alpha")
	}
}

func BenchmarkPublishParallel(b *testing.B) {
	ps := NewPubSub()
	workerTopics := runtime.GOMAXPROCS(0) * 4
	for i := 0; i < workerTopics; i++ {
		topic := fmt.Sprintf("orders-%d", i)
		mustCreateTopicBenchmark(b, ps, topic)
		for sub := 0; sub < 100; sub++ {
			mustSubscribeBenchmark(b, ps, topic, fmt.Sprintf("sub-%d", sub))
		}
	}

	var seq atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		topic := fmt.Sprintf("orders-%d", (seq.Add(1)-1)%uint64(workerTopics))
		for pb.Next() {
			if err := ps.Publish(topic, "created"); err != nil {
				b.Fatalf("Publish(%q) returned error: %v", topic, err)
			}
			drainAllSubscriptions(b, ps, topic, 100)
		}
	})
}

func BenchmarkPublishConsumeParallel(b *testing.B) {
	ps := NewPubSub()
	workerTopics := runtime.GOMAXPROCS(0) * 4
	for i := 0; i < workerTopics; i++ {
		topic := fmt.Sprintf("orders-%d", i)
		mustCreateTopicBenchmark(b, ps, topic)
		mustSubscribeBenchmark(b, ps, topic, "alpha")
	}

	var seq atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		topic := fmt.Sprintf("orders-%d", (seq.Add(1)-1)%uint64(workerTopics))
		for pb.Next() {
			if err := ps.Publish(topic, "created"); err != nil {
				b.Fatalf("Publish(%q) returned error: %v", topic, err)
			}
			drainNextMessageBenchmark(b, ps, topic, "alpha")
		}
	})
}

func drainAllSubscriptions(b *testing.B, ps *PubSub, topic string, subscriberCount int) {
	b.Helper()

	for i := 0; i < subscriberCount; i++ {
		subscription := fmt.Sprintf("sub-%d", i)
		drainNextMessageBenchmark(b, ps, topic, subscription)
	}
}

func drainSubscription(b *testing.B, ps *PubSub, topic, subscription string) {
	b.Helper()

	for {
		if _, ok := tryReadQueuedMessageBenchmark(b, ps, topic, subscription); !ok {
			return
		}
	}
}

func mustCreateTopicBenchmark(b *testing.B, ps *PubSub, topic string) {
	b.Helper()

	if err := ps.CreateTopic(topic); err != nil {
		b.Fatalf("CreateTopic(%q) returned error: %v", topic, err)
	}
}

func mustSubscribeBenchmark(b *testing.B, ps *PubSub, topic, subscription string) {
	b.Helper()

	if err := ps.Subscribe(topic, subscription); err != nil {
		b.Fatalf("Subscribe(%q, %q) returned error: %v", topic, subscription, err)
	}
}

func drainNextMessageBenchmark(b *testing.B, ps *PubSub, topic, subscription string) string {
	b.Helper()

	stream, err := ps.SubscriptionStream(topic, subscription)
	if err != nil {
		b.Fatalf("SubscriptionStream(%q, %q) returned error: %v", topic, subscription, err)
	}

	msg, ok := <-stream
	if !ok {
		b.Fatalf("subscription stream %q closed unexpectedly", subscription)
	}

	return msg
}

func tryReadQueuedMessageBenchmark(b *testing.B, ps *PubSub, topic, subscription string) (string, bool) {
	b.Helper()

	stream, err := ps.SubscriptionStream(topic, subscription)
	if err != nil {
		b.Fatalf("SubscriptionStream(%q, %q) returned error: %v", topic, subscription, err)
	}

	select {
	case msg, ok := <-stream:
		if !ok {
			b.Fatalf("subscription stream %q closed unexpectedly", subscription)
		}
		return msg, true
	default:
		return "", false
	}
}
