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
		if _, err := ps.Consume("orders", "alpha"); err != nil {
			b.Fatalf("Consume returned error: %v", err)
		}
	}
}

func BenchmarkPublishParallel(b *testing.B) {
	ps := NewPubSub()
	mustCreateTopicBenchmark(b, ps, "orders")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := ps.Publish("orders", "created"); err != nil {
				b.Fatalf("Publish returned error: %v", err)
			}
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
			if _, err := ps.Consume(topic, "alpha"); err != nil {
				b.Fatalf("Consume(%q) returned error: %v", topic, err)
			}
		}
	})
}

func drainAllSubscriptions(b *testing.B, ps *PubSub, topic string, subscriberCount int) {
	b.Helper()

	for i := 0; i < subscriberCount; i++ {
		subscription := fmt.Sprintf("sub-%d", i)
		if _, err := ps.Consume(topic, subscription); err != nil {
			b.Fatalf("Consume(%q) returned error: %v", subscription, err)
		}
	}
}

func drainSubscription(b *testing.B, ps *PubSub, topic, subscription string) {
	b.Helper()

	for {
		if _, err := ps.Consume(topic, subscription); err != nil {
			if err.Error() == "message queue empty" {
				return
			}
			b.Fatalf("Consume(%q) returned error: %v", subscription, err)
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
