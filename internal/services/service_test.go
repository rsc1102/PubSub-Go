package services

import (
	"errors"
	"slices"
	"testing"
)

func TestCreateTopicAndGetTopics(t *testing.T) {
	ps := NewPubSub()

	if err := ps.CreateTopic("orders"); err != nil {
		t.Fatalf("CreateTopic returned error: %v", err)
	}

	topics := ps.GetTopics()
	if len(topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(topics))
	}
	if topics[0] != "orders" {
		t.Fatalf("expected topic %q, got %q", "orders", topics[0])
	}
}

func TestGetTopicsReturnsSortedTopics(t *testing.T) {
	ps := NewPubSub()
	for _, topic := range []string{"payments", "orders", "audit"} {
		if err := ps.CreateTopic(topic); err != nil {
			t.Fatalf("CreateTopic(%q) returned error: %v", topic, err)
		}
	}

	topics := ps.GetTopics()
	want := []string{"audit", "orders", "payments"}
	if !slices.Equal(topics, want) {
		t.Fatalf("expected topics %v, got %v", want, topics)
	}
}

func TestCreateTopicRejectsDuplicate(t *testing.T) {
	ps := NewPubSub()

	if err := ps.CreateTopic("orders"); err != nil {
		t.Fatalf("CreateTopic returned error: %v", err)
	}

	if err := ps.CreateTopic("orders"); err == nil {
		t.Fatal("expected duplicate topic creation to fail")
	}
}

func TestCreateTopicRejectsEmptyName(t *testing.T) {
	ps := NewPubSub()

	if err := ps.CreateTopic(""); err == nil {
		t.Fatal("expected empty topic creation to fail")
	} else if err.Error() == "topic already exists:" {
		t.Fatalf("expected validation error for empty topic, got %q", err.Error())
	}
}

func TestCreateTopicRejectsWhitespaceOnlyName(t *testing.T) {
	ps := NewPubSub()

	if err := ps.CreateTopic("   "); err == nil {
		t.Fatal("expected whitespace-only topic creation to fail")
	}
}

func TestSubscribePublishAndConsume(t *testing.T) {
	ps := NewPubSub()
	mustCreateTopic(t, ps, "orders")

	if err := ps.Subscribe("orders", "alpha"); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}

	if err := ps.Publish("orders", "created"); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	msg := mustReadNextMessage(t, ps, "orders", "alpha")
	if msg != "created" {
		t.Fatalf("expected message %q, got %q", "created", msg)
	}
}

func TestPublishFansOutToAllSubscribers(t *testing.T) {
	ps := NewPubSub()
	mustCreateTopic(t, ps, "orders")

	for _, subscription := range []string{"alpha", "beta"} {
		if err := ps.Subscribe("orders", subscription); err != nil {
			t.Fatalf("Subscribe(%q) returned error: %v", subscription, err)
		}
	}

	if err := ps.Publish("orders", "created"); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	for _, subscription := range []string{"alpha", "beta"} {
		msg := mustReadNextMessage(t, ps, "orders", subscription)
		if msg != "created" {
			t.Fatalf("expected fan-out message %q for %s, got %q", "created", subscription, msg)
		}
	}
}

func TestGetSubscriptionsByTopicAndAllTopics(t *testing.T) {
	ps := NewPubSub()
	mustCreateTopic(t, ps, "orders")
	mustCreateTopic(t, ps, "payments")

	for _, tc := range []struct {
		topic        string
		subscription string
	}{
		{topic: "orders", subscription: "alpha"},
		{topic: "orders", subscription: "beta"},
		{topic: "payments", subscription: "audit"},
	} {
		if err := ps.Subscribe(tc.topic, tc.subscription); err != nil {
			t.Fatalf("Subscribe(%q, %q) returned error: %v", tc.topic, tc.subscription, err)
		}
	}

	ordersOnly, err := ps.GetSubscriptions("orders")
	if err != nil {
		t.Fatalf("GetSubscriptions returned error: %v", err)
	}
	gotOrders := ordersOnly["orders"]
	slices.Sort(gotOrders)
	wantOrders := []string{"alpha", "beta"}
	if !slices.Equal(gotOrders, wantOrders) {
		t.Fatalf("expected orders subscriptions %v, got %v", wantOrders, gotOrders)
	}

	allSubscriptions, err := ps.GetSubscriptions("")
	if err != nil {
		t.Fatalf("GetSubscriptions(all) returned error: %v", err)
	}
	if len(allSubscriptions) != 2 {
		t.Fatalf("expected subscriptions for 2 topics, got %d", len(allSubscriptions))
	}
	gotAllOrders := allSubscriptions["orders"]
	wantAllOrders := []string{"alpha", "beta"}
	if !slices.Equal(gotAllOrders, wantAllOrders) {
		t.Fatalf("expected sorted orders subscriptions %v, got %v", wantAllOrders, gotAllOrders)
	}
}

func TestUnsubscribeRemovesSubscription(t *testing.T) {
	ps := NewPubSub()
	mustCreateTopic(t, ps, "orders")
	if err := ps.Subscribe("orders", "alpha"); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}

	if err := ps.Unsubscribe("orders", "alpha"); err != nil {
		t.Fatalf("Unsubscribe returned error: %v", err)
	}

	if _, err := ps.SubscriptionStream("orders", "alpha"); err == nil {
		t.Fatal("expected stream lookup on removed subscription to fail")
	}
}

func TestDeleteTopicRemovesTopicAndSubscriptions(t *testing.T) {
	ps := NewPubSub()
	mustCreateTopic(t, ps, "orders")
	if err := ps.Subscribe("orders", "alpha"); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}

	ps.DeleteTopic("orders")

	if _, err := ps.GetSubscriptions("orders"); err == nil {
		t.Fatal("expected deleted topic lookup to fail")
	}
	if topics := ps.GetTopics(); len(topics) != 0 {
		t.Fatalf("expected no topics after delete, got %v", topics)
	}
}

func TestSubscriptionStreamStartsEmpty(t *testing.T) {
	ps := NewPubSub()
	mustCreateTopic(t, ps, "orders")
	if err := ps.Subscribe("orders", "alpha"); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}

	if _, ok := readQueuedMessage(ps, "orders", "alpha"); ok {
		t.Fatal("expected subscription buffer to start empty")
	}
}

func TestSubscribeRejectsDuplicateSubscriptionName(t *testing.T) {
	ps := NewPubSub()
	mustCreateTopic(t, ps, "orders")

	if err := ps.Subscribe("orders", "alpha"); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}

	if err := ps.Subscribe("orders", "alpha"); err == nil {
		t.Fatal("expected duplicate subscription creation to fail")
	}
}

func TestSubscribeRejectsEmptySubscriptionName(t *testing.T) {
	ps := NewPubSub()
	mustCreateTopic(t, ps, "orders")

	if err := ps.Subscribe("orders", ""); err == nil {
		t.Fatal("expected empty subscription name to fail")
	}
}

func TestSubscribeRejectsWhitespaceOnlySubscriptionName(t *testing.T) {
	ps := NewPubSub()
	mustCreateTopic(t, ps, "orders")

	if err := ps.Subscribe("orders", "   "); err == nil {
		t.Fatal("expected whitespace-only subscription name to fail")
	}
}

func TestPublishIgnoresFullSubscriberQueues(t *testing.T) {
	ps := NewPubSub()
	mustCreateTopic(t, ps, "orders")
	if err := ps.Subscribe("orders", "alpha"); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}

	for i := 0; i < defaultQueueCapacity; i++ {
		if err := ps.Publish("orders", "created"); err != nil {
			t.Fatalf("Publish returned error before queue was full: %v", err)
		}
	}

	if err := ps.Publish("orders", "overflow"); err != nil {
		t.Fatalf("expected publish to ignore full queues, got %v", err)
	}
	for i := 0; i < defaultQueueCapacity; i++ {
		msg := mustReadNextMessage(t, ps, "orders", "alpha")
		if msg != "created" {
			t.Fatalf("expected queued message %q, got %q", "created", msg)
		}
	}
	if _, ok := readQueuedMessage(ps, "orders", "alpha"); ok {
		t.Fatal("expected overflow message to be dropped when queue is full")
	}
}

func TestPublishReturnsErrorWhenTopicHasNoSubscriptions(t *testing.T) {
	ps := NewPubSub()
	mustCreateTopic(t, ps, "orders")

	err := ps.Publish("orders", "created")
	if err == nil {
		t.Fatal("expected publish to fail when topic has no subscriptions")
	}
	if !errors.Is(err, ErrTopicHasNoSubscriptions) {
		t.Fatalf("expected ErrTopicHasNoSubscriptions, got %v", err)
	}
}

func TestPublishSkipsFullSubscribersAndDeliversToAvailableOnes(t *testing.T) {
	ps := NewPubSub()
	mustCreateTopic(t, ps, "orders")
	for _, subscription := range []string{"alpha", "beta"} {
		if err := ps.Subscribe("orders", subscription); err != nil {
			t.Fatalf("Subscribe(%q) returned error: %v", subscription, err)
		}
	}

	for i := 0; i < defaultQueueCapacity; i++ {
		if err := ps.Publish("orders", "created"); err != nil {
			t.Fatalf("Publish returned error before queue was full: %v", err)
		}
	}

	if _, ok := readQueuedMessage(ps, "orders", "beta"); !ok {
		t.Fatal("expected beta stream to have a queued message before saturation check")
	}

	if err := ps.Publish("orders", "overflow"); err != nil {
		t.Fatalf("expected publish to succeed, got %v", err)
	}

	for _, tc := range []struct {
		subscription string
		remaining    []string
	}{
		{subscription: "alpha", remaining: repeatMessage("created", defaultQueueCapacity)},
		{subscription: "beta", remaining: append(repeatMessage("created", defaultQueueCapacity-1), "overflow")},
	} {
		for _, expected := range tc.remaining {
			msg := mustReadNextMessage(t, ps, "orders", tc.subscription)
			if msg != expected {
				t.Fatalf("expected queued message %q for %s, got %q", expected, tc.subscription, msg)
			}
		}
		if _, ok := readQueuedMessage(ps, "orders", tc.subscription); ok {
			t.Fatalf("expected no additional message to be delivered to %s", tc.subscription)
		}
	}
}

func TestNewPubSubUsesCustomQueueSizeWhenProvided(t *testing.T) {
	ps := NewPubSub(2)
	mustCreateTopic(t, ps, "orders")
	if err := ps.Subscribe("orders", "alpha"); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}

	for i := 0; i < 2; i++ {
		if err := ps.Publish("orders", "created"); err != nil {
			t.Fatalf("Publish returned error before queue was full: %v", err)
		}
	}

	if err := ps.Publish("orders", "overflow"); err != nil {
		t.Fatalf("expected publish to ignore full queues, got %v", err)
	}
	for i := 0; i < 2; i++ {
		msg := mustReadNextMessage(t, ps, "orders", "alpha")
		if msg != "created" {
			t.Fatalf("expected queued message %q, got %q", "created", msg)
		}
	}
	if _, ok := readQueuedMessage(ps, "orders", "alpha"); ok {
		t.Fatal("expected overflow message to be dropped when custom queue is full")
	}
}

func TestSubscribeTrimsTopicNameLikeCreateTopic(t *testing.T) {
	ps := NewPubSub()
	mustCreateTopic(t, ps, " orders ")

	if err := ps.Subscribe(" orders ", "alpha"); err != nil {
		t.Fatalf("expected subscribe with original spaced topic name to succeed, got %v", err)
	}
}

func TestPublishTrimsTopicNameLikeCreateTopic(t *testing.T) {
	ps := NewPubSub()
	mustCreateTopic(t, ps, " orders ")
	if err := ps.Subscribe("orders", "alpha"); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}

	if err := ps.Publish(" orders ", "created"); err != nil {
		t.Fatalf("expected publish with original spaced topic name to succeed, got %v", err)
	}

	msg := mustReadNextMessage(t, ps, "orders", "alpha")
	if msg != "created" {
		t.Fatalf("expected message %q, got %q", "created", msg)
	}
}

func TestDeleteTopicTrimsNameLikeCreateTopic(t *testing.T) {
	ps := NewPubSub()
	mustCreateTopic(t, ps, " orders ")

	ps.DeleteTopic(" orders ")

	if _, err := ps.GetSubscriptions("orders"); err == nil {
		t.Fatal("expected delete with original spaced topic name to remove normalized topic")
	}
	if topics := ps.GetTopics(); len(topics) != 0 {
		t.Fatalf("expected no topics after delete, got %v", topics)
	}
}

func mustCreateTopic(t *testing.T, ps *PubSub, topic string) {
	t.Helper()

	if err := ps.CreateTopic(topic); err != nil {
		t.Fatalf("CreateTopic(%q) returned error: %v", topic, err)
	}
}

func mustReadNextMessage(t *testing.T, ps *PubSub, topic, subscription string) string {
	t.Helper()

	stream, err := ps.SubscriptionStream(topic, subscription)
	if err != nil {
		t.Fatalf("SubscriptionStream(%q, %q) returned error: %v", topic, subscription, err)
	}

	msg, ok := <-stream
	if !ok {
		t.Fatalf("subscription stream %q closed unexpectedly", subscription)
	}

	return msg
}

func readQueuedMessage(ps *PubSub, topic, subscription string) (string, bool) {
	stream, err := ps.SubscriptionStream(topic, subscription)
	if err != nil {
		return "", false
	}

	select {
	case msg, ok := <-stream:
		if !ok {
			return "", false
		}
		return msg, true
	default:
		return "", false
	}
}

func repeatMessage(msg string, count int) []string {
	items := make([]string, count)
	for i := range items {
		items[i] = msg
	}
	return items
}
