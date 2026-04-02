package services

import (
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

	ps.Publish("orders", "created")

	msg, err := ps.Consume("orders", "alpha")
	if err != nil {
		t.Fatalf("Consume returned error: %v", err)
	}
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

	ps.Publish("orders", "created")

	for _, subscription := range []string{"alpha", "beta"} {
		msg, err := ps.Consume("orders", subscription)
		if err != nil {
			t.Fatalf("Consume(%q) returned error: %v", subscription, err)
		}
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

	if _, err := ps.Consume("orders", "alpha"); err == nil {
		t.Fatal("expected consume on removed subscription to fail")
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

func TestConsumeReturnsErrorWhenQueueIsEmpty(t *testing.T) {
	ps := NewPubSub()
	mustCreateTopic(t, ps, "orders")
	if err := ps.Subscribe("orders", "alpha"); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}

	if _, err := ps.Consume("orders", "alpha"); err == nil {
		t.Fatal("expected empty queue consume to fail")
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

func mustCreateTopic(t *testing.T, ps *PubSub, topic string) {
	t.Helper()

	if err := ps.CreateTopic(topic); err != nil {
		t.Fatalf("CreateTopic(%q) returned error: %v", topic, err)
	}
}
