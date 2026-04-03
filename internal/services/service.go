package services

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
)

type Topic struct {
	Topic string `json:"topic"`
}

type Message struct {
	Topic   string `json:"topic"`
	Content string `json:"content"`
}

type Subscription struct {
	Topic        string `json:"topic"`
	Subscription string `json:"subscription"`
}

type PubSub struct {
	mutex  sync.RWMutex
	topics map[string]map[string]chan string
}

// NewPubSub creates a new PubSub instance
func NewPubSub() *PubSub {
	return &PubSub{
		topics: make(map[string]map[string]chan string),
	}
}

func normalizeName(value string) string {
	return strings.TrimSpace(value)
}

// Get all topics
func (ps *PubSub) GetTopics() []string {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	keys := []string{}
	for key := range ps.topics {
		keys = append(keys, key)
	}
	return keys
}

// Create a topic
func (ps *PubSub) CreateTopic(topic string) error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	normalizedTopic := normalizeName(topic)
	if normalizedTopic == "" {
		return errors.New("topic name cannot be empty")
	}

	if _, exists := ps.topics[normalizedTopic]; exists {
		return errors.New("topic already exists: " + normalizedTopic)
	}

	ps.topics[normalizedTopic] = make(map[string]chan string)
	return nil
}

// Delete a topic
func (ps *PubSub) DeleteTopic(topic string) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	normalizedTopic := normalizeName(topic)
	subscriptions, ok := ps.topics[topic]
	if !ok {
		subscriptions, ok = ps.topics[normalizedTopic]
	}
	if ok {
		for _, ch := range subscriptions {
			close(ch)
		}
		delete(ps.topics, normalizedTopic)
	}
}

// Get all the subscriptions
func (ps *PubSub) GetSubscriptions(topic string) (map[string][]string, error) {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	subscriptionsMap := map[string][]string{}
	normalizedTopic := normalizeName(topic)
	if normalizedTopic == "" {
		for t, subscriptions := range ps.topics {
			subscriptionsMap[t] = []string{}
			for sub := range subscriptions {
				subscriptionsMap[t] = append(subscriptionsMap[t], sub)
			}
		}
		return subscriptionsMap, nil
	}

	subscriptions, ok := ps.topics[normalizedTopic]
	if !ok {
		return nil, errors.New("topic not found: " + normalizedTopic)
	} else {
		subscriptionsMap[normalizedTopic] = []string{}
		for sub := range subscriptions {
			subscriptionsMap[normalizedTopic] = append(subscriptionsMap[normalizedTopic], sub)
		}
		return subscriptionsMap, nil
	}

}

// Subscribe creates a subscription to a specific topic
func (ps *PubSub) Subscribe(topic string, subscription string) error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	normalizedTopic := normalizeName(topic)
	normalizedSubscription := normalizeName(subscription)
	if normalizedSubscription == "" {
		return errors.New("subscription name cannot be empty")
	}

	innerMap, ok := ps.topics[normalizedTopic]
	if !ok {
		return errors.New("topic not found: " + normalizedTopic)
	}
	if _, exists := innerMap[normalizedSubscription]; exists {
		return errors.New("subscription already exists: " + normalizedSubscription)
	}

	ch := make(chan string, 3) // Buffered channel with a capacity of 10
	innerMap[normalizedSubscription] = ch
	return nil
}

// Unsubscribe removes a subscription from a specific topic
func (ps *PubSub) Unsubscribe(topic string, subscription string) error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	normalizedTopic := normalizeName(topic)
	normalizedSubscription := normalizeName(subscription)
	innerMap, ok := ps.topics[normalizedTopic]
	if !ok {
		return errors.New("topic not found: " + normalizedTopic)
	}
	channel, ok := innerMap[normalizedSubscription]
	if !ok {
		return errors.New("subscription not found: " + normalizedSubscription)
	}
	close(channel)
	delete(innerMap, normalizedSubscription)
	return nil
}

// Publish sends a message to all subscriptions of a specific topic
func (ps *PubSub) Publish(topic, msg string) error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	normalizedTopic := normalizeName(topic)
	subscriptions, ok := ps.topics[normalizedTopic]
	if !ok {
		return errors.New("topic not found: " + normalizedTopic)
	}

	fullSubscriptions := []string{}
	for sub, ch := range subscriptions {
		select {
		case ch <- msg:
		default:
			log.Printf("Skipping publishing message to subscription %s as channel is full\n", sub)
			fullSubscriptions = append(fullSubscriptions, sub)
		}
	}

	if len(fullSubscriptions) > 0 {
		return fmt.Errorf("message not delivered to subscriptions with full queues: %s", strings.Join(fullSubscriptions, ", "))
	}

	return nil
}

// Consume lets a subscription consume the first message in the queue
func (ps *PubSub) Consume(topic string, subscription string) (string, error) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	normalizedTopic := normalizeName(topic)
	normalizedSubscription := normalizeName(subscription)
	innerMap, ok := ps.topics[normalizedTopic]
	if !ok {
		return "", errors.New("topic not found: " + normalizedTopic)
	}
	channel, ok := innerMap[normalizedSubscription]
	if !ok {
		return "", errors.New("subscription not found: " + normalizedSubscription)
	}

	select {
	case msg := <-channel:
		return msg, nil
	default:
		return "", errors.New("message queue empty")
	}

}
