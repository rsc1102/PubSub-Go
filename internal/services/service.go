package services

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
)

var ErrSubscriptionsHaveFullQueues = errors.New("message not delivered because subscriptions have full queues")
var ErrTopicHasNoSubscriptions = errors.New("message not delivered because topic has no subscriptions")

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
	mutex         sync.RWMutex
	topics        map[string]map[string]chan string
	queueCapacity int
}

const defaultQueueCapacity = 10

// NewPubSub creates a new PubSub instance
func NewPubSub(args ...int) *PubSub {
	queueCapacity := defaultQueueCapacity
	if len(args) > 0 && args[0] > 0 {
		queueCapacity = args[0]
	}

	return &PubSub{
		topics:        make(map[string]map[string]chan string),
		queueCapacity: queueCapacity,
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
	slices.Sort(keys)
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
			slices.Sort(subscriptionsMap[t])
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
		slices.Sort(subscriptionsMap[normalizedTopic])
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

	ch := make(chan string, ps.queueCapacity)
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
	if len(subscriptions) == 0 {
		return fmt.Errorf("%w: %s", ErrTopicHasNoSubscriptions, normalizedTopic)
	}

	fullSubscriptions := []string{}
	for sub, ch := range subscriptions {
		if len(ch) == cap(ch) {
			fullSubscriptions = append(fullSubscriptions, sub)
		}
	}

	if len(fullSubscriptions) > 0 {
		return fmt.Errorf("%w: %s", ErrSubscriptionsHaveFullQueues, strings.Join(fullSubscriptions, ", "))
	}

	for _, ch := range subscriptions {
		ch <- msg
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
