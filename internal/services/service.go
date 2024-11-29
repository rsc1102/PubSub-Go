package services

import (
	"errors"
	"log"
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

	if _, exists := ps.topics[topic]; !exists && topic != "" {
		ps.topics[topic] = make(map[string]chan string)
		return nil
	} else {
		return errors.New("topic already exists:" + topic)
	}
}

// Delete a topic
func (ps *PubSub) DeleteTopic(topic string) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	subscriptions, ok := ps.topics[topic]
	if ok {
		for _, ch := range subscriptions {
			close(ch)
		}
		delete(ps.topics, topic)
	}
}

// Get all the subscriptions
func (ps *PubSub) GetSubscriptions(topic string) (map[string][]string, error) {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	subscriptionsMap := map[string][]string{}
	if topic == "" {
		for t, subscriptions := range ps.topics {
			subscriptionsMap[t] = []string{}
			for sub := range subscriptions {
				subscriptionsMap[t] = append(subscriptionsMap[t], sub)
			}
		}
		return subscriptionsMap, nil
	}

	subscriptions, ok := ps.topics[topic]
	if !ok {
		return nil, errors.New("topic not found: " + topic)
	} else {
		subscriptionsMap[topic] = []string{}
		for sub := range subscriptions {
			subscriptionsMap[topic] = append(subscriptionsMap[topic], sub)
		}
		return subscriptionsMap, nil
	}

}

// Subscribe creates a subscription to a specific topic
func (ps *PubSub) Subscribe(topic string, subscription string) error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	innerMap, ok := ps.topics[topic]
	if !ok {
		return errors.New("topic not found: " + topic)
	}

	ch := make(chan string, 3) // Buffered channel with a capacity of 10
	innerMap[subscription] = ch
	return nil
}

// Unsubscribe removes a subscription from a specific topic
func (ps *PubSub) Unsubscribe(topic string, subscription string) error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	innerMap, ok := ps.topics[topic]
	if !ok {
		return errors.New("topic not found: " + topic)
	}
	channel, ok := innerMap[subscription]
	if !ok {
		return errors.New("subscription not found: " + subscription)
	}
	close(channel)
	delete(innerMap, subscription)
	return nil
}

// Publish sends a message to all subscriptions of a specific topic
func (ps *PubSub) Publish(topic, msg string) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	subscriptions, ok := ps.topics[topic]
	if ok {
		for sub, ch := range subscriptions {
			select {
			case ch <- msg:
			default:
				log.Printf("Skipping publishing message to subscription %s as channel is full\n", sub)
			}
		}
	}
}

// Consume lets a subscription consume the first message in the queue
func (ps *PubSub) Consume(topic string, subscription string) (string, error) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	innerMap, ok := ps.topics[topic]
	if !ok {
		return "", errors.New("topic not found: " + topic)
	}
	channel, ok := innerMap[subscription]
	if !ok {
		return "", errors.New("subscription not found: " + subscription)
	}

	select {
	case msg := <-channel:
		return msg, nil
	default:
		return "", errors.New("message queue empty")
	}

}
