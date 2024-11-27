package main

import (
	"fmt"
	"sync"
)

// PubSub is the message broker. It maintains topics and their subscribers

type PubSub struct {
	mutex  sync.RWMutex
	topics map[string][]chan string
}

// NewPubSub creates a new PubSub instance
func NewPubSub() *PubSub {
	return &PubSub{
		topics: make(map[string][]chan string),
	}
}

// Subscribe allows clients to subscribe to a specific topic
func (ps *PubSub) Subscribe(topic string) <-chan string {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	ch := make(chan string, 10) // Buffered channel with a capacity of 10
	ps.topics[topic] = append(ps.topics[topic], ch)
	return ch
}

// Unsubscribe removes a subscriber from a specific topic
func (ps *PubSub) Unsubscribe(topic string, subscriber <-chan string) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	subscribers := ps.topics[topic]
	for i, sub := range subscribers {
		if sub == subscriber {
			// Remove the subscriber by slicing it out
			ps.topics[topic] = append(subscribers[:i], subscribers[i+1:]...)
			close(sub)
			break
		}
	}
}

// Publish sends a message to all subscribers of a specific topic
func (ps *PubSub) Publish(topic, msg string) {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	subscribers, ok := ps.topics[topic]
	if ok {
		for _, ch := range subscribers {
			select {
			case ch <- msg:
			default:
				fmt.Println("Skipping message as channel is full")
			}
		}
	}
}

func main() {
	ps := NewPubSub()
	var wg sync.WaitGroup

	// Subscribe to "topic1"
	subscriber := ps.Subscribe("topic1")

	// Listen to messages from "topic1" asynchronously
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range subscriber {
			fmt.Printf("Received: %s\n", msg)
		}
	}()

	// Publish messages to "topic1"
	ps.Publish("topic1", "Hello, World!")
	ps.Publish("topic1", "Another message")

	// Unsubscribe the subscriber to close the channel and stop the goroutine
	ps.Unsubscribe("topic1", subscriber)

	// Wait for the subscriber goroutine to finish
	wg.Wait()
	fmt.Println("All messages processed, exiting.")
}
