package main

import (
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
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

func (ps *PubSub) CreateTopic(topic string) error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	if _, exists := ps.topics[topic]; !exists {
		ps.topics[topic] = make(map[string]chan string)
		return nil
	} else {
		return errors.New("topic already exists:" + topic)
	}
}

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

// Subscribe creates a subscription to a specific topic
func (ps *PubSub) Subscribe(topic string, subscription string) error {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	innerMap, ok := ps.topics[topic]
	if !ok {
		return errors.New("topic not found: " + topic)
	}

	ch := make(chan string, 10) // Buffered channel with a capacity of 10
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
		for _, ch := range subscriptions {
			select {
			case ch <- msg:
			default:
				fmt.Println("Skipping message as channel is full")
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

func main() {
	ps := NewPubSub()
	app := gin.Default()

	// Health Check
	app.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	// Create a topic
	app.POST("/topic", func(c *gin.Context) {
		var newTopic Topic
		if err := c.ShouldBindJSON(&newTopic); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		err := ps.CreateTopic(newTopic.Topic)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": err.Error(),
			})
			return
		}
		c.JSON(http.StatusCreated, gin.H{"message": fmt.Sprintf("Created topic: %s", newTopic.Topic)})
	})

	// Delete a topic
	app.DELETE("/topic", func(c *gin.Context) {
		var newTopic Topic
		if err := c.ShouldBindJSON(&newTopic); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		ps.DeleteTopic(newTopic.Topic)
		c.JSON(http.StatusNoContent, nil)
	})

	// Create a subscription to a topic
	app.POST("/subscribe", func(c *gin.Context) {
		var newSubscription Subscription
		if err := c.ShouldBindJSON(&newSubscription); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		err := ps.Subscribe(newSubscription.Topic, newSubscription.Subscription)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": err.Error(),
			})
			return
		}
		c.JSON(http.StatusCreated, newSubscription)
	})

	// Delete a subscription to a topic
	app.DELETE("/unsubscribe", func(c *gin.Context) {
		var newSubscription Subscription
		if err := c.ShouldBindJSON(&newSubscription); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		err := ps.Unsubscribe(newSubscription.Topic, newSubscription.Subscription)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": err.Error(),
			})
			return
		}
		c.JSON(http.StatusNoContent, nil)
	})

	// Publish to all subscriptions of a topic
	app.POST("/publish", func(c *gin.Context) {
		var newMessage Message
		if err := c.ShouldBindJSON(&newMessage); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		ps.Publish(newMessage.Topic, newMessage.Content)
		c.JSON(http.StatusCreated, newMessage)
	})

	// Consume from a subscription
	app.POST("/consume", func(c *gin.Context) {
		var newSubscription Subscription
		if err := c.ShouldBindJSON(&newSubscription); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		msg, err := ps.Consume(newSubscription.Topic, newSubscription.Subscription)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": err.Error(),
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"message": msg,
		})
	})

	// Run the server
	fmt.Println("starting app...")
	app.Run(":8080") // Default port is 8080
}
