package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"pubsub-go/internal/services"

	"github.com/gin-gonic/gin"
)

var ps = services.NewPubSub()

func Initialize(queueSize int) {
	ps = services.NewPubSub(queueSize)
}

func HealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "pong",
	})
}

func GetTopics(c *gin.Context) {
	topics := ps.GetTopics()
	c.JSON(http.StatusOK, gin.H{
		"topics": topics,
	})
}

func CreateTopic(c *gin.Context) {
	var newTopic services.Topic
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
	c.JSON(http.StatusCreated, gin.H{"message": fmt.Sprintf("Created topic: %s", strings.TrimSpace(newTopic.Topic))})
}

func DeleteTopic(c *gin.Context) {
	var newTopic services.Topic
	if err := c.ShouldBindJSON(&newTopic); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ps.DeleteTopic(newTopic.Topic)
	c.JSON(http.StatusNoContent, nil)
}

func GetSubscriptions(c *gin.Context) {
	topic := c.Query("topic")
	subscriptions, err := ps.GetSubscriptions(topic)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, subscriptions)
}

func CreateSubscription(c *gin.Context) {
	var newSubscription services.Subscription
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
}

func DeleteSubscription(c *gin.Context) {
	var newSubscription services.Subscription
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
}

func PublishMessage(c *gin.Context) {
	var newMessage services.Message
	if err := c.ShouldBindJSON(&newMessage); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := ps.Publish(newMessage.Topic, newMessage.Content); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, services.ErrSubscriptionsHaveFullQueues) {
			status = http.StatusTooManyRequests
		}
		c.JSON(status, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(http.StatusCreated, newMessage)
}

func StreamMessages(c *gin.Context) {
	topic := c.Query("topic")
	subscription := c.Query("subscription")

	stream, err := ps.SubscriptionStream(topic, subscription)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")
	c.Writer.WriteHeader(http.StatusOK)
	c.Writer.WriteString(": connected\n\n")
	c.Writer.Flush()

	heartbeat := time.NewTicker(30 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-c.Request.Context().Done():
			return
		case <-heartbeat.C:
			c.Writer.WriteString(": keep-alive\n\n")
			c.Writer.Flush()
		case msg, ok := <-stream:
			if !ok {
				payload, _ := json.Marshal(gin.H{
					"message": services.ErrSubscriptionClosed.Error(),
				})
				fmt.Fprintf(c.Writer, "event: error\ndata: %s\n\n", payload)
				c.Writer.Flush()
				return
			}
			payload, _ := json.Marshal(gin.H{
				"topic":        strings.TrimSpace(topic),
				"subscription": strings.TrimSpace(subscription),
				"content":      msg,
			})
			fmt.Fprintf(c.Writer, "event: message\ndata: %s\n\n", payload)
			c.Writer.Flush()
		}
	}
}
