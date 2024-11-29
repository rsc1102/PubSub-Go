package handlers

import (
	"fmt"
	"net/http"

	"pubsub-go/internal/services"

	"github.com/gin-gonic/gin"
)

var ps = services.NewPubSub()

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
	c.JSON(http.StatusCreated, gin.H{"message": fmt.Sprintf("Created topic: %s", newTopic.Topic)})
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
	ps.Publish(newMessage.Topic, newMessage.Content)
	c.JSON(http.StatusCreated, newMessage)
}

func ConsumeMessage(c *gin.Context) {
	var newSubscription services.Subscription
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
}
