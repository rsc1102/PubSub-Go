package main

import (
	"fmt"

	"pubsub-go/api/handlers"

	"github.com/gin-gonic/gin"
)

func main() {

	app := gin.Default()

	app.GET("/ping", handlers.HealthCheck)
	app.POST("/topics", handlers.CreateTopic)
	app.DELETE("/topics", handlers.DeleteTopic)
	app.GET("/topics", handlers.GetTopics)
	app.POST("/subscriptions", handlers.CreateSubscription)
	app.DELETE("/subscriptions", handlers.DeleteSubscription)
	app.GET("/subscriptions", handlers.GetSubscriptions)
	app.POST("/publish", handlers.PublishMessage)
	app.POST("/consume", handlers.ConsumeMessage)

	// Run the server
	fmt.Println("starting app...")
	app.Run(":8080") // Default port is 8080
}
