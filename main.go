package main

import (
	"flag"
	"fmt"
	"log"

	"pubsub-go/api/handlers"

	"github.com/gin-gonic/gin"
)

func main() {
	queueSize := flag.Int("queue-size", 10, "queue capacity per subscription")
	flag.Parse()

	if *queueSize <= 0 {
		log.Fatal("queue-size must be greater than 0")
	}

	handlers.Initialize(*queueSize)

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
	fmt.Printf("starting app with queue size %d...\n", *queueSize)
	app.Run(":8080") // Default port is 8080
}
