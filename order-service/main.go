package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"

	"kafka-go-demo/shared"
)

func main() {
	writer := &kafka.Writer{
		// Kafka is running locally on port 9092.
		Addr: kafka.TCP("localhost:9092"),

		// Send all messages to the "order-events" topic.
		Topic: "order-events",

		// Hash the message key to select a partition.
		// Messages with the same key go to the same partition,
		// preserving their order within that partition.
		Balancer: &kafka.Hash{},
	}
	defer writer.Close()

	// Create multiple orders to publish to Kafka.
	orders := []shared.OrderEvent{
		{OrderID: "order-101", Status: "PLACED", Amount: 500.00},
		{OrderID: "order-102", Status: "PLACED", Amount: 750.00},
		{OrderID: "order-103", Status: "PLACED", Amount: 1200.00},
		{OrderID: "order-104", Status: "PLACED", Amount: 900.00},
	}

	for _, order := range orders {
		jsonData, err := json.Marshal(order)
		if err != nil {
			fmt.Println("JSON conversion failed:", err)
			continue
		}

		err = writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(order.OrderID),
			Value: jsonData,
		})
		if err != nil {
			fmt.Println("Failed to send order:", order.OrderID, err)
			continue
		}

		fmt.Printf("Published order: %s | JSON: %s\n", order.OrderID, string(jsonData))
	}

	fmt.Println("Finished publishing orders.")
}
