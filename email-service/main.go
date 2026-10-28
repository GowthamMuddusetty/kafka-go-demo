package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"kafka-go-demo/shared"
)

func main() {
	// Detect Ctrl+C and termination signals.
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// Join the email service's consumer group.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "order-events",
		GroupID:  "email-service-group",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing reader: %v", err)
		}
		fmt.Println("Kafka reader closed.")
	}()

	dlqWriter := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "email-failed-events",
		Balancer: &kafka.Hash{},
	}
	defer dlqWriter.Close()

	sentEmails := make(map[string]bool)

	fmt.Println("Email service started. Waiting for orders...")

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				fmt.Println("Shutdown signal received.")
				break
			}
			log.Printf("Error reading message: %v", err)
			continue
		}

		// Convert the JSON message into our shared Go structure.
		var order shared.OrderEvent
		if err := json.Unmarshal(message.Value, &order); err != nil {
			log.Printf("Invalid order JSON: %v", err)

			dlqErr := dlqWriter.WriteMessages(ctx, kafka.Message{
				Key:   message.Key,
				Value: message.Value,
			})

			if dlqErr != nil {
				log.Printf("Failed to publish invalid message to DLQ: %v", dlqErr)
				continue
			}

			if err := reader.CommitMessages(ctx, message); err != nil {
				log.Printf("Failed to commit invalid message offset: %v", err)
				continue
			}

			continue
		}

		if sentEmails[order.OrderID] {
			log.Printf("Email already sent for order %s; skipping", order.OrderID)
		} else {
			var sendErr error

			// Retry sending the email up to 3 times.
			for attempt := 1; attempt <= 3; attempt++ {
				sendErr = sendEmail(order)
				if sendErr == nil {
					sentEmails[order.OrderID] = true
					break
				}

				log.Printf(
					"Attempt %d failed for order %s: %v",
					attempt, order.OrderID, sendErr,
				)

				if attempt < 3 {
					delay := time.Duration(1<<(attempt-1)) * time.Second
					time.Sleep(delay)
				}
			}

			if sendErr != nil {
				dlqErr := dlqWriter.WriteMessages(ctx, kafka.Message{
					Key:   []byte(order.OrderID),
					Value: message.Value,
				})

				if dlqErr != nil {
					log.Printf("Failed to publish order %s to DLQ: %v", order.OrderID, dlqErr)
					continue
				}

				log.Printf("Order %s sent to DLQ", order.OrderID)
			}
		}

		// Commit only after successful email processing or DLQ publishing.
		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Printf("Failed to commit order %s: %v", order.OrderID, err)
			continue
		}

		log.Printf("Offset committed for order %s", order.OrderID)
	}

	fmt.Println("Consumer stopped gracefully.")
}

func sendEmail(order shared.OrderEvent) error {
	// Simulate a temporary email failure for this order.
	if order.OrderID == "order-104" {
		return fmt.Errorf("temporary email provider error")
	}

	fmt.Printf("Email sent for order %s\n", order.OrderID)
	return nil
}
