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
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "order-events",
		GroupID:  "notification-service-group",
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	defer func() {
		fmt.Println("Closing notification consumer...")
		if err := reader.Close(); err != nil {
			log.Printf("Error closing Kafka reader: %v", err)
		}
	}()

	dlqWriter := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "notification-failed-events",
		Balancer: &kafka.Hash{},
	}
	defer func() {
		if err := dlqWriter.Close(); err != nil {
			log.Printf("Error closing DLQ writer: %v", err)
		}
	}()

	fmt.Println("Notification Service started. Waiting for orders...")

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

		var order shared.OrderEvent

		if err := json.Unmarshal(message.Value, &order); err != nil {
			log.Printf("Invalid order JSON: %v", err)

			if err := publishToDLQ(ctx, dlqWriter, message); err != nil {
				log.Printf("Failed to publish invalid message to DLQ: %v", err)
				continue
			}

			if err := reader.CommitMessages(ctx, message); err != nil {
				log.Printf("Failed to commit invalid message offset: %v", err)
				continue
			}

			log.Println("Invalid message published to DLQ and offset committed")
			continue
		}

		var notifyErr error

		for attempt := 1; attempt <= 3; attempt++ {
			notifyErr = sendNotification(order)

			if notifyErr == nil {
				break
			}

			log.Printf(
				"Attempt %d failed for order %s: %v",
				attempt,
				order.OrderID,
				notifyErr,
			)

			if attempt < 3 {
				delay := time.Duration(1<<(attempt-1)) * time.Second

				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return
				}
			}
		}

		if notifyErr != nil {
			log.Printf(
				"Notification failed after 3 attempts for order %s",
				order.OrderID,
			)

			if err := publishToDLQ(ctx, dlqWriter, message); err != nil {
				log.Printf("Failed to publish order %s to DLQ: %v", order.OrderID, err)
				continue
			}

			log.Printf("Order %s published to notification DLQ", order.OrderID)
		} else {
			log.Printf("Notification processed successfully for order %s", order.OrderID)
		}

		// Commit only after successful processing or successful DLQ publishing.
		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Printf(
				"Failed to commit offset for order %s: %v",
				order.OrderID,
				err,
			)
			continue
		}

		log.Printf("Offset committed for order %s", order.OrderID)
	}

	fmt.Println("Notification Service stopped gracefully.")
}

func publishToDLQ(
	ctx context.Context,
	writer *kafka.Writer,
	message kafka.Message,
) error {
	dlqMessage := kafka.Message{
		Key:   message.Key,
		Value: message.Value,
		Headers: []kafka.Header{
			{
				Key:   "original-topic",
				Value: []byte(message.Topic),
			},
		},
		Time: time.Now(),
	}

	return writer.WriteMessages(ctx, dlqMessage)
}

func sendNotification(order shared.OrderEvent) error {
	// Simulate a failure to test retries and DLQ handling.
	if order.OrderID == "order-104" {
		return fmt.Errorf("temporary notification provider error")
	}

	fmt.Printf(
		"📱 Notification: Order %s is %s. Amount: ₹%.2f\n",
		order.OrderID,
		order.Status,
		order.Amount,
	)

	return nil
}
