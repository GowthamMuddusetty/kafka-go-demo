package shared

// OrderEvent represents an order event exchanged through Kafka.
type OrderEvent struct {
	OrderID string  `json:"orderId"`
	Status  string  `json:"status"`
	Amount  float64 `json:"amount"`
}
