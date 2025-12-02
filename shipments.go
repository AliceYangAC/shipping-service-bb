package main

import (
	"time"
)

// Request coming from Store Admin via HTTP
type ShippingRequest struct {
	OrderID  string       `json:"orderId"`
	Shipping ShippingInfo `json:"shipping"`
	Status   string       `json:"status"`
}

type ShippingInfo struct {
	Address1   string `json:"address1"`
	City       string `json:"city"`
	Province   string `json:"province"`
	PostalCode string `json:"postalCode"`
	Country    string `json:"country"`
}

// Record stored in MongoDB
type ShipmentRecord struct {
	OrderID         string    `bson:"orderId"`
	TrackingNumber  string    `bson:"trackingNumber"`
	Duration        int       `bson:"durationSeconds"`
	Destination     string    `bson:"destination"`
	ShippedAt       time.Time `bson:"shippedAt"`
	EstimatedArrive time.Time `bson:"estimatedArrive"`
}

// Repository Interface
type ShippingRepo interface {
	InsertShipment(record ShipmentRecord) error
	UpdateOrderStatus(orderID string, status int) error
}

type ShippingService struct {
	repo ShippingRepo
}

func NewShippingService(repo ShippingRepo) *ShippingService {
	return &ShippingService{repo}
}
