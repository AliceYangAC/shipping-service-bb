package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

// StartShippingWorker starts the background listener loop
func StartShippingWorker(service *ShippingService) {
	queueName := os.Getenv("SHIPPING_QUEUE_NAME")
	if queueName == "" {
		log.Fatal("SHIPPING_QUEUE_NAME not set")
	}

	// Initialize Client
	var sbClient *azservicebus.Client
	var err error
	asbConnStr := os.Getenv("ASB_CONNECTION_STRING")

	if asbConnStr != "" {
		sbClient, err = azservicebus.NewClientFromConnectionString(asbConnStr, nil)
	} else {
		hostName := os.Getenv("AZURE_SERVICEBUS_FULLYQUALIFIEDNAMESPACE")
		cred, _ := azidentity.NewDefaultAzureCredential(nil)
		sbClient, err = azservicebus.NewClient(hostName, cred, nil)
	}

	if err != nil {
		log.Fatalf("ASB connection failed: %v", err)
	}

	receiver, err := sbClient.NewReceiverForQueue(queueName, nil)
	if err != nil {
		log.Fatalf("Failed to create receiver: %v", err)
	}

	ctx := context.Background()
	log.Printf("Shipping Worker listening on queue: %s", queueName)

	for {
		// Block until message arrives
		messages, err := receiver.ReceiveMessages(ctx, 1, nil)
		if err != nil {
			log.Printf("Error receiving message: %v. Retrying in 5s...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		for _, msg := range messages {
			processMessage(ctx, service, receiver, msg)
		}
	}
}

func processMessage(ctx context.Context, service *ShippingService, receiver *azservicebus.Receiver, msg *azservicebus.ReceivedMessage) {
	var req ShippingRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		log.Printf("Invalid message format: %v", err)
		receiver.AbandonMessage(ctx, msg, nil)
		return
	}

	log.Printf("Processing shipment for Order %s to %s", req.OrderID, req.Shipping.PostalCode)

	duration := calculateDuration(req.Shipping.PostalCode)
	trackingNum := fmt.Sprintf("TN-%d-%s", rand.Intn(9999), req.OrderID)

	shipmentDetails := ShipmentRecord{
		OrderID:         req.OrderID,
		TrackingNumber:  trackingNum,
		Duration:        duration,
		Destination:     req.Shipping.PostalCode,
		ShippedAt:       time.Now(),
		EstimatedArrive: time.Now().Add(time.Duration(duration) * time.Second),
	}

	// Update Order with Shipment Info and Status=2 (In Transit)
	if err := service.repo.UpdateOrderShipmentInfo(req.OrderID, 2, shipmentDetails); err != nil {
		log.Printf("Failed to update Order %s with shipment info: %v", req.OrderID, err)
		receiver.AbandonMessage(ctx, msg, nil)
		return
	}

	// Complete the message
	receiver.CompleteMessage(ctx, msg, nil)

	// Start delivery simulation in background
	go simulateDelivery(service, req.OrderID, duration, shipmentDetails)
}

func simulateDelivery(service *ShippingService, orderID string, duration int, shipmentDetails ShipmentRecord) {
	log.Printf("Order %s is In Transit (%ds)...", orderID, duration)

	time.Sleep(time.Duration(duration) * time.Second)

	// Update Order status to 3 (Delivered)
	if err := service.repo.UpdateOrderShipmentInfo(orderID, 3, shipmentDetails); err != nil {
		log.Printf("Failed to update status for %s: %v", orderID, err)
	} else {
		log.Printf("Order %s Delivered!", orderID)
	}
}

func calculateDuration(postalCode string) int {
	normalized := strings.ToUpper(strings.ReplaceAll(postalCode, " ", ""))
	if strings.HasPrefix(normalized, "K") {
		return 20 + rand.Intn(11) // 20-30s
	} else if strings.HasPrefix(normalized, "L") || strings.HasPrefix(normalized, "M") {
		return 30 + rand.Intn(11) // 30-40s
	} else {
		return 75 + rand.Intn(26) // 75-100s
	}
}
