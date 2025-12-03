package main

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBShippingRepo struct {
	client           *mongo.Client
	dbName           string
	ordersCollection *mongo.Collection
}

func NewMongoDBShippingRepo(mongoUri string, mongoDb string, mongoCollection string, mongoUser string, mongoPassword string) (*MongoDBShippingRepo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Connect to Client
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoUri))
	if err != nil {
		return nil, err
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}
	log.Println("Connected to MongoDB")

	// Initialize the Collection Client (Orders is the target for updates)
	db := client.Database(mongoDb)
	ordersColl := db.Collection("orders")

	return &MongoDBShippingRepo{
		client:           client,
		dbName:           mongoDb,
		ordersCollection: ordersColl,
	}, nil
}

// UpdateOrderShipmentInfo is the comprehensive update method (Status 2 and Status 3)
func (r *MongoDBShippingRepo) UpdateOrderShipmentInfo(orderID string, status int, shipment ShipmentRecord) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.M{"orderId": orderID}

	update := bson.M{
		"$set": bson.M{
			"status":   status,
			"shipment": shipment, // Embeds the entire ShipmentRecord struct
		},
	}

	_, err := r.ordersCollection.UpdateOne(ctx, filter, update)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			log.Printf("Order %s not found for update.", orderID)
			return nil
		}
		return err
	}
	return nil
}
