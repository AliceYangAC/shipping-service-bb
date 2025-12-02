package main

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBShippingRepo struct {
	client *mongo.Client
	dbName string
}

func NewMongoDBShippingRepo(mongoUri string, mongoDb string, mongoCollection string, mongoUser string, mongoPassword string) (*MongoDBShippingRepo, error) {
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoUri))
	if err != nil {
		return nil, err
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}
	log.Println("Connected to MongoDB")

	return &MongoDBShippingRepo{
		client: client,
		dbName: "orderdb", // Using the same DB as Order/Makeline services
	}, nil
}

func (r *MongoDBShippingRepo) InsertShipment(record ShipmentRecord) error {
	coll := r.client.Database(r.dbName).Collection("shipments")
	_, err := coll.InsertOne(context.Background(), record)
	return err
}

func (r *MongoDBShippingRepo) UpdateOrderStatus(orderID string, status int, duration int) error {
	coll := r.client.Database(r.dbName).Collection("orders")
	filter := bson.M{"orderid": orderID}
	update := bson.M{"$set": bson.M{"status": status, "duration": duration}}

	_, err := coll.UpdateOne(context.Background(), filter, update)
	return err
}
