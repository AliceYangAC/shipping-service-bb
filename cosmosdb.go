package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

type PartitionKey struct {
	Key   string
	Value string
}

type CosmosDBServiceRepo struct {
	// We need clients for BOTH containers
	ordersContainer    *azcosmos.ContainerClient
	shipmentsContainer *azcosmos.ContainerClient
	partitionKey       PartitionKey
}

func NewCosmosDBServiceRepoWithManagedIdentity(cosmosDbEndpoint string, dbName string, containerName string, partitionKey PartitionKey) (*CosmosDBServiceRepo, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Printf("failed to create cosmosdb workload identity credential: %v\n", err)
		return nil, err
	}

	client, err := azcosmos.NewClient(cosmosDbEndpoint, cred, nil)
	if err != nil {
		log.Printf("failed to create cosmosdb client: %v\n", err)
		return nil, err
	}

	return createContainerClients(client, dbName, partitionKey)
}

func NewCosmosDBServiceRepo(cosmosDbEndpoint string, dbName string, containerName string, cosmosDbKey string, partitionKey PartitionKey) (*CosmosDBServiceRepo, error) {
	cred, err := azcosmos.NewKeyCredential(cosmosDbKey)
	if err != nil {
		log.Printf("failed to create cosmosdb key credential: %v\n", err)
		return nil, err
	}

	client, err := azcosmos.NewClientWithKey(cosmosDbEndpoint, cred, nil)
	if err != nil {
		log.Printf("failed to create cosmosdb client: %v\n", err)
		return nil, err
	}

	return createContainerClients(client, dbName, partitionKey)
}

// Helper to initialize both container clients
func createContainerClients(client *azcosmos.Client, dbName string, pk PartitionKey) (*CosmosDBServiceRepo, error) {
	// Hardcode or fetch env vars for the specific container names
	// Assuming 'orders' and 'shipments' are the names
	ordersContainer, err := client.NewContainer(dbName, "orders")
	if err != nil {
		return nil, err
	}

	shipmentsContainer, err := client.NewContainer(dbName, "shipments")
	if err != nil {
		return nil, err
	}

	return &CosmosDBServiceRepo{
		ordersContainer:    ordersContainer,
		shipmentsContainer: shipmentsContainer,
		partitionKey:       pk,
	}, nil
}

// --- INTERFACE IMPLEMENTATION ---

// 1. InsertShipment
func (r *CosmosDBServiceRepo) InsertShipment(record ShipmentRecord) error {
	ctx := context.Background()

	// Ensure Partition Key is set if needed by your schema
	// For shipments, we usually use OrderID or a specific location as PK
	// For this example, we assume the record struct has the necessary fields.

	marshalled, err := json.Marshal(record)
	if err != nil {
		return err
	}

	// Use the partition key passed in config, or derive from record
	pk := azcosmos.NewPartitionKeyString(r.partitionKey.Value)

	_, err = r.shipmentsContainer.CreateItem(ctx, pk, marshalled, nil)
	return err
}

// 2. UpdateOrderStatus
func (r *CosmosDBServiceRepo) UpdateOrderStatus(orderID string, status int) error {
	ctx := context.Background()
	pk := azcosmos.NewPartitionKeyString(r.partitionKey.Value)

	// A. Find the internal Cosmos 'id' using the OrderID
	var existingId string
	query := "SELECT * FROM o WHERE o.orderId = @orderId"
	opt := &azcosmos.QueryOptions{
		QueryParameters: []azcosmos.QueryParameter{
			{Name: "@orderId", Value: orderID},
		},
	}

	pager := r.ordersContainer.NewQueryItemsPager(query, pk, opt)

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			log.Printf("Query error: %v", err)
			return err
		}
		for _, bytes := range resp.Items {
			var doc map[string]interface{}
			if err := json.Unmarshal(bytes, &doc); err == nil {
				existingId = doc["id"].(string)
				break
			}
		}
		if existingId != "" {
			break
		}
	}

	if existingId == "" {
		log.Printf("Order %s not found in CosmosDB", orderID)
		return nil
	}

	// B. Patch the status
	patch := azcosmos.PatchOperations{}
	patch.AppendReplace("/status", status)

	_, err := r.ordersContainer.PatchItem(ctx, pk, existingId, patch, nil)
	return err
}
