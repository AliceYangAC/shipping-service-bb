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
	ordersContainer *azcosmos.ContainerClient
	partitionKey    PartitionKey
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

	// FIX: Pass containerName to the helper
	return createContainerClients(client, dbName, containerName, partitionKey)
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

	// FIX: Pass containerName to the helper
	return createContainerClients(client, dbName, containerName, partitionKey)
}

// added containerName parameter
func createContainerClients(client *azcosmos.Client, dbName string, containerName string, pk PartitionKey) (*CosmosDBServiceRepo, error) {

	// use the passed containerName instead of hardcoded "orders"
	ordersContainer, err := client.NewContainer(dbName, containerName)
	if err != nil {
		return nil, err
	}

	return &CosmosDBServiceRepo{
		ordersContainer: ordersContainer,
		partitionKey:    pk,
	}, nil
}

func (r *CosmosDBServiceRepo) UpdateOrderDelivered(orderID string, status int) error {
	ctx := context.Background()
	pk := azcosmos.NewPartitionKeyString(r.partitionKey.Value)

	// Find the internal Cosmos 'id' using the OrderID
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

	// Prepare the Patch to update only the status
	patch := azcosmos.PatchOperations{}
	patch.AppendReplace("/status", status)

	// Execute the Patch
	_, err := r.ordersContainer.PatchItem(ctx, pk, existingId, patch, nil)
	return err
}

func (r *CosmosDBServiceRepo) UpdateOrderShipmentInfo(orderID string, status int, shipment ShipmentRecord) error {
	ctx := context.Background()
	pk := azcosmos.NewPartitionKeyString(r.partitionKey.Value)

	// Find the internal Cosmos 'id' using the OrderID
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

	// Prepare the shipment object for adding into shipping field
	shipmentBytes, err := json.Marshal(shipment)
	if err != nil {
		return err
	}
	var shipmentJson map[string]interface{}
	if err := json.Unmarshal(shipmentBytes, &shipmentJson); err != nil {
		return err
	}

	// C. Patch the status and embed the full shipment object
	patch := azcosmos.PatchOperations{}

	// 1. Patch the root status field
	patch.AppendReplace("/status", status)

	// Add duration, startAt, and trackingNumber under shipping
	patch.AppendReplace("/shipping/duration", shipmentJson["duration"])
	patch.AppendReplace("/shipping/trackingNumber", shipmentJson["trackingNumber"])
	patch.AppendReplace("/shipping/shippedAt", shipmentJson["shippedAt"])

	// D. Execute the Patch
	_, err = r.ordersContainer.PatchItem(ctx, pk, existingId, patch, nil)
	return err
}
