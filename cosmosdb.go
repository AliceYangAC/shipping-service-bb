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

	ordersContainer, err := client.NewContainer(dbName, "orders")
	if err != nil {
		return nil, err
	}

	return &CosmosDBServiceRepo{
		ordersContainer: ordersContainer,
		partitionKey:    pk,
	}, nil
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

	// B. Prepare the Shipment Record for Embedding
	// We must marshal the struct to JSON and then unmarshal it into a map[string]interface{}
	// before patching, as the azcosmos Patch API expects a valid JSON value.
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

	// 2. Embed the entire shipment object under the 'shipment' key
	patch.AppendReplace("/shipment", shipmentJson)

	// D. Execute the Patch
	_, err = r.ordersContainer.PatchItem(ctx, pk, existingId, patch, nil)
	return err
}
