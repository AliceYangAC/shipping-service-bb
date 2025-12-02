package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// Valid database API types
const (
	AZURE_COSMOS_DB_SQL_API = "cosmosdbsql"
)

func main() {
	var shippingService *ShippingService

	apiType := os.Getenv("ORDER_DB_API")
	switch apiType {
	case "cosmosdbsql":
		log.Printf("Using Azure CosmosDB SQL API")
	default:
		log.Printf("Using MongoDB API")
	}

	shippingService, err := initDatabase(apiType)
	if err != nil {
		log.Printf("Failed to initialize database: %s", err)
		os.Exit(1)
	}

	// start background shipping worker
	go StartShippingWorker(shippingService)

	// 3. Start HTTP Server
	r := gin.Default()
	r.Use(cors.Default())
	r.POST("/", func(c *gin.Context) {
		handleShipRequest(c)
	})

	log.Printf("Shipping Service running on :3003")
	r.Run(":3003")
}

func handleShipRequest(c *gin.Context) {
	var req ShippingRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "Invalid payload"})
		return
	}

	// Setup temporary sender connection
	asbConnStr := os.Getenv("ASB_CONNECTION_STRING")
	client, _ := azservicebus.NewClientFromConnectionString(asbConnStr, nil)
	sender, _ := client.NewSender(os.Getenv("SHIPPING_QUEUE_NAME"), nil)
	defer client.Close(c)
	defer sender.Close(c)

	body, _ := json.Marshal(req)
	err := sender.SendMessage(c, &azservicebus.Message{Body: body}, nil)

	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to enqueue"})
		return
	}

	// Note: We don't update DB here, the worker will do it when it picks up the msg
	c.JSON(202, gin.H{"status": "Queued for shipping"})
}

// Gets an environment variable or exits if it is not set
func getEnvVar(varName string, fallbackVarNames ...string) string {
	value := os.Getenv(varName)
	if value == "" {
		for _, fallbackVarName := range fallbackVarNames {
			value = os.Getenv(fallbackVarName)
			if value == "" {
				break
			}
		}
		if value == "" {
			log.Printf("%s is not set", varName)
			if len(fallbackVarNames) > 0 {
				log.Printf("Tried fallback variables: %v", fallbackVarNames)
			}
			os.Exit(1)
		}
	}
	return value
}

// Initializes the database based on the API type
func initDatabase(apiType string) (*ShippingService, error) {
	dbURI := getEnvVar("AZURE_COSMOS_RESOURCEENDPOINT", "SHIPPING_DB_URI")
	dbName := getEnvVar("SHIPPING_DB_NAME")

	switch apiType {
	case AZURE_COSMOS_DB_SQL_API:
		containerName := getEnvVar("SHIPPING_DB_CONTAINER_NAME")
		dbPartitionKey := getEnvVar("SHIPPING_DB_PARTITION_KEY")
		dbPartitionValue := getEnvVar("SHIPPING_DB_PARTITION_VALUE")

		// check if USE_WORKLOAD_IDENTITY_AUTH is set
		useWorkloadIdentityAuth := os.Getenv("USE_WORKLOAD_IDENTITY_AUTH")
		if useWorkloadIdentityAuth == "" {
			useWorkloadIdentityAuth = "false"
		}

		if useWorkloadIdentityAuth == "true" {
			cosmosRepo, err := NewCosmosDBServiceRepoWithManagedIdentity(dbURI, dbName, containerName, PartitionKey{dbPartitionKey, dbPartitionValue})
			if err != nil {
				return nil, err
			}
			return NewShippingService(cosmosRepo), nil
		} else {
			dbPassword := os.Getenv("SHIPPING_DB_PASSWORD")
			cosmosRepo, err := NewCosmosDBServiceRepo(dbURI, dbName, containerName, dbPassword, PartitionKey{dbPartitionKey, dbPartitionValue})
			if err != nil {
				return nil, err
			}
			return NewShippingService(cosmosRepo), nil
		}
	default:
		collectionName := getEnvVar("SHIPPING_DB_COLLECTION_NAME")
		dbUsername := os.Getenv("SHIPPING_DB_USERNAME")
		dbPassword := os.Getenv("SHIPPING_DB_PASSWORD")
		mongoRepo, err := NewMongoDBShippingRepo(dbURI, dbName, collectionName, dbUsername, dbPassword)
		if err != nil {
			return nil, err
		}
		return NewShippingService(mongoRepo), nil
	}
}
