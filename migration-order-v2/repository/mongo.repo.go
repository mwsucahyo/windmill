package repository

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

type FilterParam struct {
	StartDate    string `bson:"start_date"`
	EndDate      string `bson:"end_date"`
	OrderNumbers string `bson:"order_numbers"`
}

type MigrationLog struct {
	OrderID          int64        `bson:"order_id"`
	OrderNumber      string       `bson:"order_number"`
	Schema           string       `bson:"schema"`
	Case             string       `bson:"case"`
	Status           string       `bson:"status"`
	Action           string       `bson:"action"`
	Detail           string       `bson:"detail"`
	ProcessingMethod string       `bson:"processing_method"`
	FulfillmentIDs   []int64      `bson:"fulfillment_ids"`
	OrderVersion     int          `bson:"order_version"`
	Filter           FilterParam  `bson:"filter"`
	MigratedAt       time.Time    `bson:"migrated_at"`
}

type MongoRepository struct {
	client *mongo.Client
	dbName string
}

func NewMongo(client *mongo.Client, dbName string) *MongoRepository {
	return &MongoRepository{client: client, dbName: dbName}
}

func (r *MongoRepository) SaveMigrationLog(ctx context.Context, log *MigrationLog) error {
	coll := r.client.Database(r.dbName).Collection("migration_order_v2_log")
	_, err := coll.InsertOne(ctx, log)
	if err != nil {
		return fmt.Errorf("insert migration log failed: %w", err)
	}
	return nil
}
