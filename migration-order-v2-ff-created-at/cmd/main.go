package main

import (
	"fmt"
	"os"
	inner "windmill/migration-order-v2-ff-created-at"

	"github.com/joho/godotenv"
)

func main() {
	loadEnv()

	xmsCatalystDSN := os.Getenv("XMS_CATALYST_DSN")
	if xmsCatalystDSN == "" {
		fmt.Println("XMS_CATALYST_DSN is not set")
		return
	}

	mongoURI := os.Getenv("XMS_CATALYST_MONGO_URI")
	if mongoURI == "" {
		fmt.Println("XMS_CATALYST_MONGO_URI is not set")
		return
	}

	schema := os.Getenv("MIGRATION_SCHEMA")
	if schema == "" {
		fmt.Println("MIGRATION_SCHEMA is not set")
		return
	}

	orderNumbers := os.Getenv("MIGRATION_ORDER_NUMBERS")
	ffCase := os.Getenv("MIGRATION_CASE")
	environment := os.Getenv("MIGRATION_ENVIRONMENT")

	res, err := inner.Main(xmsCatalystDSN, mongoURI, schema, orderNumbers, ffCase, environment)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println(res)
}

func loadEnv() {
	_ = godotenv.Load()
	_ = godotenv.Load("../.env")
	_ = godotenv.Load("../../.env")
	_ = godotenv.Load("../../../.env")
	_ = godotenv.Load("../../../../.env")
}
