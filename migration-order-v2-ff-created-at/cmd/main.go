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

	limit := 0
	if v := os.Getenv("MIGRATION_LIMIT"); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &limit); err != nil {
			fmt.Printf("MIGRATION_LIMIT is not a number: %q\n", v)
			return
		}
	}

	res, err := inner.Main(struct {
		Environment  string `json:"environment"`
		Schema       string `json:"schema"`
		OrderNumbers string `json:"order_numbers"`
		Case         string `json:"case"`
		Limit        int    `json:"limit"`
	}{
		Environment:  environment,
		Schema:       schema,
		OrderNumbers: orderNumbers,
		Case:         ffCase,
		Limit:        limit,
	}, xmsCatalystDSN, mongoURI)
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
