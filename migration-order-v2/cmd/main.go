package main

import (
	"fmt"
	"os"
	"strconv"

	inner "windmill/migration-order-v2"

	"github.com/joho/godotenv"
)

func main() {
	loadEnv()

	xmsCatalystDSN := os.Getenv("XMS_CATALYST_DSN")
	if xmsCatalystDSN == "" {
		fmt.Println("XMS_CATALYST_DSN is not set")
		return
	}

	schema := os.Getenv("MIGRATION_SCHEMA")
	if schema == "" {
		fmt.Println("MIGRATION_SCHEMA is not set")
		return
	}

	environment := os.Getenv("MIGRATION_ENVIRONMENT")
	if environment == "" {
		environment = "dev"
	}

	isTesting := os.Getenv("MIGRATION_IS_TESTING") == "true"

	mongoURI := os.Getenv("XMS_CATALYST_MONGO_URI")
	if mongoURI == "" {
		fmt.Println("XMS_CATALYST_MONGO_URI is not set")
		return
	}

	startDate := os.Getenv("MIGRATION_START_DATE")

	endDate := os.Getenv("MIGRATION_END_DATE")

	orderNumbers := os.Getenv("MIGRATION_ORDER_NUMBERS")

	if startDate == "" && endDate == "" && orderNumbers == "" {
		fmt.Println("MIGRATION_START_DATE, MIGRATION_END_DATE, or MIGRATION_ORDER_NUMBERS is not set")
		return
	}

	limit := 0
	if v := os.Getenv("MIGRATION_LIMIT"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			fmt.Printf("MIGRATION_LIMIT is not a valid integer: %v\n", err)
			return
		}
		limit = parsed
	}

	res, err := inner.Main(struct {
		Environment  string `json:"environment"`
		IsTesting    bool   `json:"is_testing"`
		Schema       string `json:"schema"`
		OrderNumbers string `json:"order_numbers"`
		StartDate    string `json:"start_date"`
		EndDate      string `json:"end_date"`
		Limit        int    `json:"limit"`
	}{
		Environment:  environment,
		IsTesting:    isTesting,
		Schema:       schema,
		OrderNumbers: orderNumbers,
		StartDate:    startDate,
		EndDate:      endDate,
		Limit:        limit,
	}, xmsCatalystDSN, mongoURI)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if res == nil {
		fmt.Println("No orders found in the given range")
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
