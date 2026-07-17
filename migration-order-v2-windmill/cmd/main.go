package main

import (
	"fmt"
	"os"

	inner "windmill/migration-order-v2-windmill"

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

	res, err := inner.Main(xmsCatalystDSN, schema, startDate, endDate, orderNumbers, mongoURI)
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
