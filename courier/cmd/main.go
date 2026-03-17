package main

import (
	"fmt"
	"os"
	inner "windmill/courier"

	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables locally
	loadEnv()

	// Use a specific DSN if available, or let it fallback to default resource (which will fail locally without credentials)
	shipmentDSN := os.Getenv("VOILA_SHIPMENT_DSN")
	legacyDSN := os.Getenv("XMS_LEGACY_DSN")

	// Call the Main function exported by the 'inner' package
	res, err := inner.Main(shipmentDSN, legacyDSN)
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
}
