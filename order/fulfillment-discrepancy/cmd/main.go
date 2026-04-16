package main

import (
	"fmt"
	"os"
	inner "windmill/order/fulfillment-discrepancy"

	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables locally
	loadEnv()

	xmsCatalystDSN := os.Getenv("XMS_CATALYST_VOILA_DSN")
	promPushgatewayURL := os.Getenv("PROM_PUSHGATEWAY_URL")

	// Call the Main function exported by the 'inner' package
	res, err := inner.Main(xmsCatalystDSN, promPushgatewayURL)
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
