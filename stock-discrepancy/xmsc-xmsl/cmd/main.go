package main

import (
	"fmt"
	"os"
	inner "windmill/stock-discrepancy/xmsc-xmsl"

	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables locally
	loadEnv()

	xmsCatalystDSN := os.Getenv("XMS_CATALYST_VOILA_DSN")
	xmsLegacyDSN := os.Getenv("XMS_LEGACY_DSN")
	promPushgatewayURL := os.Getenv("PROM_PUSHGATEWAY_URL")

	if xmsCatalystDSN == "" || xmsLegacyDSN == "" {
		fmt.Println("Note: XMS_CATALYST_VOILA_DSN or XMS_LEGACY_DSN missing in environment.")
		return
	}

	// Call the Main function exported by the 'inner' package
	res, err := inner.Main(xmsCatalystDSN, xmsLegacyDSN, promPushgatewayURL)
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
}
