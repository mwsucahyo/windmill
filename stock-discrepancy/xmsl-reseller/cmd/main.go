package main

import (
	"fmt"
	"os"
	inner "windmill/stock-discrepancy/xmsl-reseller"

	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables locally
	loadEnv()

	resellerDSN := os.Getenv("RESELLER_DSN")
	voilaDSN := os.Getenv("XMS_LEGACY_DSN")

	if resellerDSN == "" || voilaDSN == "" {
		fmt.Println("Note: RESELLER_DSN or XMS_LEGACY_DSN missing in environment.")
		return
	}

	// Call the Main function exported by the 'inner' package
	res, err := inner.Main(resellerDSN, voilaDSN)
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
