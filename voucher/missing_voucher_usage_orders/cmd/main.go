package main

import (
	"fmt"
	"os"
	inner "windmill/voucher/missing_voucher_usage_orders"

	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables locally
	loadEnv()

	xmsCatalystDSN := os.Getenv("XMS_CATALYST_VOILA_DSN")
	voilaVoucherDSN := os.Getenv("VOILA_VOUCHER_DSN")

	// Call the Main function exported by the 'inner' package
	res, err := inner.Main(xmsCatalystDSN, voilaVoucherDSN)
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
