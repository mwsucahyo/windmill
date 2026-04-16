package main

import (
	"fmt"
	"os"
	inner "windmill/order/status-discrepancy/xmsc-uf"

	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables locally
	loadEnv()

	xmsCatalystDSN := os.Getenv("XMS_CATALYST_VOILA_DSN")
	mongoURI := os.Getenv("VOILA_UF_MONGO_URI")

	// Call the Main function exported by the 'inner' package
	res, err := inner.Main(xmsCatalystDSN, mongoURI)
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
