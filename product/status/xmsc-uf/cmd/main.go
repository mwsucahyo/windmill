package main

import (
	"fmt"
	"os"
	inner "windmill/product/status/xmsc-uf"

	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables locally
	loadEnv()

	xmsCatalystDSN := os.Getenv("XMS_CATALYST_DSN")
	esURL := os.Getenv("ES_URL")

	// Call the Main function exported by the 'inner' package
	res, err := inner.Main(xmsCatalystDSN, esURL)
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
