package main

import (
	"fmt"
	"os"

	inner "windmill/order/order-not-sync"

	"github.com/joho/godotenv"
)

func main() {
	loadEnv()

	xmsCatalystDSN := os.Getenv("XMS_CATALYST_VOILA_DSN")
	xmsLegacyDSN := os.Getenv("XMS_LEGACY_DSN")

	res, err := inner.Main(xmsCatalystDSN, xmsLegacyDSN)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if s, ok := res.(string); ok && s != "" {
		fmt.Println(s)
	}
}

func loadEnv() {
	_ = godotenv.Load()
	_ = godotenv.Load("../.env")
	_ = godotenv.Load("../../.env")
	_ = godotenv.Load("../../../.env")
	_ = godotenv.Load("../../../../.env")
}
