package main

import (
	"fmt"
	"os"

	inner "windmill/payment/payment-cash-not-sync"

	"github.com/joho/godotenv"
)

func main() {
	loadEnv()

	xmsCatalystDSN := os.Getenv("XMS_CATALYST_VOILA_DSN")
	xmsLegacyDSN := os.Getenv("XMS_LEGACY_DSN")

	res, err := 	inner.Main(xmsCatalystDSN, xmsLegacyDSN)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if res == nil {
		return
	}
	if m, ok := res.(map[string]string); ok {
		fmt.Println("=== MSG ===")
		fmt.Println(m["msg"])
		fmt.Println("\n=== QUERY ===")
		fmt.Println(m["query"])
	} else {
		fmt.Println(res)
	}
}

func loadEnv() {
	_ = godotenv.Load()
	_ = godotenv.Load("../.env")
	_ = godotenv.Load("../../.env")
	_ = godotenv.Load("../../../.env")
	_ = godotenv.Load("../../../../.env")
}
