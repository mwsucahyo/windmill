package main

import (
	"fmt"
	"os"
	inner "windmill/order/suspicious-order"

	"github.com/joho/godotenv"
)

func main() {
	loadEnv()

	xmsCatalystDSN := os.Getenv("XMS_CATALYST_VOILA_DSN")
	schemaName := os.Getenv("XMS_CATALYST_SCHEMA")
	if schemaName == "" {
		schemaName = "voila"
	}

	res, err := inner.Main(xmsCatalystDSN, schemaName)
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
