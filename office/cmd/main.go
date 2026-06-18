package main

import (
	"fmt"
	"os"
	inner "windmill/office"

	"github.com/joho/godotenv"
)

func main() {
	loadEnv()

	xmsCatalystDSN := os.Getenv("XMS_CATALYST_VOILA_DSN")
	xmsLegacyDSN := os.Getenv("XMS_LEGACY_DSN")
	mongoURI := os.Getenv("VOILA_UF_MONGO_URI")

	if xmsCatalystDSN == "" || xmsLegacyDSN == "" || mongoURI == "" {
		fmt.Println("Note: XMS_CATALYST_VOILA_DSN, XMS_LEGACY_DSN, or VOILA_UF_MONGO_URI missing in environment.")
		return
	}

	res, err := inner.Main(xmsCatalystDSN, xmsLegacyDSN, mongoURI)
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
