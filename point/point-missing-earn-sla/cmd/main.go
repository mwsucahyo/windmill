package main

import (
	"fmt"
	"os"
	inner "windmill/point/point-missing-earn-sla"

	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables locally
	loadEnv()

	xmsCatalystDSN := os.Getenv("XMS_CATALYST_DSN")
	voilaAccountDSN := os.Getenv("VOILA_ACCOUNT_DSN")

	// Call the Main function exported by the 'inner' package
	res, err := inner.Main(xmsCatalystDSN, voilaAccountDSN)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if res != nil {
		fmt.Println(res)
	} else {
		fmt.Println("No discrepancies found.")
	}
}

func loadEnv() {
	_ = godotenv.Load()
	_ = godotenv.Load("../.env")
	_ = godotenv.Load("../../.env")
	_ = godotenv.Load("../../../.env")
}
