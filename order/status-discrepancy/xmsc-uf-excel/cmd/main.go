package main

import (
	"fmt"
	"os"
	inner "windmill/order/status-discrepancy/xmsc-uf-excel"

	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables locally
	loadEnv()

	xmsCatalystDSN := os.Getenv("XMS_CATALYST_DSN")
	mongoURI := os.Getenv("VOILA_UF_MONGO_URI")

	startDate := "2026-03-01"
	endDate := "2026-03-03"

	// Call the Main function exported by the 'inner' package
	res, err := inner.Main(xmsCatalystDSN, mongoURI, startDate, endDate)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if b, ok := res.([]byte); ok {
		fileName := fmt.Sprintf("discrepancy_%s_%s.xlsx", startDate, endDate)
		err := os.WriteFile(fileName, b, 0644)
		if err != nil {
			fmt.Printf("Error saving file: %v\n", err)
		} else {
			fmt.Printf("Saved to %s\n", fileName)
		}
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
