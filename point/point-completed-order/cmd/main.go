package main

import (
	"fmt"
	"os"
	"strconv"
	inner "windmill/point/pooint-completed-order"

	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables locally
	loadEnv()

	xmsCatalystDSN := os.Getenv("XMS_CATALYST_VOILA_DSN")
	voilaAccountDSN := os.Getenv("VOILA_ACCOUNT_DSN")

	orderIDStr := os.Getenv("ORDER_ID")
	orderID, _ := strconv.Atoi(orderIDStr)

	// Call the Main function exported by the 'inner' package
	res, err := inner.Main(xmsCatalystDSN, voilaAccountDSN, orderID)
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
