package main

import (
	"fmt"
	"os"

	inner "windmill/order/order-apg-failed-with-ff"

	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()
	_ = godotenv.Load("../.env")
	_ = godotenv.Load("../../.env")
	_ = godotenv.Load("../../../.env")

	xmsCatalystDSN := os.Getenv("XMS_CATALYST_DSN")

	res, err := inner.Main(xmsCatalystDSN)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if res == nil {
		fmt.Println("No APG failed orders with fulfillments found")
		return
	}

	fmt.Println(res)
}
