package main

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	inner "windmill/user/inactive-user"
)

func main() {
	// Let's rely on .env from the project root
	err := godotenv.Load("../../.env")
	if err != nil {
		log.Println("No .env file found, relying on environment variables")
	}

	uamDSN := os.Getenv("XMS_CATALYST_UAM_DSN")
	voilaDSN := os.Getenv("XMS_CATALYST_VOILA_DSN")
	jamDSN := os.Getenv("XMS_CATALYST_JAMTANGAN_DSN")
	
	email := os.Getenv("TARGET_EMAIL")
	if email == "" {
		email = "test@example.com"
	}

	fmt.Printf("Starting inactive user update for email: %s\n", email)

	res, err := inner.Main(uamDSN, voilaDSN, jamDSN, email)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	fmt.Println(res)
}
