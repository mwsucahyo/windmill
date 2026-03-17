package inner

import (
	"fmt"
	"strings"
	"time"

	windmill "github.com/windmill-labs/windmill-go-client"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// require github.com/windmill-labs/windmill-go-client v1.655.0
// require gorm.io/driver/postgres v1.6.0
// require gorm.io/gorm v1.31.1

const (
	DefaultShipmentResource = "u/sucahyo/voila_shipment_postgresql_stg"
	DefaultLegacyResource   = "u/mirza/voila_postgresql_stg"
)

// Main is exported for local runner
func Main(shipmentDSN, legacyDSN string) (interface{}, error) {
	return main(shipmentDSN, legacyDSN)
}

// main is the entry point for Windmill
func main(shipmentDSN, legacyDSN string) (interface{}, error) {
	// 1. Resolve Credentials
	dsnShip := resolveDSN(shipmentDSN, DefaultShipmentResource)
	dsnLeg := resolveDSN(legacyDSN, DefaultLegacyResource)

	if dsnShip == "" || dsnLeg == "" {
		return nil, fmt.Errorf("could not resolve database credentials")
	}

	// 2. Connect to Databases
	dbShip, err := gorm.Open(postgres.Open(dsnShip), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Shipment DB: %w", err)
	}

	dbLeg, err := gorm.Open(postgres.Open(dsnLeg), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Legacy DB: %w", err)
	}

	// 3. Determine status based on date (Cutoff: 21st)
	loc := time.FixedZone("WIB", 7*3600)
	now := time.Now().In(loc)

	targetIsDeleted := 0
	if now.Day() < 21 {
		targetIsDeleted = 1
	}

	// 4. Update Both Databases
	// Shipment DB
	resShip := dbShip.Exec("UPDATE public.courier SET is_deleted = ? WHERE id IN (15, 16)", targetIsDeleted)
	if resShip.Error != nil {
		return nil, fmt.Errorf("failed to update Shipment courier: %w", resShip.Error)
	}

	// Legacy DB
	resLeg := dbLeg.Exec("UPDATE public.ms_courier SET is_deleted = ? WHERE id IN (15, 16)", targetIsDeleted)
	if resLeg.Error != nil {
		return nil, fmt.Errorf("failed to update Legacy courier: %w", resLeg.Error)
	}

	statusLabel := "OFF"
	if targetIsDeleted == 0 {
		statusLabel = "ON"
	}

	// 5. Return Formatted Message for Windmill Flow
	msg := fmt.Sprintf("### 🚚 Courier Status Update (Date-based)\n"+
		"**New Status**: `%s` (is_deleted = %d)\n"+
		"**Time**: `%s` WIB\n"+
		"**Condition**: Day < 21 -> OFF (1), Day >= 21 -> ON (0)\n"+
		"**Couriers**: Instant & Sameday (IDs: 15, 16)\n\n"+
		"**Databases Updated**:\n"+
		"- Voila Shipment (Courier UF)\n"+
		"- XMS Legacy (Courier POS)",
		statusLabel, targetIsDeleted, now.Format("2006-01-02 15:04:05"))

	return msg, nil
}

// resolveDSN helper to handle both direct DSN and Windmill resource paths
func resolveDSN(provided, resourcePath string) string {
	if provided != "" && !strings.HasPrefix(provided, "u/") && !strings.HasPrefix(provided, "f/") {
		return provided
	}

	path := resourcePath
	if provided != "" {
		path = provided
	}

	res, err := windmill.GetResource(path)
	if err != nil {
		fmt.Printf("Warning: failed to get resource %s: %v\n", path, err)
		return ""
	}

	m, ok := res.(map[string]interface{})
	if !ok {
		return ""
	}

	// Return dsn if available
	if dsn, ok := m["dsn"].(string); ok && dsn != "" {
		return dsn
	}

	// Fallback to building DSN from parts
	host := m["host"]
	user := m["user"]
	password := m["password"]
	dbname := m["dbname"]
	port := m["port"]

	if port == nil {
		port = 5432
	}

	return fmt.Sprintf("host=%v user=%v password=%v dbname=%v port=%v sslmode=disable",
		host, user, password, dbname, port)
}
