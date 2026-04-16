package inner

import (
	"fmt"
	"strings"

	wmill "github.com/windmill-labs/windmill-go-client"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// --- Constants ---

const (
	DefaultUamResource       = "u/sucahyo/catalyst_uam_postgresql_prod"
	DefaultVoilaResource     = "u/mirza/catalyst_xms_postgresql_voila_prod"
	DefaultJamtanganResource = "u/mirza/catalyst_xms_postgresql_jt_prod"
)

// --- Main Entry ---

func Main(xmsCatalystUamDSN, xmsCatalystVoilaDSN, xmsCatalystJamtanganDSN, email string) (interface{}, error) {
	if email == "" {
		return nil, fmt.Errorf("email parameter is required")
	}

	// 1. Resolve Credentials
	fmt.Println("Resolving DSNs...")
	resolvedUamDSN := resolveDSN(xmsCatalystUamDSN, DefaultUamResource)
	resolvedVoilaDSN := resolveDSN(xmsCatalystVoilaDSN, DefaultVoilaResource)
	resolvedJamtanganDSN := resolveDSN(xmsCatalystJamtanganDSN, DefaultJamtanganResource)

	type DBContext struct {
		Name      string
		DSN       string
		TableName string
	}

	targets := []DBContext{
		{Name: "UAM", DSN: resolvedUamDSN, TableName: `"user"`},
		{Name: "VOILA", DSN: resolvedVoilaDSN, TableName: "ms_user"},
		{Name: "JAMTANGAN", DSN: resolvedJamtanganDSN, TableName: "ms_user"},
	}

	// 2. Connect to Databases & Update
	successCount := 0

	for _, target := range targets {
		if target.DSN == "" {
			fmt.Printf("[ERROR] [%s] Database connection string (DSN) is empty.\n", target.Name)
			continue
		}

		db, err := connectDB(target.DSN)
		if err != nil {
			fmt.Printf("[ERROR] [%s] Failed to establish database connection: %v\n", target.Name, err)
			continue
		}

		// Execute update
		query := fmt.Sprintf(`UPDATE %s SET status = 'INACTIVE' WHERE email = ?`, target.TableName)
		res := db.Exec(query, email)

		if res.Error != nil {
			fmt.Printf("[ERROR] [%s] Failed to execute update query: %v\n", target.Name, res.Error)
		} else {
			if res.RowsAffected > 0 {
				fmt.Printf("[INFO] [%s] Successfully updated user status to INACTIVE. Affected rows: %d\n", target.Name, res.RowsAffected)
				successCount++
			} else {
				fmt.Printf("[INFO] [%s] User not found\n", target.Name)
			}
		}
	}

	if successCount > 0 {
		return "XSMC: Account deactivated :white_check_mark:", nil
	}
	return "XSMC: Account not found", nil
}

// --- Helper Functions ---

func resolveDSN(provided, resourcePath string) string {
	if strings.HasPrefix(provided, "postgres://") || strings.HasPrefix(provided, "host=") {
		return provided
	}

	// Try resolving provided param if it looks like a resource path
	if provided != "" && strings.HasPrefix(provided, "u/") {
		res, err := wmill.GetResource(provided)
		if err == nil {
			return parseResource(res, provided)
		}
	}

	// Fallback to default resource path
	res, err := wmill.GetResource(resourcePath)
	if err != nil {
		return provided
	}

	return parseResource(res, provided)
}

func parseResource(res interface{}, fallback string) string {
	m, ok := res.(map[string]interface{})
	if !ok {
		return fallback
	}

	if dsn, ok := m["dsn"].(string); ok && dsn != "" {
		return dsn
	}

	return fmt.Sprintf("host=%v user=%v password=%v dbname=%v port=%v",
		m["host"], m["user"], m["password"], m["dbname"], m["port"])
}

func connectDB(dsn string) (*gorm.DB, error) {
	config := &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: ""},
	}
	return gorm.Open(postgres.Open(dsn), config)
}
