package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/joho/godotenv"
	windmill "github.com/windmill-labs/windmill-go-client"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

const (
	RESELLER_RESOURCE_PATH = "u/mirza/catalyst_xms_postgresql_voila_stg"
	VOILA_RESOURCE_PATH    = "u/mirza/voila_postgresql_stg"
)

// ComparisonResult represents the result of the stock check
type ComparisonResult struct {
	ID          int `gorm:"column:id"`
	ResellerQty int `gorm:"column:reseller_qty"`
	VoilaQty    int `gorm:"column:voila_qty"`
	Difference  int `gorm:"column:difference"`
}

// Main is the entry point for Windmill
func Main(RESELLER_DSN string, XMS_LEGACY_DSN string) string {
	// Defaults if not provided via parameters
	if RESELLER_DSN == "" {
		res, err := windmill.GetResource(RESELLER_RESOURCE_PATH)
		if err == nil {
			if dsn, ok := res.(map[string]interface{})["dsn"].(string); ok {
				RESELLER_DSN = dsn
			}
		}
	}

	if RESELLER_DSN == "" {
		return "Error: RESELLER_DSN is missing. Please provide as parameter or check Windmill resource."
	}

	var dblinkConn string

	// If XMS_LEGACY_DSN is provided (local testing), use it to build dblink connection
	if strings.HasPrefix(XMS_LEGACY_DSN, "postgres://") {
		// Clean up DSN and extract parts for dblink
		// format: postgres://user:pass@host:port/dbname
		trimmed := strings.TrimPrefix(XMS_LEGACY_DSN, "postgres://")

		// Handle user:pass@host...
		parts := strings.SplitN(trimmed, "@", 2)
		if len(parts) == 2 {
			credentials := strings.SplitN(parts[0], ":", 2)
			if len(credentials) == 2 {
				dblinkConn += fmt.Sprintf("user=%s password=%s ", credentials[0], credentials[1])
			}
			trimmed = parts[1]
		}

		// Handle host:port/dbname...
		parts = strings.SplitN(trimmed, "/", 2)
		if len(parts) == 2 {
			dblinkConn += fmt.Sprintf("dbname=%s ", parts[1])
			hostPort := strings.SplitN(parts[0], ":", 2)
			dblinkConn += fmt.Sprintf("host=%s ", hostPort[0])
			if len(hostPort) == 2 {
				dblinkConn += fmt.Sprintf("port=%s ", hostPort[1])
			}
		}
		dblinkConn = strings.TrimSpace(dblinkConn)
	} else {
		// Fetch Voila credentials from Windmill resource
		resourcePath := XMS_LEGACY_DSN
		if resourcePath == "" {
			resourcePath = VOILA_RESOURCE_PATH
		}

		voilaRes, err := windmill.GetResource(resourcePath)
		if err != nil {
			return fmt.Sprintf("Error fetching Voila resource '%s': %v", resourcePath, err)
		}

		voilaMap, ok := voilaRes.(map[string]interface{})
		if !ok {
			return "Error: Voila resource is not a valid map."
		}

		voilaHost := voilaMap["host"]
		voilaUser := voilaMap["user"]
		voilaPass := voilaMap["password"]
		voilaDB := voilaMap["dbname"]
		voilaPort := voilaMap["port"]

		if voilaHost == nil || voilaUser == nil || voilaPass == nil || voilaDB == nil {
			return "Error: Voila resource missing required fields (host, user, password, dbname)."
		}

		dblinkConn = fmt.Sprintf("host=%v user=%v password=%v dbname=%v", voilaHost, voilaUser, voilaPass, voilaDB)
		if voilaPort != nil {
			dblinkConn = fmt.Sprintf("%s port=%v", dblinkConn, voilaPort)
		}
	}

	// Connect to Reseller DB
	db, err := gorm.Open(postgres.Open(RESELLER_DSN), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			TablePrefix: "",
		},
	})
	if err != nil {
		return fmt.Sprintf("Error connecting to Reseller DB: %v", err)
	}

	// Prepare the query using the user's template
	query := fmt.Sprintf(`
		SELECT 
			pv.id,
			pv.qty_available AS reseller_qty,
			COALESCE(pvs.total_stock, 0) AS voila_qty,
			(pv.qty_available - COALESCE(pvs.total_stock, 0)) AS difference
		FROM 
			ms_product_variant pv
		LEFT JOIN dblink(
			'%s',
			$$
			SELECT 
				variant_id,
				SUM(qty_available) AS total_stock
			FROM ms_product_variant_stock
			GROUP BY variant_id
			$$
		) AS pvs(variant_id INT, total_stock NUMERIC)
			ON pv.id = pvs.variant_id
		WHERE pv.qty_available <> COALESCE(pvs.total_stock, 0)
		ORDER BY pv.id
		LIMIT 10;
`, dblinkConn)

	var results []ComparisonResult
	err = db.Raw(query).Scan(&results).Error
	if err != nil {
		return fmt.Sprintf("Query execution failed: %v", err)
	}

	if len(results) == 0 {
		return "✅ No stock discrepancies found between XMS/Reseller and Voila."
	}

	// Build Mattermost Table
	var mmTable strings.Builder
	mmTable.WriteString(fmt.Sprintf("### 🚨 Stock Discrepancy Found (Reseller vs Voila)\n"))
	mmTable.WriteString(fmt.Sprintf("Found **%d** discrepancies.\n\n", len(results)))
	mmTable.WriteString("| Variant ID | Reseller | Voila | Diff |\n")
	mmTable.WriteString("| :--- | :---: | :---: | :---: |\n")

	var queries strings.Builder
	for _, res := range results {
		mmTable.WriteString(fmt.Sprintf("| %d | %d | %d | **%d** |\n",
			res.ID, res.ResellerQty, res.VoilaQty, res.Difference))

		queries.WriteString(fmt.Sprintf("UPDATE ms_product_variant SET qty_available = %d WHERE id = %d;\n", res.VoilaQty, res.ID))
	}

	mmTable.WriteString("\n### 🛠️ Bulk Update Queries\n")
	mmTable.WriteString("```sql\n")
	mmTable.WriteString(queries.String())
	mmTable.WriteString("```\n")

	return mmTable.String()
}

// main allows for local testing; Windmill uses func Main()
func main() {
	// Load .env from windmill root directory
	_ = godotenv.Load("../../.env")

	resellerDSN := os.Getenv("RESELLER_DSN")
	voilaDSN := os.Getenv("XMS_LEGACY_DSN")

	if resellerDSN == "" || voilaDSN == "" {
		fmt.Println("Note: RESELLER_DSN or XMS_LEGACY_DSN not found in environment.")
		return
	}

	fmt.Println(Main(resellerDSN, voilaDSN))
}
