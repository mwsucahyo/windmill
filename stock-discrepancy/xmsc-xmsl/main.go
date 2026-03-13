package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/joho/godotenv"
	windmill "github.com/windmill-labs/windmill-go-client"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// trStockMovementHistory model for Catalyst DB
type trStockMovementHistory struct {
	VariantID int `gorm:"column:variant_id"`
	OfficeID  int `gorm:"column:office_id"`
}

func (trStockMovementHistory) TableName() string {
	return "voila.tr_stock_movement_history"
}

// msProductVariantStockCatalyst model for Catalyst DB
type msProductVariantStockCatalyst struct {
	VariantID    int  `gorm:"column:variant_id"`
	OfficeID     int  `gorm:"column:office_id"`
	QtyAvailable int  `gorm:"column:qty_available"`
	IsDeleted    bool `gorm:"column:is_deleted"`
}

func (msProductVariantStockCatalyst) TableName() string {
	return "voila.ms_product_variant_stock"
}

// msProductVariantStockLegacy model for Legacy DB
type msProductVariantStockLegacy struct {
	VariantID    int `gorm:"column:variant_id"`
	OfficeID     int `gorm:"column:office_id"`
	QtyAvailable int `gorm:"column:qty_available"`
	IsDeleted    int `gorm:"column:is_deleted"`
}

func (msProductVariantStockLegacy) TableName() string {
	return "public.ms_product_variant_stock"
}

// ComparisonResult model for the final table
type ComparisonResult struct {
	VariantID    string
	SKU          string
	OfficeID     string
	OfficeName   string
	CatalystQty  int
	LegacyQty    int
	Diff         int
	Message      string
	HasDiscovery bool
}

// Main is the entry point for Windmill
func Main(XMS_CATALYST_DSN string, XMS_LEGACY_DSN string) string {
	// Fetch Catalyst DSN from Windmill resource if empty
	if XMS_CATALYST_DSN == "" {
		res, err := windmill.GetResource("u/mirza/catalyst_xms_postgresql_voila_stg")
		if err == nil {
			if dsn, ok := res.(map[string]interface{})["dsn"].(string); ok {
				XMS_CATALYST_DSN = dsn
			}
		}
	}

	// Fetch Legacy DSN from Windmill resource if empty
	if XMS_LEGACY_DSN == "" {
		res, err := windmill.GetResource("u/mirza/voila_postgresql_stg")
		if err == nil {
			if dsn, ok := res.(map[string]interface{})["dsn"].(string); ok {
				XMS_LEGACY_DSN = dsn
			}
		}
	}

	if XMS_CATALYST_DSN == "" || XMS_LEGACY_DSN == "" {
		return "Error: Missing DSN. Please provide as parameter or check Windmill resources at 'u/mirza/catalyst_xms_postgresql_voila_stg' and 'u/mirza/voila_postgresql_stg'."
	}

	// 1. Connect to Catalyst DB
	catalystDB, err := gorm.Open(postgres.Open(XMS_CATALYST_DSN), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			TablePrefix: "",
		},
	})
	if err != nil {
		return fmt.Sprintf("Error connecting to Catalyst DB: %v", err)
	}

	// 2. Connect to Legacy DB
	legacyDB, err := gorm.Open(postgres.Open(XMS_LEGACY_DSN), &gorm.Config{})
	if err != nil {
		return fmt.Sprintf("Error connecting to Legacy DB: %v", err)
	}

	// 3. Query Catalyst DB
	subQuery := catalystDB.Table("voila.tr_stock_movement_history").
		Select("DISTINCT variant_id, office_id").
		Where("qty_column = ?", "qty_available").
		Where("created_at >= ?", time.Now().Add(-1*time.Hour))

	var catalystStocks []msProductVariantStockCatalyst
	err = catalystDB.Table("voila.ms_product_variant_stock").
		Where("(variant_id, office_id) IN (?)", subQuery).
		Where("is_deleted = ?", false).
		Find(&catalystStocks).Error

	if err != nil {
		return fmt.Sprintf("Query failed on Catalyst DB: %v", err)
	}

	if len(catalystStocks) == 0 {
		return "No stock movements found in the last 24 hours."
	}

	catalystDataMap := make(map[string]int)
	var pairs [][]interface{}
	for _, s := range catalystStocks {
		key := fmt.Sprintf("%d-%d", s.VariantID, s.OfficeID)
		catalystDataMap[key] = s.QtyAvailable
		pairs = append(pairs, []interface{}{s.VariantID, s.OfficeID})
	}

	// 4. Query Legacy DB
	type LegacyResult struct {
		VariantID    int
		SKU          string
		OfficeID     int
		OfficeName   string
		QtyAvailable int
	}
	var legacyResults []LegacyResult

	err = legacyDB.Table("public.ms_product_variant_stock mpvs").
		Select("mpvs.variant_id, mpv.sku, mpvs.office_id, mo.name as office_name, mpvs.qty_available").
		Joins("JOIN public.ms_office mo ON mo.id = mpvs.office_id").
		Joins("JOIN public.ms_product_variant mpv ON mpv.id = mpvs.variant_id").
		Where("(mpvs.variant_id, mpvs.office_id) IN (?)", pairs).
		Where("mpvs.is_deleted = ?", 0).
		Scan(&legacyResults).Error

	if err != nil {
		return fmt.Sprintf("Query failed on Legacy DB: %v", err)
	}

	legacyDataMap := make(map[string]struct {
		Qty        int
		OfficeName string
		SKU        string
	})
	for _, r := range legacyResults {
		key := fmt.Sprintf("%d-%d", r.VariantID, r.OfficeID)
		legacyDataMap[key] = struct {
			Qty        int
			OfficeName string
			SKU        string
		}{
			Qty:        r.QtyAvailable,
			OfficeName: r.OfficeName,
			SKU:        r.SKU,
		}
	}

	// 5. Compare
	var results []ComparisonResult
	for key, catalystQty := range catalystDataMap {
		legacyInfo, exists := legacyDataMap[key]
		parts := strings.Split(key, "-")
		variantID := parts[0]
		officeID := parts[1]

		res := ComparisonResult{
			VariantID:   variantID,
			OfficeID:    officeID,
			CatalystQty: catalystQty,
		}

		if !exists {
			res.SKU = "N/A"
			res.OfficeName = "N/A"
			res.Message = " (Not found in Legacy)"
			res.HasDiscovery = true
			res.Diff = 0
		} else if catalystQty != legacyInfo.Qty {
			res.SKU = legacyInfo.SKU
			res.OfficeName = legacyInfo.OfficeName
			res.LegacyQty = legacyInfo.Qty
			res.Diff = catalystQty - legacyInfo.Qty
			res.Message = " (Mismatch!)"
			res.HasDiscovery = true
		}

		if res.HasDiscovery {
			results = append(results, res)
		}
	}

	if len(results) == 0 {
		return "Success: No stock discrepancies found between Catalyst and Legacy."
	}

	// Sorting by Diff DESC
	sort.Slice(results, func(i, j int) bool {
		return results[i].Diff > results[j].Diff
	})

	// 6. Build Mattermost Table
	var mmTable strings.Builder
	mmTable.WriteString("### 🚨 Stock Discrepancy Found (Catalyst vs Legacy)\n")
	mmTable.WriteString("| Variant ID | SKU | Office | Catalyst | Legacy | Diff |\n")
	mmTable.WriteString("| :--- | :--- | :--- | :---: | :---: | :---: |\n")

	for _, res := range results {
		if res.Message == " (Not found in Legacy)" {
			mmTable.WriteString(fmt.Sprintf("| %s | %s | %s | %d | N/A | N/A |\n", res.VariantID, res.SKU, res.OfficeName, res.CatalystQty))
		} else {
			mmTable.WriteString(fmt.Sprintf("| %s | %s | %s | %d | %d | %d |\n", res.VariantID, res.SKU, res.OfficeName, res.CatalystQty, res.LegacyQty, res.Diff))
		}
	}

	return mmTable.String()
}

// main allows for local testing; Windmill uses func Main()
func main() {
	_ = godotenv.Load()
	_ = godotenv.Load("stock-discrepancy/xmsc-xmsl/.env") // fallback to local if any

	catalystDSN := os.Getenv("XMS_CATALYST_DSN")
	legacyDSN := os.Getenv("XMS_LEGACY_DSN")

	if catalystDSN == "" || legacyDSN == "" {
		log.Println("Note: XMS_CATALYST_DSN or XMS_LEGACY_DSN not found in environment.")
		return
	}

	// Just call Main and print the output
	fmt.Println(Main(catalystDSN, legacyDSN))
}
