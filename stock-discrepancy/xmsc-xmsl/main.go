package inner

import (
	"fmt"
	"sort"
	"strings"
	"time"

	wmill "github.com/windmill-labs/windmill-go-client"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// require gorm.io/gorm v1.25.12
// require gorm.io/driver/postgres v1.5.9

// Model definitions
type trStockMovementHistory struct {
	VariantID int `gorm:"column:variant_id"`
	OfficeID  int `gorm:"column:office_id"`
}

func (trStockMovementHistory) TableName() string { return "voila.tr_stock_movement_history" }

type msProductVariantStockCatalyst struct {
	VariantID    int  `gorm:"column:variant_id"`
	OfficeID     int  `gorm:"column:office_id"`
	QtyAvailable int  `gorm:"column:qty_available"`
	IsDeleted    bool `gorm:"column:is_deleted"`
}

func (msProductVariantStockCatalyst) TableName() string { return "voila.ms_product_variant_stock" }

type ComparisonResult struct {
	VariantID   string
	SKU         string
	OfficeID    string
	OfficeName  string
	CatalystQty int
	LegacyQty   int
	Diff        int
}

// Helper to build DSN from Windmill Resource Map
func buildDSN(res interface{}) string {
	m, ok := res.(map[string]interface{})
	if !ok {
		return ""
	}

	// Ambil data dari map. Gunakan fmt.Sprint agar aman jika tipenya (int/string) berbeda
	user := fmt.Sprint(m["user"])
	password := fmt.Sprint(m["password"])
	host := fmt.Sprint(m["host"])
	port := fmt.Sprint(m["port"])
	dbname := fmt.Sprint(m["dbname"])

	// Jika field dsn ternyata ada, pakai itu saja
	if dsn, ok := m["dsn"].(string); ok && dsn != "" {
		return dsn
	}

	// Susun format: postgres://user:password@host:port/dbname
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, password, host, port, dbname)
}

func main() (interface{}, error) {
	// 1. Fetch Catalyst Resource
	resCatalyst, err := wmill.GetResource("u/mirza/catalyst_xms_postgresql_voila_prod")
	if err != nil {
		return nil, fmt.Errorf("failed to get catalyst resource: %v", err)
	}
	catalystDSN := buildDSN(resCatalyst)
	// Tambahkan search_path khusus catalyst
	if !strings.Contains(catalystDSN, "search_path") {
		catalystDSN += "?search_path=voila"
	}

	// 2. Fetch Legacy Resource
	resLegacy, err := wmill.GetResource("u/mirza/voila_postgresql_prod")
	if err != nil {
		return nil, fmt.Errorf("failed to get legacy resource: %v", err)
	}
	legacyDSN := buildDSN(resLegacy)

	if catalystDSN == "" || legacyDSN == "" {
		return nil, fmt.Errorf("one or more DSN strings could not be constructed")
	}

	// 3. Connect to Databases
	catalystDB, err := gorm.Open(postgres.Open(catalystDSN), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: ""},
	})
	if err != nil {
		return nil, fmt.Errorf("error connecting to Catalyst DB: %v", err)
	}

	legacyDB, err := gorm.Open(postgres.Open(legacyDSN), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("error connecting to Legacy DB: %v", err)
	}

	// 4. Logic: Fetch Latest Movements (24h)
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
		return nil, fmt.Errorf("catalyst query failed: %v", err)
	}

	if len(catalystStocks) == 0 {
		return "No stock movements found in the last 24 hours.", nil
	}

	catalystDataMap := make(map[string]int)
	var pairs [][]interface{}
	for _, s := range catalystStocks {
		key := fmt.Sprintf("%d-%d", s.VariantID, s.OfficeID)
		catalystDataMap[key] = s.QtyAvailable
		pairs = append(pairs, []interface{}{s.VariantID, s.OfficeID})
	}

	// 5. Query Legacy
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
		return nil, fmt.Errorf("legacy query failed: %v", err)
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
			Qty: r.QtyAvailable, OfficeName: r.OfficeName, SKU: r.SKU,
		}
	}

	// 6. Compare items
	var results []ComparisonResult
	for key, catalystQty := range catalystDataMap {
		legacyInfo, exists := legacyDataMap[key]
		parts := strings.Split(key, "-")
		if !exists || catalystQty != legacyInfo.Qty {
			sku, officeName, legacyQtyVal := "N/A", "N/A", 0
			if exists {
				sku, officeName, legacyQtyVal = legacyInfo.SKU, legacyInfo.OfficeName, legacyInfo.Qty
			}
			results = append(results, ComparisonResult{
				VariantID: parts[0], SKU: sku, OfficeID: parts[1],
				OfficeName: officeName, CatalystQty: catalystQty,
				LegacyQty: legacyQtyVal, Diff: catalystQty - legacyQtyVal,
			})
		}
	}

	if len(results) == 0 {
		return "Success: No stock discrepancies found.", nil
	}

	sort.Slice(results, func(i, j int) bool { return results[i].Diff > results[j].Diff })

	var mmTable strings.Builder
	mmTable.WriteString("##### Hi @channel, Ada perbedaan stock antara XMS Catalyst & XMS Legacy, minta tolong dicek yah..\n")
	mmTable.WriteString("| Variant ID | SKU | Office | Catalyst | Legacy | Diff |\n| :--- | :--- | :--- | :---: | :---: | :---: |\n")
	for _, res := range results {
		mmTable.WriteString(fmt.Sprintf("| %s | %s | %s | %d | %d | %d |\n", res.VariantID, res.SKU, res.OfficeName, res.CatalystQty, res.LegacyQty, res.Diff))
	}

	return mmTable.String(), nil
}
