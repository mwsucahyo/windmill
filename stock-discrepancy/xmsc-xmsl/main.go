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

// --- Models ---

type StockMovement struct {
	VariantID int `gorm:"column:variant_id"`
	OfficeID  int `gorm:"column:office_id"`
}

func (StockMovement) TableName() string { return "voila.tr_stock_movement_history" }

type CatalystStock struct {
	VariantID    int  `gorm:"column:variant_id"`
	OfficeID     int  `gorm:"column:office_id"`
	QtyAvailable int  `gorm:"column:qty_available"`
	IsDeleted    bool `gorm:"column:is_deleted"`
}

func (CatalystStock) TableName() string { return "voila.ms_product_variant_stock" }

type LegacyStock struct {
	VariantID    int    `gorm:"column:variant_id"`
	ProductID    int    `gorm:"column:product_id"`
	OfficeID     int    `gorm:"column:office_id"`
	SKU          string `gorm:"column:sku"`
	OfficeName   string `gorm:"column:office_name"`
	QtyAvailable int    `gorm:"column:qty_available"`
}

type Discrepancy struct {
	VariantID   int
	ProductID   int
	SKU         string
	OfficeName  string
	CatalystQty int
	LegacyQty   int
	Diff        int
}

// --- Main Entry ---

func Main(xmsCatalystDSN, xmsLegacyDSN string) (interface{}, error) {
	// 1. Resolve DSNs
	catalystDSN := resolveDSN(xmsCatalystDSN, "u/mirza/catalyst_xms_postgresql_voila_prod")
	legacyDSN := resolveDSN(xmsLegacyDSN, "u/mirza/voila_postgresql_prod")

	if catalystDSN == "" || legacyDSN == "" {
		return nil, fmt.Errorf("could not resolve database credentials")
	}

	// 2. Connect
	catalystDB, err := connectDB(catalystDSN, true)
	if err != nil {
		return nil, fmt.Errorf("catalyst db error: %w", err)
	}

	legacyDB, err := connectDB(legacyDSN, false)
	if err != nil {
		return nil, fmt.Errorf("legacy db error: %w", err)
	}

	// 3. Get Recent Movements (1 hour)
	movements, err := getRecentMovements(catalystDB)
	if err != nil {
		return nil, err
	}
	if len(movements) == 0 {
		return "No stock movements in the last hour.", nil
	}

	// 4. Fetch Stock Data
	catData, err := fetchCatalystData(catalystDB, movements)
	if err != nil {
		return nil, err
	}

	legData, err := fetchLegacyData(legacyDB, movements)
	if err != nil {
		return nil, err
	}

	// 5. Compare & Report
	diffs := compareStocks(movements, catData, legData)
	if len(diffs) == 0 {
		return "Success: No stock discrepancies found.", nil
	}

	return formatMarkdown(diffs), nil
}

// --- Helper Functions ---

func resolveDSN(provided, resourcePath string) string {
	// Use provided if it's already a DSN
	if strings.HasPrefix(provided, "postgres://") {
		return provided
	}

	// Try fetching from Windmill resource
	res, err := wmill.GetResource(resourcePath)
	if err != nil {
		return ""
	}

	m, ok := res.(map[string]interface{})
	if !ok {
		return ""
	}

	if dsn, ok := m["dsn"].(string); ok && dsn != "" {
		return dsn
	}

	// Build from parts if 'dsn' field is missing
	return fmt.Sprintf("postgres://%v:%v@%v:%v/%v",
		m["user"], m["password"], m["host"], m["port"], m["dbname"])
}

func connectDB(dsn string, isCatalyst bool) (*gorm.DB, error) {
	config := &gorm.Config{}
	if isCatalyst {
		config.NamingStrategy = schema.NamingStrategy{TablePrefix: ""}
		if !strings.Contains(dsn, "search_path") {
			if strings.Contains(dsn, "?") {
				dsn += "&search_path=voila"
			} else {
				dsn += "?search_path=voila"
			}
		}
	}
	return gorm.Open(postgres.Open(dsn), config)
}

func getRecentMovements(db *gorm.DB) ([]StockMovement, error) {
	var movements []StockMovement
	err := db.Debug().Select("DISTINCT variant_id, office_id").
		Where("qty_column = ? AND created_at >= ?", "qty_available", time.Now().Add(-1*time.Hour)).
		Find(&movements).Error
	return movements, err
}

func applyCompositeFilter(query *gorm.DB, movements []StockMovement, variantCol, officeCol string) *gorm.DB {
	filter := query.Session(&gorm.Session{})
	for i, m := range movements {
		cond := fmt.Sprintf("%s = ? AND %s = ?", variantCol, officeCol)
		if i == 0 {
			filter = filter.Where(cond, m.VariantID, m.OfficeID)
		} else {
			filter = filter.Or(cond, m.VariantID, m.OfficeID)
		}
	}
	return filter
}

func fetchCatalystData(db *gorm.DB, movements []StockMovement) (map[string]int, error) {
	var stocks []CatalystStock
	query := db.Where("is_deleted = ?", false)
	err := applyCompositeFilter(query, movements, "variant_id", "office_id").Find(&stocks).Error
	if err != nil {
		return nil, fmt.Errorf("catalyst stock query: %w", err)
	}

	data := make(map[string]int)
	for _, s := range stocks {
		data[fmt.Sprintf("%d-%d", s.VariantID, s.OfficeID)] = s.QtyAvailable
	}
	return data, nil
}

func fetchLegacyData(db *gorm.DB, movements []StockMovement) (map[string]LegacyStock, error) {
	var results []LegacyStock
	query := db.Table("public.ms_product_variant_stock mpvs").
		Select("mpvs.variant_id, mpv.product_id, mpv.sku, mpvs.office_id, mo.name as office_name, mpvs.qty_available").
		Joins("JOIN public.ms_office mo ON mo.id = mpvs.office_id").
		Joins("JOIN public.ms_product_variant mpv ON mpv.id = mpvs.variant_id").
		Where("mpvs.is_deleted = ?", 0)

	err := applyCompositeFilter(query, movements, "mpvs.variant_id", "mpvs.office_id").Scan(&results).Error
	if err != nil {
		return nil, fmt.Errorf("legacy stock query: %w", err)
	}

	data := make(map[string]LegacyStock)
	for _, r := range results {
		data[fmt.Sprintf("%d-%d", r.VariantID, r.OfficeID)] = r
	}
	return data, nil
}

func compareStocks(movements []StockMovement, cat map[string]int, leg map[string]LegacyStock) []Discrepancy {
	var diffs []Discrepancy
	for _, m := range movements {
		key := fmt.Sprintf("%d-%d", m.VariantID, m.OfficeID)
		catQty := cat[key]
		legInfo, exists := leg[key]

		if !exists || catQty != legInfo.QtyAvailable {
			d := Discrepancy{
				VariantID:   m.VariantID,
				CatalystQty: catQty,
			}
			if exists {
				d.ProductID, d.SKU, d.OfficeName, d.LegacyQty = legInfo.ProductID, legInfo.SKU, legInfo.OfficeName, legInfo.QtyAvailable
			} else {
				d.SKU, d.OfficeName = "N/A", "N/A"
			}
			d.Diff = d.CatalystQty - d.LegacyQty
			diffs = append(diffs, d)
		}
	}
	sort.Slice(diffs, func(i, j int) bool { return diffs[i].Diff > diffs[j].Diff })
	return diffs
}

func formatMarkdown(diffs []Discrepancy) string {
	var sb strings.Builder
	sb.WriteString("##### Hi @channel, Ada perbedaan stock antara XMS Catalyst & XMS Legacy, minta tolong dicek yah..\n")
	sb.WriteString("| Variant ID | SKU | Office | Catalyst | Legacy | Diff | Links |\n")
	sb.WriteString("| :--- | :--- | :--- | :---: | :---: | :---: | :--- |\n")
	for _, d := range diffs {
		catLink := fmt.Sprintf("[Catalyst](https://xms.ctlyst.id/voila/stock/office/%d)", d.VariantID)
		legLink := "N/A"
		if d.ProductID != 0 {
			legLink = fmt.Sprintf("[Legacy](https://xms.voila.id/product/%d/stockOffice)", d.ProductID)
		}
		sb.WriteString(fmt.Sprintf("| %d | %s | %s | %d | %d | %d | %s / %s |\n",
			d.VariantID, d.SKU, d.OfficeName, d.CatalystQty, d.LegacyQty, d.Diff, catLink, legLink))
	}
	return sb.String()
}
