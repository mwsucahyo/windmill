package inner

import (
	"fmt"
	"strings"

	windmill "github.com/windmill-labs/windmill-go-client"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// Indikasikan dependensi untuk Windmill
//require github.com/windmill-labs/windmill-go-client v1.655.0
//require gorm.io/driver/postgres v1.6.0
//require gorm.io/gorm v1.31.1

const (
	DefaultResellerResource = "u/sucahyo/voila_reseller_postgresql_stg"
	DefaultVoilaResource    = "u/mirza/voila_postgresql_stg"
)

// Hasil perbandingan stok
type ComparisonResult struct {
	ID           int    `gorm:"column:id"`
	SKU          string `gorm:"column:sku"`
	ResellerQty  int    `gorm:"column:reseller_qty"`
	XMSLegacyQty int    `gorm:"column:voila_qty"`
	Difference   int    `gorm:"column:difference"`
}

func Main(RESELLER_DSN string, XMS_LEGACY_DSN string) (interface{}, error) {
	return main(RESELLER_DSN, XMS_LEGACY_DSN)
}

// Fungsi utama harus bernama main (lowercase) dan mengembalikan (interface{}, error)
func main(RESELLER_DSN string, XMS_LEGACY_DSN string) (interface{}, error) {
	// 1. Ambil DSN Reseller
	resDSN := getDSN(RESELLER_DSN, DefaultResellerResource)
	if resDSN == "" {
		path := DefaultResellerResource
		if strings.HasPrefix(RESELLER_DSN, "u/") || strings.HasPrefix(RESELLER_DSN, "f/") {
			path = RESELLER_DSN
		}
		return nil, fmt.Errorf("RESELLER_DSN tidak ditemukan di resource: %s", path)
	}

	// 2. Bangun dblink untuk Voila/Legacy
	dblink, err := buildDblink(XMS_LEGACY_DSN, DefaultVoilaResource)
	if err != nil {
		return nil, err
	}

	// 3. Eksekusi Query
	results, err := runCheck(resDSN, dblink)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return "", nil
	}

	return generateMarkdown(results), nil
}

func getDSN(provided, defaultPath string) string {
	if provided != "" && !strings.HasPrefix(provided, "u/") && !strings.HasPrefix(provided, "f/") {
		return provided
	}
	path := defaultPath
	if provided != "" && (strings.HasPrefix(provided, "u/") || strings.HasPrefix(provided, "f/")) {
		path = provided
	}
	res, err := windmill.GetResource(path)
	if err != nil {
		return ""
	}
	m, ok := res.(map[string]interface{})
	if !ok {
		return ""
	}

	// Try the 'dsn' field first
	if dsn, ok := m["dsn"].(string); ok && dsn != "" {
		return dsn
	}

	// Fallback to individual fields
	if host, hOk := m["host"]; hOk && host != nil {
		dsn := fmt.Sprintf("host=%v user=%v password=%v dbname=%v", host, m["user"], m["password"], m["dbname"])
		if port, pOk := m["port"]; pOk && port != nil {
			dsn = fmt.Sprintf("%s port=%v", dsn, port)
		}
		return dsn
	}

	return ""
}

func buildDblink(provided, defaultPath string) (string, error) {
	if strings.HasPrefix(provided, "postgres://") {
		return parseDSN(provided), nil
	}
	path := defaultPath
	if provided != "" {
		path = provided
	}
	res, err := windmill.GetResource(path)
	if err != nil {
		return "", err
	}
	m := res.(map[string]interface{})
	conn := fmt.Sprintf("host=%v user=%v password=%v dbname=%v", m["host"], m["user"], m["password"], m["dbname"])
	if m["port"] != nil {
		conn = fmt.Sprintf("%s port=%v", conn, m["port"])
	}
	return conn, nil
}

func parseDSN(dsn string) string {
	trimmed := strings.TrimPrefix(dsn, "postgres://")
	var auth, host string
	if parts := strings.SplitN(trimmed, "@", 2); len(parts) == 2 {
		auth, host = parts[0], parts[1]
	} else {
		host = parts[0]
	}
	var res []string
	if creds := strings.SplitN(auth, ":", 2); len(creds) == 2 {
		res = append(res, fmt.Sprintf("user=%s password=%s", creds[0], creds[1]))
	}
	hp := strings.SplitN(host, "/", 2)
	res = append(res, fmt.Sprintf("dbname=%s", hp[1]))
	hostPort := strings.SplitN(hp[0], ":", 2)
	res = append(res, fmt.Sprintf("host=%s", hostPort[0]))
	if len(hostPort) == 2 {
		res = append(res, fmt.Sprintf("port=%s", hostPort[1]))
	}
	return strings.Join(res, " ")
}

func runCheck(dsn, dblink string) ([]ComparisonResult, error) {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: ""},
	})
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf(`
		SELECT 
			pv.id,
			pv.sku,
			pv.qty_available AS reseller_qty,
			COALESCE(pvs.total_stock, 0) AS voila_qty,
			(pv.qty_available - COALESCE(pvs.total_stock, 0)) AS difference
		FROM 
			ms_product_variant pv
		LEFT JOIN dblink('%s',
			$$ SELECT variant_id, SUM(qty_available) AS total_stock FROM ms_product_variant_stock GROUP BY variant_id $$
		) AS pvs(variant_id INT, total_stock NUMERIC) ON pv.id = pvs.variant_id
		WHERE pv.qty_available <> COALESCE(pvs.total_stock, 0)
		ORDER BY pv.id LIMIT 1000;`, dblink)
	var data []ComparisonResult
	err = db.Raw(query).Scan(&data).Error
	return data, err
}

func generateMarkdown(data []ComparisonResult) string {
	var sb strings.Builder
	sb.WriteString("#####Hi@channel, Ada perbedaan stock antara XMS Legacy & Biz, minta tolong dicek yah\n")
	sb.WriteString(fmt.Sprintf("Found **%d** discrepancies.\n\n", len(data)))
	sb.WriteString("| Variant ID | SKU | XMS Legacy | Biz | Diff |\n| :--- | :--- | :---: | :---: | :---: |\n")
	for _, r := range data {
		sb.WriteString(fmt.Sprintf("| %d | %s | %d | %d | **%d** |\n", r.ID, r.SKU, r.XMSLegacyQty, r.ResellerQty, r.Difference))
	}
	return sb.String()
}
