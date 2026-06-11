package inner

import (
	"fmt"
	"strings"
	"time"

	wmill "github.com/windmill-labs/windmill-go-client"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// --- Constants ---

const (
	XMS_CATALYST_BASE_URL = "https://xms.ctlyst.id"

	DefaultCatalystResource     = "u/mirza/catalyst_xms_postgresql_voila_prod"
	DefaultVoilaVoucherResource = "u/sucahyo/voila_voucher_domain_postgresql_prod"
)

// --- Models ---

type VoucherDiscrepancy struct {
	CustomerID      int64      `gorm:"column:customer_id"`
	CustomerEmail   string     `gorm:"column:customer_email"`
	OrderID         int64      `gorm:"column:order_id"`
	OrderNumber     string     `gorm:"column:order_number"`
	VoucherCode     string     `gorm:"column:voucher_code"`
	CreatedAt       *time.Time `gorm:"column:created_at"`
	OrderStatus     string     `gorm:"column:order_status"`
	TotalPointUsed  float64    `gorm:"column:total_point_used"`
	VoucherID       int64      `gorm:"column:voucher_id"`
	VoucherStatusID int        `gorm:"column:voucher_status_id"`
	OrderLink       string     `gorm:"-"`
}

// --- Main Entry ---

func Main(xmsCatalystDSN, voilaVoucherDSN string) (interface{}, error) {
	// 1. Resolve Credentials
	fmt.Println("Resolving DSNs...")
	catalystDSN := resolveDSN(xmsCatalystDSN, DefaultCatalystResource)
	if catalystDSN == "" {
		return nil, fmt.Errorf("catalyst dsn could not be resolved")
	}

	dblinkConn := resolveDSN(voilaVoucherDSN, DefaultVoilaVoucherResource)
	fmt.Println("DSNs resolved.")

	// 2. Connect to Catalyst (Postgres)
	fmt.Println("Connecting to Catalyst DB...")
	db, err := connectDB(catalystDSN)
	if err != nil {
		return nil, fmt.Errorf("catalyst db error: %w", err)
	}
	fmt.Println("Connected to Catalyst DB.")

	// 3. Query
	fmt.Println("Running query...")

	query := fmt.Sprintf(`
		SELECT 
			toc.customer_id,
			toc.customer_email,
			t.id AS order_id,
			t.order_number,
			STRING_AGG(tod.code, ', ') AS voucher_code,
			t.created_at,
			mos.name AS order_status,
			SUM(tod.amount) AS total_point_used,
			tod.voucher_id,
			mv.status_id AS voucher_status_id
		FROM tr_order t
		JOIN tr_order_customer toc 
			ON toc.order_id = t.id
		JOIN ms_order_status mos 
			ON mos.id = t.status_id
		JOIN tr_order_discount tod 
			ON tod.order_id = t.id
		LEFT JOIN dblink(
			'%s'::text,
			'SELECT order_id, voucher_id FROM tr_voucher_usage'::text
		) AS tvu(order_id BIGINT, voucher_id BIGINT)
			ON tvu.order_id = t.id 
			AND tvu.voucher_id = tod.voucher_id
		LEFT JOIN dblink(
			'%s'::text,
			'SELECT id, status_id FROM ms_voucher'::text
		) AS mv(id BIGINT, status_id INT)
			ON mv.id = tod.voucher_id
		WHERE 
			tod.voucher_id IS NOT NULL
			AND tvu.order_id IS NULL
			AND t.status_id != 4
			AND t.created_at >= '2026-02-10'
		GROUP BY 
			t.id, 
			toc.customer_id,	
			toc.customer_email,
			t.order_number, 
			mos.name, 
			t.created_at, 
			tod.voucher_id,
			mv.status_id
		LIMIT 10;
	`, dblinkConn, dblinkConn)

	var results []VoucherDiscrepancy
	err = db.Raw(query).Scan(&results).Error
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}

	for i := range results {
		results[i].OrderLink = fmt.Sprintf("https://xms.ctlyst.id/voila/order/order-detail/%s", results[i].OrderNumber)
	}

	fmt.Printf("Query finished. Found %d results.\n", len(results))

	if len(results) == 0 {
		return nil, nil
	}

	return formatMarkdown(results), nil
}

// --- Helper Functions ---

func resolveDSN(provided, resourcePath string) string {
	if strings.HasPrefix(provided, "postgres://") || strings.HasPrefix(provided, "host=") {
		return provided
	}

	res, err := wmill.GetResource(resourcePath)
	if err != nil {
		return provided
	}

	m, ok := res.(map[string]interface{})
	if !ok {
		return provided
	}

	if dsn, ok := m["dsn"].(string); ok && dsn != "" {
		return dsn
	}

	// Fallback to components
	return fmt.Sprintf("host=%v user=%v password=%v dbname=%v port=%v",
		m["host"], m["user"], m["password"], m["dbname"], m["port"])
}

func connectDB(dsn string) (*gorm.DB, error) {
	config := &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: ""},
	}
	// Ensure search_path is set to voila,public
	if !strings.Contains(dsn, "search_path") {
		if strings.Contains(dsn, "?") {
			dsn += "&search_path=voila,public"
		} else if !strings.Contains(dsn, "host=") {
			dsn += "?search_path=voila,public"
		}
	}
	return gorm.Open(postgres.Open(dsn), config)
}

func formatMarkdown(data []VoucherDiscrepancy) string {
	var sb strings.Builder
	sb.WriteString("##### Hi @oncall-voila. Ditemukan order yang menggunakan voucher namun voucher usage nya tidak masuk & berpotensi adanya kelolosan voucher nya, tolong dicek..\n\n")

	sb.WriteString("| Order ID | Order Number | Voucher Code | Amount | Order Status | Customer ID | Order Link |\n")
	sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")

	for _, d := range data {
		sb.WriteString(fmt.Sprintf("| %d | %s | %s | %.2f | %s | %d | [View Order](%s) |\n",
			d.OrderID, d.OrderNumber, d.VoucherCode, d.TotalPointUsed, d.OrderStatus, d.CustomerID, d.OrderLink))
	}

	sb.WriteString("\n**How to Handle:**\nhttps://jamtangan.atlassian.net/wiki/spaces/EN/pages/3698032644/Handle+Missing+Voucher+Usage+for+Orders\n")

	return sb.String()
}
