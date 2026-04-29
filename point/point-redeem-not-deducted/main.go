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
	DefaultCatalystResource     = "u/mirza/catalyst_xms_postgresql_voila_prod"
	DefaultVoilaAccountResource = "u/sucahyo/voila_account_domain_postgresql_prod"
)

// --- Models ---

type RedeemedNotDeductedOrder struct {
	OrderID         int64   `gorm:"column:order_id"`
	ReferenceNumber string  `gorm:"column:reference_number"`
	OrderNumber     string  `gorm:"column:order_number"`
	StatusID        int64   `gorm:"column:status_id"`
	StatusCode      string  `gorm:"column:code"`
	DiscountID      int64   `gorm:"column:discount_id"`
	Amount          float64 `gorm:"column:amount"`
	CustomerID      int64   `gorm:"column:customer_id"`
	CustomerLink    string  `gorm:"-"`
	OrderLink       string  `gorm:"-"`
}

// --- Main Entry ---

func Main(xmsCatalystDSN, voilaAccountDSN string) (interface{}, error) {
	// 1. Resolve Credentials
	fmt.Println("Resolving DSNs...")
	catalystDSN := resolveDSN(xmsCatalystDSN, DefaultCatalystResource)
	if catalystDSN == "" {
		return nil, fmt.Errorf("catalyst dsn could not be resolved")
	}

	dblinkConn := resolveDSN(voilaAccountDSN, DefaultVoilaAccountResource)
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
			o.id AS order_id,
			o.reference_number,
			o.order_number,
			o.status_id,
			mos.code,
			d.id AS discount_id,
			d.amount,
			toc.customer_id
		FROM tr_order o
		JOIN tr_order_customer toc ON o.id = toc.order_id
		JOIN ms_order_status mos ON mos.id = o.status_id
		JOIN tr_order_discount d 
			ON d.order_id = o.id
			AND d.type = 'POINT_REWARD'
			AND d.code = 'point_redeemed'
		LEFT JOIN dblink(
			'%s',
			'SELECT 
				order_id,
				SUM(point_used) AS total_point_used
			FROM account_point_transaction
			WHERE type = ''redemption''
			GROUP BY order_id'
		) AS apt(order_id BIGINT, total_point_used NUMERIC)
		ON apt.order_id = o.id
		WHERE (apt.order_id IS NULL OR apt.total_point_used IS NULL OR apt.total_point_used = 0) 
		AND o.sales_channel_code != 'RESELLER'
	`, dblinkConn)

	var results []RedeemedNotDeductedOrder
	err = db.Raw(query).Scan(&results).Error
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}

	for i := range results {
		results[i].CustomerLink = fmt.Sprintf("https://xms-customer.voila.id/customer/%d/loyalty", results[i].CustomerID)
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

func formatMarkdown(data []RedeemedNotDeductedOrder) string {
	var sb strings.Builder
	sb.WriteString("##### Hi @oncall-voila, Ditemukan order dengan point yang di-redeem tapi point-nya belum terpotong di voila_account (account_point_transaction). Mohon dicek.\n")
	sb.WriteString("| Order ID | Order Number | Status | Amount Diskon | Customer ID | Order Link | Customer Link |\n")
	sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")

	for _, d := range data {
		sb.WriteString(fmt.Sprintf("| %d | %s | %s | %.0f | %d | [View Order](%s) | [View Customer](%s) |\n",
			d.OrderID, d.OrderNumber, d.StatusCode, d.Amount, d.CustomerID, d.OrderLink, d.CustomerLink))
	}
	return sb.String()
}
