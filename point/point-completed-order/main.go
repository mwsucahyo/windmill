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
	DefaultVoilaAccountResource = "u/sucahyo/voila_account_domain_postgresql_prod"
)

// --- Models ---

type CompletedOrder struct {
	ID           int64      `gorm:"column:id"`
	OrderNumber  string     `gorm:"column:order_number"`
	CreatedAt    *time.Time `gorm:"column:created_at"`
	CompletedAt  *time.Time `gorm:"column:completed_at"`
	CustomerID   int64      `gorm:"column:customer_id"`
	CustomerLink string     `gorm:"-"`
	OrderLink    string     `gorm:"-"`
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

	// Use the SQL structure provided by the user
	query := fmt.Sprintf(`
		SELECT 
			o.id,
			o.order_number,
			o.created_at,
			o.completed_at,
			oc.customer_id
		FROM tr_order o
		INNER JOIN tr_order_customer oc 
			ON oc.order_id = o.id
		LEFT JOIN dblink(
			'%s',
			'SELECT customer_id, is_verified
			 FROM account'
		) AS acc(
			customer_id BIGINT,
			is_verified BOOLEAN
		)
			ON oc.customer_id = acc.customer_id
		LEFT JOIN dblink(
			'%s',
			'SELECT xmsc_order_id
			 FROM customer_completed_order
			 WHERE created_at >= ''2026-02-10'''
		) AS cco(
			xmsc_order_id BIGINT
		)
			ON o.id = cco.xmsc_order_id
		WHERE o.status_id = 5
		AND o.completed_at <= NOW() - INTERVAL '4 days'
		AND o.point_earned > 0
		AND o.sales_channel_code != 'RESELLER'
		AND o.created_at >= DATE '2026-02-10'
		AND cco.xmsc_order_id IS NULL
		AND acc.is_verified = true LIMIT 10
	`, dblinkConn, dblinkConn)

	var results []CompletedOrder
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

func formatMarkdown(data []CompletedOrder) string {
	var sb strings.Builder
	sb.WriteString("##### Hi @oncall-voila, Ditemukan order COMPLETED yang data-nya XMS Catalyst sudah completed tapi belum ada data completed order-nya di voila_account, tolong dicek yah..\n")
	sb.WriteString("| Order ID | Order Number | Created At | Completed At | Customer ID | Order Link | Customer Link |\n")
	sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")

	for _, d := range data {
		createdAt := "-"
		if d.CreatedAt != nil {
			createdAt = d.CreatedAt.Format("2006-01-02 15:04:05")
		}
		completedAt := "-"
		if d.CompletedAt != nil {
			completedAt = d.CompletedAt.Format("2006-01-02 15:04:05")
		}

		sb.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %d | [View Order](%s) | [View Customer](%s) |\n",
			d.ID, d.OrderNumber, createdAt, completedAt, d.CustomerID, d.OrderLink, d.CustomerLink))
	}
	return sb.String()
}
