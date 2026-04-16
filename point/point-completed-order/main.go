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
	DefaultVoilaAccountResource = "u/chandra/voila_account_postgresql_prod"
)

// --- Models ---

type CompletedOrder struct {
	ID               int64      `gorm:"column:id"`
	OrderNumber      string     `gorm:"column:order_number"`
	CreatedAt        *time.Time `gorm:"column:created_at"`
	CompletedAt      *time.Time `gorm:"column:completed_at"`
	CustomerID       int64      `gorm:"column:customer_id"`
	SalesChannelCode string     `gorm:"column:sales_channel_code"`
	PlatformSource   string     `gorm:"column:platform_source"`
	CustomerLink     string     `gorm:"column:customer_link"`
	OrderLink        string     `gorm:"column:order_link"`
}

// --- Main Entry ---

func Main(xmsCatalystDSN, voilaAccountDSN string, orderID int) (interface{}, error) {
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

	whereClause := ""
	if orderID != 0 {
		whereClause = fmt.Sprintf("\n\t\tAND o.id = %d", orderID)
	}

	// Use the SQL structure provided by the user
	query := fmt.Sprintf(`
		SELECT 
			o.id,
			o.order_number,
			o.created_at,
			o.completed_at,
			oc.customer_id,
			o.sales_channel_code,
			o.platform_source,
			'https://xms-customer.voila.id/customer/' || oc.customer_id || '/loyalty' AS customer_link,
			'https://xms.ctlyst.id/voila/order/order-detail/' || o.order_number AS order_link
		FROM tr_order o
		INNER JOIN tr_order_customer oc 
			ON oc.order_id = o.id
		LEFT JOIN dblink(
			'%s',
			'SELECT cco.xmsc_order_id, a.is_verified
			 FROM customer_completed_order cco
			 LEFT JOIN account a 
			   ON a.customer_id = cco.customer_id
			 WHERE cco.created_at >= ''2026-02-10'''
		) AS acc(
			xmsc_order_id BIGINT,
			is_verified BOOLEAN
		)
			ON o.id = acc.xmsc_order_id
		WHERE acc.xmsc_order_id IS NULL
		AND o.status_id = 5
		AND o.completed_at <= NOW() - INTERVAL '4 days'
		AND o.point_earned > 0
		AND o.sales_channel_code != 'RESELLER'
		AND o.created_at >= DATE '2026-02-10'
		AND (acc.is_verified = FALSE OR acc.is_verified IS NULL) %s
	`, dblinkConn, whereClause)

	var results []CompletedOrder
	err = db.Raw(query).Scan(&results).Error
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	fmt.Printf("Query finished. Found %d results.\n", len(results))

	if len(results) == 0 {
		return "No results found for the given criteria.", nil
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
	sb.WriteString("### Completed Order Check Results\n\n")
	sb.WriteString("| Order ID | Order Number | Created At | Completed At | Customer ID | Sales Channel | Platform Source | Order Link | Customer Link |\n")
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

		sb.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %d | %s | %s | [View Order](%s) | [View Customer](%s) |\n",
			d.ID, d.OrderNumber, createdAt, completedAt, d.CustomerID, d.SalesChannelCode, d.PlatformSource, d.OrderLink, d.CustomerLink))
	}
	return sb.String()
}
