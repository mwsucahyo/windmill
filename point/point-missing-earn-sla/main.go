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
	DefaultCatalystResource     = "u/mirza/catalyst_xms_postgresql_voila_prod"
	DefaultVoilaAccountResource = "u/sucahyo/voila_account_domain_postgresql_prod"

	// Query parameters
	QueryStartDate  = "2026-02-10"
	SLAIntervalDays = 4
)

// --- Models ---

type MissingEarnOrder struct {
	OrderID         int64      `gorm:"column:id"`
	OrderNumber     string     `gorm:"column:order_number"`
	ReferenceNumber string     `gorm:"column:reference_number"`
	CreatedAt       *time.Time `gorm:"column:created_at"`
	CompletedAt     *time.Time `gorm:"column:completed_at"`
	PointEarned     float64    `gorm:"column:point_earned"`
	CustomerID      int64      `gorm:"column:customer_id"`
	CustomerEmail   string     `gorm:"column:customer_email"`
	ReturIDRef      *int64     `gorm:"column:retur_id_ref"`
	Source          *string    `gorm:"column:source"`
	CustomerLink    string     `gorm:"-"`
	OrderLink       string     `gorm:"-"`
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
			t.id,
			t.order_number,
			t.reference_number,
			t.created_at,
			t.completed_at,
			t.point_earned,
			toc.customer_id,
			toc.customer_email,
			t.retur_id_ref,
			ap.source
		FROM tr_order t
		JOIN tr_order_customer toc
			ON toc.order_id = t.id
		LEFT JOIN public.dblink(
			'%[1]s',
			$$
			SELECT 
				order_id,
				order_number,
				source
			FROM account_point
			WHERE 
				source = 'transaction'
				AND created_at >= '%[2]s'
			$$
		) AS ap(
			order_id BIGINT,
			order_number TEXT,
			source TEXT
		)
		ON t.id = ap.order_id
		LEFT JOIN tr_retur r
			ON r.reference_id = t.id
			AND r.reference_source = 'ORDER'
			AND r.deleted_at IS NULL
		WHERE 
			t.status_id = 5
			AND ap.order_id IS NULL
			AND r.id IS NULL -- exclude yang sudah retur
			AND t.sales_channel_code != 'RESELLER'
			AND t.point_earned > 0
			AND t.created_at >= '%[2]s'
			AND t.completed_at IS NOT NULL
			AND t.completed_at::DATE < CURRENT_DATE - INTERVAL '%[3]d days'
		ORDER BY t.completed_at ASC;
	`, dblinkConn, QueryStartDate, SLAIntervalDays)

	var results []MissingEarnOrder
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

func formatMarkdown(data []MissingEarnOrder) string {
	var sb strings.Builder
	sb.WriteString("##### Hi @oncall-voila, Ditemukan order Completed (SLA > 4 hari) namun Point Earned-nya belum masuk ke voila_account. Mohon dicek.\n")
	sb.WriteString("| Order ID | Order Number | Created At | Completed At | Point Earned | Order Link | Customer Link |\n")
	sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")

	for _, d := range data {
		createdAt := "-"
		if d.CreatedAt != nil {
			createdAt = d.CreatedAt.Format("2006-01-02 15:04:05")
		}
		completedAt := "-"
		if d.CompletedAt != nil {
			completedAt = d.CompletedAt.Format("2006-01-02 15:04:05")
		}

		sb.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %.0f | [View Order](%s) | [View Customer](%s) |\n",
			d.OrderID, d.OrderNumber, createdAt, completedAt, d.PointEarned, d.OrderLink, d.CustomerLink))
	}
	return sb.String()
}
