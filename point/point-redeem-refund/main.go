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
	XMS_CATALYST_BASE_URL = "https://xms.ctlyst.id"

	DefaultCatalystResource     = "u/mirza/catalyst_xms_postgresql_voila_prod"
	DefaultVoilaAccountResource = "u/chandra/voila_account_postgresql_prod"
)

// --- Models ---

type PointDiscrepancy struct {
	ID                 int64   `gorm:"column:id"`
	OrderNumber        string  `gorm:"column:order_number"`
	CustomerID         int64   `gorm:"column:customer_id"`
	TotalPointRedeemed float64 `gorm:"column:total_point_redeemed"`
}

// --- Main Entry ---

func Main(xmsCatalystDSN, voilaAccountDSN string) (interface{}, error) {
	// 1. Resolve Credentials
	catalystDSN := resolveDSN(xmsCatalystDSN, DefaultCatalystResource)
	if catalystDSN == "" {
		return nil, fmt.Errorf("catalyst dsn could not be resolved")
	}

	dblinkConn := resolveDSN(voilaAccountDSN, DefaultVoilaAccountResource)

	// 2. Connect to Catalyst (Postgres)
	db, err := connectDB(catalystDSN)
	if err != nil {
		return nil, fmt.Errorf("catalyst db error: %w", err)
	}

	// 3. Query Discrepancy
	// Status 4 is CANCELED. We are looking for Canceled orders where points were redeemed
	// but no refund record exists in voila_account.account_point.
	query := fmt.Sprintf(`
		SELECT 
			o.id,
			o.order_number,
			oc.customer_id,
			SUM(d.amount) AS total_point_redeemed
		FROM voila.tr_order o
		INNER JOIN voila.tr_order_customer oc ON oc.order_id = o.id
		INNER JOIN voila.tr_order_discount d ON d.order_id = o.id AND d.code = 'point_redeemed' AND d.deleted_at IS NULL
		WHERE o.status_id = 4
		AND o.is_deleted = false
		AND o.sales_channel_code != 'RESELLER'
		AND NOT EXISTS (
			SELECT 1
			FROM public.dblink('%s',
				$$
				SELECT order_id
				FROM public.account_point
				WHERE source = 'refund'
				$$
			) AS ap(order_id INT)
			WHERE ap.order_id = o.id
		)
		GROUP BY 
			o.id, o.order_number, oc.customer_id
		LIMIT 100
	`, dblinkConn)

	var discrepancies []PointDiscrepancy
	err = db.Raw(query).Scan(&discrepancies).Error
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}

	if len(discrepancies) == 0 {
		return nil, nil
	}

	return formatMarkdown(discrepancies), nil
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
	// Ensure search_path is set to voila
	if !strings.Contains(dsn, "search_path") {
		if strings.Contains(dsn, "?") {
			dsn += "&search_path=voila,public"
		} else if !strings.Contains(dsn, "host=") {
			dsn += "?search_path=voila,public"
		}
	}
	return gorm.Open(postgres.Open(dsn), config)
}

func formatMarkdown(data []PointDiscrepancy) string {
	var sb strings.Builder
	sb.WriteString("##### Hi @channel, Ditemukan order CANCELED yang point-nya sudah di-redeem tapi belum ada data refund point-nya di voila_account, tolong dicek yah..\n")
	sb.WriteString("| ID | Order Number | Customer ID | Points Redeemed | Link |\n")
	sb.WriteString("| :--- | :--- | :--- | :--- | :--- |\n")

	for _, d := range data {
		catLink := fmt.Sprintf("%s/voila/order/order-detail/%s", XMS_CATALYST_BASE_URL, d.OrderNumber)
		sb.WriteString(fmt.Sprintf("| %d | %s | %d | %.2f | [View Detail](%s) |\n",
			d.ID, d.OrderNumber, d.CustomerID, d.TotalPointRedeemed, catLink))
	}
	return sb.String()
}
