package inner

import (
	"fmt"
	"strings"
	"time"

	wmill "github.com/windmill-labs/windmill-go-client"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	DefaultCatalystResource = "u/mirza/catalyst_xms_postgresql_voila_prod"
	LookbackHours           = 24
)

type AnomalyOrder struct {
	OrderID               int64
	OrderNumber           string
	StatusName            string
	FulfillmentStatus     string
	CreatedAt             time.Time
	FulfillmentCode       string
	FulfillmentStatusCode string
}

func Main(xmsCatalystDSN string) (interface{}, error) {
	catalystDSN := resolveDSN(xmsCatalystDSN, DefaultCatalystResource)
	if catalystDSN == "" {
		return nil, fmt.Errorf("catalyst dsn could not be resolved")
	}

	db, err := connectDB(catalystDSN)
	if err != nil {
		return nil, fmt.Errorf("catalyst db error: %w", err)
	}

	var results []AnomalyOrder
	err = db.Raw(fmt.Sprintf(`
		SELECT o.id AS order_id, o.order_number,
			   ost.name AS status_name,
			   f.status::text AS fulfillment_status_code,
			   o.created_at,
			   f.code AS fulfillment_code
		FROM tr_order o
		JOIN tr_fulfillment f ON f.order_id = o.id AND f.deleted_at IS NULL AND f.is_replaced = false
		JOIN ms_order_status ost ON ost.id = o.status_id
		WHERE o.status_id IN (1, 8, 4)
		  AND (f.status IS NULL OR f.status::text != 'REJECTED')
		  AND o.is_deleted = false
		  AND o.deleted_at IS NULL
		  AND o.created_at >= NOW() - INTERVAL '%d hours'
		ORDER BY o.created_at DESC
	`, LookbackHours)).Scan(&results).Error
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	if len(results) == 0 {
		return nil, nil
	}

	out := fmt.Sprintf("##### Orders With Fulfillment (Pending/Draft/Canceled) — %d orders, last %d hours\n\n", len(results), LookbackHours)
	out += "| Order ID | Order Number | Status | FF Status | Created | FF Code |\n"
	out += "|---|---|---|---|---|---|\n"

	for _, r := range results {
		ffStatus := r.FulfillmentStatusCode
		if ffStatus == "" || ffStatus == "-" {
			ffStatus = "-"
		}
		out += fmt.Sprintf("| %d | %s | %s | %s | %s | %s |\n",
			r.OrderID, r.OrderNumber, r.StatusName, ffStatus,
			r.CreatedAt.Format("2006-01-02 15:04"),
			r.FulfillmentCode)
	}

	return out, nil
}

func resolveDSN(provided, resourcePath string) string {
	if strings.HasPrefix(provided, "postgres://") || strings.HasPrefix(provided, "postgresql://") {
		return provided
	}

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

	return fmt.Sprintf("postgres://%v:%v@%v:%v/%v",
		m["user"], m["password"], m["host"], m["port"], m["dbname"])
}

func connectDB(dsn string) (*gorm.DB, error) {
	config := &gorm.Config{}
	return gorm.Open(postgres.Open(dsn), config)
}
