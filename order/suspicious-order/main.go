package inner

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	wmill "github.com/windmill-labs/windmill-go-client"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

const (
	XMS_CATALYST_ORDER_URL_VOILA     = "https://xms.ctlyst.id/voila/order"
	XMS_CATALYST_ORDER_URL_JAMTANGAN = "https://xms.ctlyst.id/jamtangan/order"

	DefaultCatalystVoilaResource     = "u/mirza/catalyst_xms_postgresql_voila_prod"
	DefaultCatalystJamtanganResource = "u/mirza/catalyst_xms_postgresql_jamtangan_prod"

	ScanWindow     = 24 * time.Hour
	OrderThreshold = 20
)

type SuspiciousCustomer struct {
	CustomerID      int64  `gorm:"column:customer_id"`
	CustomerName    string `gorm:"column:customer_name"`
	CustomerPhone   string `gorm:"column:customer_phone"`
	CustomerEmail   string `gorm:"column:customer_email"`
	TotalOrders     int    `gorm:"column:total_orders"`
	CancelledOrders int    `gorm:"column:cancelled_orders"`
	SuccessOrders   int    `gorm:"column:success_orders"`
}

func Main(xmsCatalystDSN string, schemaName string) (interface{}, error) {
	if schemaName == "" {
		schemaName = "voila"
	}
	resource := DefaultCatalystVoilaResource
	if schemaName == "jamtangan" {
		resource = DefaultCatalystJamtanganResource
	}

	catalystDSN := resolveDSN(xmsCatalystDSN, resource)
	if catalystDSN == "" {
		return nil, fmt.Errorf("catalyst dsn could not be resolved")
	}

	db, err := connectDB(catalystDSN, schemaName)
	if err != nil {
		return nil, fmt.Errorf("catalyst db error: %w", err)
	}

	fmt.Println("[INFO] Scanning last 24 hours for customers with >= 20 orders (cancelled vs success)...")

	var results []SuspiciousCustomer
	query := fmt.Sprintf(`
		SELECT
			toc.customer_id,
			MIN(toc.customer_name)  AS customer_name,
			MIN(toc.customer_phone) AS customer_phone,
			MIN(toc.customer_email) AS customer_email,
			COUNT(*)                AS total_orders,
			COUNT(*) FILTER (WHERE o.status_id = 4)  AS cancelled_orders,
			COUNT(*) FILTER (WHERE o.status_id != 4) AS success_orders
		FROM %s.tr_order o
		JOIN %s.tr_order_customer toc ON toc.order_id = o.id
		WHERE o.created_at >= ?
		AND o.is_deleted = false
		AND o.is_test = false
		AND o.order_number IS NOT NULL
		AND o.status_id != 8
		AND toc.customer_id != 0
		GROUP BY toc.customer_id
		HAVING COUNT(*) >= ?
		ORDER BY total_orders DESC
	`, schemaName, schemaName)
	err = db.Raw(query, time.Now().Add(-ScanWindow), OrderThreshold).Scan(&results).Error

	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}

	if len(results) == 0 {
		fmt.Println("[INFO] No suspicious order patterns found in the last 24 hours.")
		return nil, nil
	}

	fmt.Printf("[WARN] Found %d customers with suspicious order activity (>= %d orders in 24 hours)\n",
		len(results), OrderThreshold)

	return formatMarkdown(results, schemaName), nil
}

func resolveDSN(provided, resourcePath string) string {
	if strings.HasPrefix(provided, "postgres://") {
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

	return fmt.Sprintf("postgres://%v:%v@%v:%v/%v",
		m["user"], m["password"], m["host"], m["port"], m["dbname"])
}

func connectDB(dsn, schemaName string) (*gorm.DB, error) {
	config := &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: ""},
	}
	if !strings.Contains(dsn, "search_path") {
		if strings.Contains(dsn, "?") {
			dsn += "&search_path=" + schemaName
		} else {
			dsn += "?search_path=" + schemaName
		}
	}
	return gorm.Open(postgres.Open(dsn), config)
}

func formatMarkdown(results []SuspiciousCustomer, schemaName string) string {
	orderURL := XMS_CATALYST_ORDER_URL_VOILA
	if schemaName == "jamtangan" {
		orderURL = XMS_CATALYST_ORDER_URL_JAMTANGAN
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("##### **SUSPICIOUS ORDER** — Hi @channel, Terdapat **%d customer** dengan order mencurigakan (>= %d order dalam 24 jam), minta tolong dicek yah..\n",
		len(results), OrderThreshold))
	sb.WriteString("| Customer ID | Nama | Phone | Email | Total | Canceled | Success | Link |\n")
	sb.WriteString("| :--- | :--- | :--- | :--- | ---: | ---: | ---: | :--- |\n")

	for _, r := range results {
		keyword := r.CustomerEmail
		if keyword == "" {
			keyword = r.CustomerName
		}
		link := fmt.Sprintf("%s?status_code=all&page=1&keyword=%s",
			orderURL, url.QueryEscape(keyword))
		sb.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %d | %d | %d | [Lihat Order](%s) |\n",
			r.CustomerID,
			emptyIfBlank(r.CustomerName),
			emptyIfBlank(r.CustomerPhone),
			emptyIfBlank(r.CustomerEmail),
			r.TotalOrders,
			r.CancelledOrders,
			r.SuccessOrders,
			link,
		))
	}

	return sb.String()
}

func emptyIfBlank(v string) string {
	if v == "" {
		return "-"
	}
	return v
}
