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
	XMS_CATALYST_ORDER_URL = "https://xms.ctlyst.id/voila/order"

	DefaultCatalystResource = "u/mirza/catalyst_xms_postgresql_voila_prod"

	ScanWindow     = 1 * time.Hour
	OrderThreshold = 10
)

type SuspiciousCustomer struct {
	CustomerID      int64  `gorm:"column:customer_id"`
	CustomerName    string `gorm:"column:customer_name"`
	CustomerPhone   string `gorm:"column:customer_phone"`
	CustomerEmail   string `gorm:"column:customer_email"`
	TotalOrders     int    `gorm:"column:total_orders"`
	PeakWindowCount int    `gorm:"column:peak_window_count"`
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

	fmt.Println("[INFO] Scanning last hour for customers with >= 10 orders in any 1-hour window...")

	var results []SuspiciousCustomer
	err = db.Raw(`
			WITH candidates AS (
				SELECT toc.customer_id
				FROM voila.tr_order o
				JOIN voila.tr_order_customer toc ON toc.order_id = o.id
				WHERE o.created_at >= ?
		AND o.is_deleted = false
			AND o.is_test = false
			AND o.order_number IS NOT NULL
			AND o.status_id != 8
			AND toc.customer_id != 0
			GROUP BY toc.customer_id
			HAVING COUNT(*) >= ?
		),
		order_windows AS (
			SELECT
				toc.customer_id,
				toc.customer_name,
				toc.customer_phone,
				toc.customer_email,
				o.id,
				o.order_number,
				o.created_at,
				COUNT(*) OVER (
					PARTITION BY toc.customer_id
					ORDER BY o.created_at
					RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
				) AS window_count
			FROM voila.tr_order o
			JOIN voila.tr_order_customer toc ON toc.order_id = o.id
			WHERE o.created_at >= ?
			AND o.is_deleted = false
			AND o.is_test = false
			AND o.order_number IS NOT NULL
			AND o.status_id != 8
			AND toc.customer_id != 0
			AND toc.customer_id IN (SELECT customer_id FROM candidates)
		)
			SELECT
				customer_id,
				MIN(customer_name)  AS customer_name,
				MIN(customer_phone) AS customer_phone,
				MIN(customer_email) AS customer_email,
				COUNT(*)            AS total_orders,
				MAX(window_count)   AS peak_window_count
			FROM order_windows
			GROUP BY customer_id
			HAVING MAX(window_count) >= ?
			ORDER BY peak_window_count DESC
		`, time.Now().Add(-ScanWindow), OrderThreshold,
		time.Now().Add(-ScanWindow), OrderThreshold).Scan(&results).Error

	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}

	if len(results) == 0 {
		fmt.Println("[INFO] No suspicious order patterns found in the last hour.")
		return nil, nil
	}

	fmt.Printf("[WARN] Found %d customers with suspicious order activity (>= %d orders in any 1-hour window)\n",
		len(results), OrderThreshold)

	return formatMarkdown(results), nil
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

func connectDB(dsn string) (*gorm.DB, error) {
	config := &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: ""},
	}
	if !strings.Contains(dsn, "search_path") {
		if strings.Contains(dsn, "?") {
			dsn += "&search_path=voila"
		} else {
			dsn += "?search_path=voila"
		}
	}
	return gorm.Open(postgres.Open(dsn), config)
}

func formatMarkdown(results []SuspiciousCustomer) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("##### Hi @channel, Terdapat **%d customer** dengan order mencurigakan (>= %d order dalam 1 jam), minta tolong dicek yah..\n",
		len(results), OrderThreshold))
	sb.WriteString("| Customer ID | Nama | Phone | Email | Total Order | Peak 1 Jam | Link |\n")
	sb.WriteString("| :--- | :--- | :--- | :--- | ---: | ---: | :--- |\n")

	for _, r := range results {
		keyword := r.CustomerEmail
		if keyword == "" {
			keyword = r.CustomerName
		}
		link := fmt.Sprintf("%s?status_code=all&page=1&keyword=%s",
			XMS_CATALYST_ORDER_URL, url.QueryEscape(keyword))
		sb.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %d | %d | [Lihat Order](%s) |\n",
			r.CustomerID,
			emptyIfBlank(r.CustomerName),
			emptyIfBlank(r.CustomerPhone),
			emptyIfBlank(r.CustomerEmail),
			r.TotalOrders,
			r.PeakWindowCount,
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
