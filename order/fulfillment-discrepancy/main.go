package inner

import (
	"context"
	"fmt"
	"net/http"
	"os"
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

	DefaultCatalystResource   = "u/mirza/catalyst_xms_postgresql_voila_prod"
	DefaultPrometheusVariable = "f/voila_anomalies/push_metrics_data_anomaly_monitoring"

	LookbackDuration = 30 * time.Minute
)

// --- Models ---

type OrderResult struct {
	ID          int64  `gorm:"column:id"`
	OrderNumber string `gorm:"column:order_number"`
}

// --- Main Entry ---

func Main(xmsCatalystDSN, promPushgatewayURL string) (interface{}, error) {
	ctx := context.Background()

	// 0. Resolve Prometheus URL
	pushURL := resolveVariable(promPushgatewayURL, DefaultPrometheusVariable)

	// 1. Resolve Credentials
	catalystDSN := resolveDSN(xmsCatalystDSN, DefaultCatalystResource)
	if catalystDSN == "" {
		return nil, fmt.Errorf("catalyst dsn could not be resolved")
	}

	// 2. Connect to Catalyst (Postgres)
	db, err := connectDB(catalystDSN)
	if err != nil {
		return nil, fmt.Errorf("catalyst db error: %w", err)
	}

	// 3. Query Inconsistent Data
	var orders []OrderResult
	// Query to find orders where:
	// - status is fulfillment on process (sub_status_id = 2)
	// - but no record exists in tr_fulfillment
	err = db.Raw(`
		SELECT o.id, o.order_number 
		FROM voila.tr_order o 
		LEFT JOIN voila.tr_fulfillment f ON f.order_id = o.id 
		WHERE o.status_id = 2 and o.sub_status_id = 2 AND f.id IS NULL
		AND o.created_at >= ?
	`, time.Now().Add(-LookbackDuration)).Scan(&orders).Error

	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}

	// 4. Query Total Orders in Fulfillment Process
	var totalInProcess int64
	err = db.Table("voila.tr_order").Where("sub_status_id = ? AND created_at >= ?", 2, time.Now().Add(-LookbackDuration)).Count(&totalInProcess).Error
	if err != nil {
		fmt.Fprintf(os.Stderr, "[WARN] failed to fetch total in-process orders: %v\n", err)
	}

	inconsistentCount := len(orders)
	successRate := 100.0
	if totalInProcess > 0 {
		successRate = float64(totalInProcess-int64(inconsistentCount)) / float64(totalInProcess) * 100.0
	}

	// 5. Push metrics to Prometheus
	pushMetrics(ctx, pushURL, int(totalInProcess), inconsistentCount, successRate)

	if totalInProcess == 0 {
		return "No orders in fulfillment process found in the last 30 minutes.", nil
	}

	if inconsistentCount == 0 {
		fmt.Println("[INFO] No inconsistent fulfillment data found.")
		return "Success: No inconsistent fulfillment status found in the last 30 minutes.", nil
	}

	fmt.Printf("[WARN] Found %d orders with fulfillment inconsistency out of %d total in-process orders\n", inconsistentCount, totalInProcess)
	return formatMarkdown(orders), nil
}

// --- Helper Functions ---

func resolveDSN(provided, resourcePath string) string {
	// If it's already a DSN starting with postgres://, use it directly
	if strings.HasPrefix(provided, "postgres://") {
		return provided
	}

	// Try fetching from Windmill resource
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

	// Build from parts if 'dsn' field is missing
	return fmt.Sprintf("postgres://%v:%v@%v:%v/%v",
		m["user"], m["password"], m["host"], m["port"], m["dbname"])
}

func resolveVariable(provided, variablePath string) string {
	// If it's a plain string (not a path starting with f/ or u/), use it directly
	if provided != "" && !strings.HasPrefix(provided, "f/") && !strings.HasPrefix(provided, "u/") {
		return provided
	}

	// Determine the path to use
	path := variablePath
	if provided != "" {
		path = provided
	}

	// Fetch from Windmill
	res, err := wmill.GetVariable(path)
	if err != nil {
		fmt.Printf("Warning: failed to get variable %s: %v\n", path, err)
		return provided
	}

	return res
}

func connectDB(dsn string) (*gorm.DB, error) {
	config := &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: ""},
	}
	// Ensure search_path is set to voila
	if !strings.Contains(dsn, "search_path") {
		if strings.Contains(dsn, "?") {
			dsn += "&search_path=voila"
		} else {
			dsn += "?search_path=voila"
		}
	}
	return gorm.Open(postgres.Open(dsn), config)
}

func pushMetrics(ctx context.Context, url string, total int, count int, rate float64) {
	if url == "" {
		return
	}

	payload := fmt.Sprintf(
		"order_fulfillment_sync_total_checked %d\n"+
			"order_fulfillment_sync_discrepancy_count %d\n"+
			"order_fulfillment_sync_success_rate %f\n",
		total, count, rate,
	)

	fmt.Printf("[DEBUG] Prometheus payload: %s\n", payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(payload))
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] failed to create prometheus request: %v\n", err)
		return
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[WARN] failed to push metrics to prometheus: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "[WARN] prometheus pushgateway returned status %d\n", resp.StatusCode)
	} else {
		fmt.Println("[DEBUG] successfully pushed metrics to prometheus")
	}
}

func formatMarkdown(orders []OrderResult) string {
	var sb strings.Builder
	sb.WriteString("##### Hi @channel, Ada data order yang statusnya fulfillment on process tapi data fulfillment-nya kosong, minta tolong dicek yah..\n")
	sb.WriteString("| ID | Order Number | Link |\n")
	sb.WriteString("| :--- | :--- | :--- |\n")

	for _, o := range orders {
		catLink := fmt.Sprintf("%s/voila/order/order-detail/%s", XMS_CATALYST_BASE_URL, o.OrderNumber)
		sb.WriteString(fmt.Sprintf("| %d | %s | [View Detail](%s) |\n", o.ID, o.OrderNumber, catLink))
	}
	return sb.String()
}
