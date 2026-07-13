package inner

import (
	"fmt"
	"strings"

	wmill "github.com/windmill-labs/windmill-go-client"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	DefaultCatalystResource = "u/mirza/catalyst_xms_postgresql_voila_prod"
	DefaultLegacyResource   = "u/mirza/voila_postgresql_prod"

	MaxOrders = 10
	StartDate = "2026-02-11"
)

func Main(xmsCatalystDSN, xmsLegacyDSN string) (interface{}, error) {
	catalystDSN := resolveDSN(xmsCatalystDSN, DefaultCatalystResource)
	if catalystDSN == "" {
		return nil, fmt.Errorf("catalyst dsn could not be resolved")
	}

	dblinkConn := resolveDSN(xmsLegacyDSN, DefaultLegacyResource)

	db, err := connectDB(catalystDSN)
	if err != nil {
		return nil, fmt.Errorf("catalyst db error: %w", err)
	}

	var sb strings.Builder

	// Query 1: Orders in Catalyst not in Legacy
	query1 := fmt.Sprintf(`
		SELECT
			o.id,
			o.order_number,
			o.sales_channel_code,
			o.created_at::TEXT AS created_at
		FROM voila.tr_order o
		LEFT JOIN public.dblink(
			'%s',
			'SELECT xmsc_order_id
			 FROM public.tr_order
			 WHERE xmsc_order_id IS NOT NULL
			 AND order_status_id != 3
			 AND created_date >= DATE ''2026-02-11'''
		) AS leg(xmsc_order_id BIGINT)
			ON o.id = leg.xmsc_order_id
		WHERE o.sales_channel_code IN ('MARKETPLACE', 'OFFLINE', 'ONLINE', 'RESELLER')
		AND o.status_id != 4
		AND o.created_at >= DATE '2026-02-11'
		AND leg.xmsc_order_id IS NULL
		LIMIT 10
	`, dblinkConn)

	type OrderRow struct {
		ID               int64  `gorm:"column:id"`
		OrderNumber      string `gorm:"column:order_number"`
		SalesChannelCode string `gorm:"column:sales_channel_code"`
		CreatedAt        string `gorm:"column:created_at"`
	}

	var catOnly []OrderRow
	err = db.Raw(query1).Scan(&catOnly).Error
	if err != nil {
		return nil, fmt.Errorf("query 1 error: %w", err)
	}

	if len(catOnly) > 0 {
		sb.WriteString("##### Hi @channel, Pesanan di XMS Catalyst yang tidak ada di XMS Legacy:\n")
		sb.WriteString("| Order ID | Order Number | Channel | Created At |\n")
		sb.WriteString("| :---: | :--- | :--- | :--- |\n")
		for _, d := range catOnly {
			sb.WriteString(fmt.Sprintf("| %d | %s | %s | %s |\n", d.ID, d.OrderNumber, d.SalesChannelCode, d.CreatedAt))
		}
		sb.WriteString("\n")
	}

	// Query 2: Orders in Legacy not in Catalyst
	query2 := fmt.Sprintf(`
		SELECT
			leg.id,
			leg.order_number,
			leg.xmsc_order_id,
			leg.created_date::TEXT AS created_date
		FROM public.dblink(
			'%s',
			'SELECT id, order_number, xmsc_order_id, created_date
			 FROM public.tr_order
			 WHERE xmsc_order_id IS NOT NULL
			 AND order_status_id != 3
			 AND created_date >= DATE ''2026-02-11'''
		) AS leg(id BIGINT, order_number VARCHAR, xmsc_order_id BIGINT, created_date TIMESTAMPTZ)
		LEFT JOIN voila.tr_order o ON o.id = leg.xmsc_order_id
		WHERE o.id IS NULL
		LIMIT 10
	`, dblinkConn)

	type LegacyOnlyRow struct {
		ID          int64  `gorm:"column:id"`
		OrderNumber string `gorm:"column:order_number"`
		XmscOrderID int64  `gorm:"column:xmsc_order_id"`
		CreatedDate string `gorm:"column:created_date"`
	}

	var legOnly []LegacyOnlyRow
	err = db.Raw(query2).Scan(&legOnly).Error
	if err != nil {
		return nil, fmt.Errorf("query 2 error: %w", err)
	}

	if len(legOnly) > 0 {
		sb.WriteString("Pesanan di XMS Legacy yang tidak ada di XMS Catalyst:\n")
		sb.WriteString("| Legacy ID | Order Number | XMSC Order ID | Created Date |\n")
		sb.WriteString("| :---: | :--- | :---: | :--- |\n")
		for _, d := range legOnly {
			sb.WriteString(fmt.Sprintf("| %d | %s | %d | %s |\n", d.ID, d.OrderNumber, d.XmscOrderID, d.CreatedDate))
		}
	}

	if sb.Len() == 0 {
		return "", nil
	}

	return sb.String(), nil
}

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

	return fmt.Sprintf("host=%v user=%v password=%v dbname=%v port=%v",
		m["host"], m["user"], m["password"], m["dbname"], m["port"])
}

func connectDB(dsn string) (*gorm.DB, error) {
	config := &gorm.Config{}
	return gorm.Open(postgres.Open(dsn), config)
}
