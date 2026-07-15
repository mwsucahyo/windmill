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
	PaymentMethodID         = 10
	LegacyPaymentID         = 40
	LookbackHours           = 1
	// StartDate = "2026-07-03"
	// EndDate   = "2026-07-10"
)

type CatPayment struct {
	OrderID int64   `gorm:"column:reference_id"`
	RowCnt  int64   `gorm:"column:row_cnt"`
	Amount  float64 `gorm:"column:amount"`
}

type LegacyPayment struct {
	OrderID         int64   `gorm:"column:order_id"`
	OrderNumber     string  `gorm:"column:order_number"`
	XmscOrderID     int64   `gorm:"column:xmsc_order_id"`
	XmscOrderNumber string  `gorm:"column:xmsc_order_number"`
	TotalPrice      float64 `gorm:"column:total_price"`
	RowCnt          int64   `gorm:"column:row_cnt"`
	Amount          float64 `gorm:"column:amount"`
	PendingAmount   float64 `gorm:"column:pending_amount"`
}

type AnomalyRow struct {
	OrderID         int64
	OrderNumber     string
	XmscOrderID     int64
	XmscOrderNumber string
	TotalPrice      float64
	CatRowCnt       int64
	CatAmount       float64
	LegRowCnt       int64
	LegAmount       float64
	Status          string
	InsertQuery     string
}

func Main(xmsCatalystDSN, xmsLegacyDSN string) (interface{}, error) {
	catalystDSN := resolveDSN(xmsCatalystDSN, DefaultCatalystResource)
	if catalystDSN == "" {
		return nil, fmt.Errorf("catalyst dsn could not be resolved")
	}

	legacyDSN := resolveDSN(xmsLegacyDSN, DefaultLegacyResource)
	if legacyDSN == "" {
		return nil, fmt.Errorf("legacy dsn could not be resolved")
	}

	catalystDB, err := connectDB(catalystDSN)
	if err != nil {
		return nil, fmt.Errorf("catalyst db error: %w", err)
	}

	legacyDB, err := connectDB(legacyDSN)
	if err != nil {
		return nil, fmt.Errorf("legacy db error: %w", err)
	}

	var orderIDs []int64
	err = catalystDB.Raw(fmt.Sprintf(`
		SELECT id FROM tr_order
		WHERE created_at >= NOW() - INTERVAL '%d hours'
	`, LookbackHours)).Pluck("id", &orderIDs).Error
	// err = catalystDB.Raw(fmt.Sprintf(`
	// 	SELECT id FROM tr_order
	// 	WHERE created_at >= '%s' AND created_at < '%s'::timestamp + INTERVAL '1 DAY'
	// `, StartDate, EndDate)).Pluck("id", &orderIDs).Error

	if err != nil {
		return nil, fmt.Errorf("query orders failed: %w", err)
	}

	if len(orderIDs) == 0 {
		return nil, nil // no orders in date range
	}

	idStrs := make([]string, len(orderIDs))
	for i, id := range orderIDs {
		idStrs[i] = fmt.Sprintf("%d", id)
	}
	inClause := strings.Join(idStrs, ", ")

	var cat []CatPayment
	err = catalystDB.Raw(fmt.Sprintf(`
		SELECT reference_id, COUNT(*) AS row_cnt, SUM(verified_amount) AS amount
		FROM tr_payment_transaction
		WHERE payment_method_id = %d
		AND paid_status = 'PAID'
		AND reference_id IN (%s)
		GROUP BY reference_id
	`, PaymentMethodID, inClause)).Scan(&cat).Error
	if err != nil {
		return nil, fmt.Errorf("query catalyst payments failed: %w", err)
	}

	if len(cat) == 0 {
		return nil, nil // no paid orders with this payment method
	}

	catIDs := make([]string, len(cat))
	for i, p := range cat {
		catIDs[i] = fmt.Sprintf("%d", p.OrderID)
	}
	catInClause := strings.Join(catIDs, ", ")

	var leg []LegacyPayment
	err = legacyDB.Raw(fmt.Sprintf(`
		SELECT 
			o.id AS order_id, 
			o.order_number, 
			o.xmsc_order_id, 
			o.xmsc_order_number, 
			o.total_price,
			COUNT(CASE WHEN p.status = 'success' THEN 1 END) AS row_cnt,
			COALESCE(SUM(CASE WHEN p.status = 'success' THEN p.amount ELSE 0 END), 0) AS amount,
			COALESCE(SUM(CASE WHEN p.status = 'pending' THEN p.amount ELSE 0 END), 0) AS pending_amount
		FROM public.tr_order o
		LEFT JOIN public.tr_order_payment p ON p.order_id = o.id AND p.payment_id = %d
		WHERE o.xmsc_order_id IN (%s)
		GROUP BY o.id, o.order_number, o.xmsc_order_id, o.xmsc_order_number, o.total_price
	`, LegacyPaymentID, catInClause)).Scan(&leg).Error
	if err != nil {
		return nil, fmt.Errorf("query legacy payments failed: %w", err)
	}

	legByID := make(map[int64]LegacyPayment, len(leg))
	for _, l := range leg {
		legByID[l.XmscOrderID] = l
	}

	var anomalies []AnomalyRow
	for _, c := range cat {
		l, found := legByID[c.OrderID]

		a := AnomalyRow{
			XmscOrderID: c.OrderID,
			CatRowCnt:   c.RowCnt,
			CatAmount:   c.Amount,
		}

		if found {
			a.OrderID = l.OrderID
			a.OrderNumber = l.OrderNumber
			a.XmscOrderNumber = l.XmscOrderNumber
			a.TotalPrice = l.TotalPrice
			a.LegRowCnt = l.RowCnt
			a.LegAmount = l.Amount

			if c.RowCnt != l.RowCnt || c.Amount != l.Amount {
				a.Status = "MISMATCH"
				remaining := c.Amount - l.Amount

				if l.RowCnt == 0 && l.PendingAmount > 0 {
					a.InsertQuery = fmt.Sprintf("UPDATE public.tr_order_payment SET status = 'success', processed_at = NOW() WHERE order_id = %d AND payment_id = %d AND status = 'pending';",
						l.OrderID, LegacyPaymentID)
				} else {
					a.InsertQuery = fmt.Sprintf("INSERT INTO public.tr_order_payment (order_id, amount, gateway, status, processed_at, created_date, created_by, payment_id) VALUES (%d, %.2f, 'Cash', 'success', NOW(), NOW(), 'system', %d);",
						l.OrderID, remaining, LegacyPaymentID)
				}
				anomalies = append(anomalies, a)
			}
		} else {
			a.Status = "MISSING IN LEGACY"
			anomalies = append(anomalies, a)
		}
	}

	if len(anomalies) == 0 {
		return nil, nil // all cash payments match
	}

	var rows []string
	for _, a := range anomalies {
		rows = append(rows, fmt.Sprintf("| %d | %d | %s | %s | %.2f | %.2f | %.2f | %s | %s |",
			a.OrderID, a.XmscOrderID, a.OrderNumber, a.XmscOrderNumber, a.TotalPrice,
			a.CatAmount, a.LegAmount, a.Status, a.InsertQuery))
	}

	out := fmt.Sprintf("##### Hi @channel, Ada perbedaan pembayaran CASH antara Catalyst & Legacy (method=%d, lookback=%d hours) #####\n\n", PaymentMethodID, LookbackHours)
	out += "| ID | XMSC ID | Number | XMSC Number | Total Price | XMSC Amount | Voila Amount | Status | Query |\n"
	out += "|---|---|---|---|---|---|---|---|---|\n"
	out += strings.Join(rows, "\n")

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

	return fmt.Sprintf(
		"postgres://%v:%v@%v:%v/%v",
		m["user"],
		m["password"],
		m["host"],
		m["port"],
		m["dbname"],
	)
}

func connectDB(dsn string) (*gorm.DB, error) {
	config := &gorm.Config{}
	return gorm.Open(postgres.Open(dsn), config)
}
