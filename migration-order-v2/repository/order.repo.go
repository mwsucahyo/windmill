package repository

import (
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

// ─── Models ────────────────────────────────────────────────────────────────

type Order struct {
	ID               int64
	OrderNumber      string
	ReferenceNumber  string
	StatusID         int32
	ShippingMethod   string
	OfficeID         int32
	SalesChannelCode string
	PaymentProgress  string
	ProcessedAt      *time.Time
	CompletedAt      *time.Time
	ShippingFee      float64
	InsuranceFee     float64
}

type OrderItem struct {
	ID           int64
	OrderID      int64
	VariantID    int32
	VariantSKU   string
	VariantName  string
	Qty          int32
	ProductName  string
	BrandID      int32
	SellingPrice float64
	SkuUniversal string
	ImageURL     string
	IsAddOn      bool
	IsBundling   bool
	IsCouple     bool
	IsPreOrder   bool
	IsConsign    bool
}

type ConsignmentData struct {
	OfficeID  int32
	StoreName string
	AwbNumber string
	ItemCode  string
}

// ─── Repository ────────────────────────────────────────────────────────────

type Repository struct {
	DB     *gorm.DB
	Schema string
}

func New(db *gorm.DB, schema string) *Repository {
	return &Repository{DB: db, Schema: schema}
}

// ─── Order Queries ─────────────────────────────────────────────────────────

func (r *Repository) QueryOrders(startDate, endDate, orderNumbers string) ([]Order, error) {
	var conditions []string
	if startDate != "" {
		conditions = append(conditions, fmt.Sprintf("o.created_at >= '%s'::timestamp", startDate))
	}
	if endDate != "" {
		conditions = append(conditions, fmt.Sprintf("o.created_at < '%s'::timestamp + INTERVAL '1 DAY'", endDate))
	}
	conditions = append(conditions, "o.is_deleted = false")
	conditions = append(conditions, "o.deleted_at IS NULL")
	conditions = append(conditions, "o.order_version = 1")
	conditions = append(conditions, "o.status_id = 5")
	conditions = append(conditions, "o.completed_at + INTERVAL '5 days' < NOW()") // exclude orders completed more than 5 days ago

	args := []interface{}{}
	if orderNumbers != "" {
		nums := strings.Split(orderNumbers, ",")
		placeholders := make([]string, len(nums))
		for i, n := range nums {
			n = strings.TrimSpace(n)
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args = append(args, n)
		}
		conditions = append(conditions, fmt.Sprintf("o.order_number IN (%s)", strings.Join(placeholders, ",")))
	}

	query := fmt.Sprintf(`
		SELECT o.id, o.order_number, o.reference_number, o.status_id, o.shipping_method::text,
			   o.office_id, o.sales_channel_code, o.payment_progress::text,
			   o.processed_at, o.completed_at, o.shipping_fee, o.insurance_fee
		FROM %s.tr_order o
		WHERE %s
		ORDER BY o.id
	`, r.Schema, strings.Join(conditions, " AND "))

	var orders []Order
	err := r.DB.Raw(query, args...).Debug().Scan(&orders).Error
	if err != nil {
		return nil, fmt.Errorf("query orders failed: %w", err)
	}
	return orders, nil
}

func (r *Repository) QueryOrderItems(orderIDs []int64) (map[int64][]OrderItem, error) {
	if len(orderIDs) == 0 {
		return nil, nil
	}
	query := fmt.Sprintf(`
		SELECT oi.id, oi.order_id, oi.variant_id, oi.variant_sku, oi.variant_name,
			   oi.qty, oi.product_name, oi.brand_id, oi.selling_price,
			   oi.sku_universal, oi.product_image,
			   oi.is_add_on, oi.is_bundling, oi.is_couple, oi.is_pre_order,
			   oi.is_consign
		FROM %s.tr_order_item oi
		WHERE oi.order_id IN (%s) AND oi.is_deleted = false AND oi.deleted_at IS NULL
		ORDER BY oi.id
	`, r.Schema, joinIDs(orderIDs))

	type rawItem struct {
		ID           int64
		OrderID      int64
		VariantID    int32
		VariantSKU   string
		VariantName  string
		Qty          int32
		ProductName  string
		BrandID      int32
		SellingPrice float64
		SkuUniversal string
		ProductImage string
		IsAddOn      bool
		IsBundling   bool
		IsCouple     bool
		IsPreOrder   bool
		IsConsign    bool
	}

	var raw []rawItem
	err := r.DB.Raw(query).Scan(&raw).Error
	if err != nil {
		return nil, fmt.Errorf("query order items failed: %w", err)
	}

	m := make(map[int64][]OrderItem)
	for _, rw := range raw {
		m[rw.OrderID] = append(m[rw.OrderID], OrderItem{
			ID: rw.ID, OrderID: rw.OrderID, VariantID: rw.VariantID,
			VariantSKU: rw.VariantSKU, VariantName: rw.VariantName,
			Qty: rw.Qty, ProductName: rw.ProductName, BrandID: rw.BrandID,
			SellingPrice: rw.SellingPrice, SkuUniversal: rw.SkuUniversal,
			ImageURL: rw.ProductImage, IsAddOn: rw.IsAddOn,
			IsBundling: rw.IsBundling, IsCouple: rw.IsCouple, IsPreOrder: rw.IsPreOrder,
			IsConsign: rw.IsConsign,
		})
	}
	return m, nil
}

func (r *Repository) ResolveBrandNames(itemsByOrder map[int64][]OrderItem) (map[int32]string, error) {
	brandSet := make(map[int32]struct{})
	for _, items := range itemsByOrder {
		for _, item := range items {
			if item.BrandID > 0 {
				brandSet[item.BrandID] = struct{}{}
			}
		}
	}
	if len(brandSet) == 0 {
		return nil, nil
	}

	var ids []int32
	for id := range brandSet {
		ids = append(ids, id)
	}

	type brandRow struct {
		ID   int32
		Name string
	}
	var rows []brandRow
	err := r.DB.Raw(fmt.Sprintf(`
		SELECT id, name FROM %s.ms_brand WHERE id IN (%s)
	`, r.Schema, joinInt32s(ids))).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("query brands failed: %w", err)
	}

	m := make(map[int32]string, len(rows))
	for _, row := range rows {
		m[row.ID] = row.Name
	}
	return m, nil
}

func (r *Repository) ResolveConsignmentData(orderIDs []int64) (map[int64]*ConsignmentData, error) {
	if len(orderIDs) == 0 {
		return nil, nil
	}

	type consignRow struct {
		OrderID   int64  `gorm:"column:order_id"`
		OfficeID  int32  `gorm:"column:office_id"`
		StoreName string `gorm:"column:store_name"`
		AwbNumber string `gorm:"column:awb_number"`
		ItemCode  string `gorm:"column:item_code"`
	}
	var rows []consignRow
	err := r.DB.Raw(fmt.Sprintf(`
		SELECT DISTINCT ON (oc.order_id) oc.order_id, oc.office_id, mo.name AS store_name,
			   COALESCE(oc.awb_number, '') AS awb_number,
			   COALESCE(oc.item_codes[1]::text, '') AS item_code
		FROM %s.tr_order_consign oc
		JOIN %s.ms_office mo ON mo.id = oc.office_id
		WHERE oc.order_id IN (%s)
	`, r.Schema, r.Schema, joinIDs(orderIDs))).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("query consignment failed: %w", err)
	}

	m := make(map[int64]*ConsignmentData)
	for _, row := range rows {
		m[row.OrderID] = &ConsignmentData{
			OfficeID:  row.OfficeID,
			StoreName: row.StoreName,
			AwbNumber: row.AwbNumber,
			ItemCode:  row.ItemCode,
		}
	}
	return m, nil
}

func (r *Repository) ResolveOfficeName(tx *gorm.DB, officeID int32) string {
	var name string
	tx.Raw(fmt.Sprintf("SELECT name FROM %s.ms_office WHERE id = ?", r.Schema), officeID).Scan(&name)
	return name
}

type OrderShipping struct {
	ID                 int64
	TrackingCode       string
	DropshipName       string
	CourierID          int32
	CourierServiceCode string
}

func (r *Repository) QueryOrderShipping(tx *gorm.DB, orderID int64) *OrderShipping {
	var s OrderShipping
	tx.Raw(fmt.Sprintf(`
		SELECT COALESCE(id, 0) AS id,
			   COALESCE(tracking_code, '') AS tracking_code,
			   COALESCE(dropship_name, '') AS dropship_name,
			   COALESCE(courier_id, 0) AS courier_id,
			   COALESCE(courier_name_with_service, '') AS courier_service_code
		FROM %s.tr_order_shipping
		WHERE order_id = ? AND deleted_at IS NULL
		ORDER BY id DESC LIMIT 1
	`, r.Schema), orderID).Scan(&s)
	return &s
}

// ─── Order Writes ──────────────────────────────────────────────────────────

func (r *Repository) UpdateOrder(tx *gorm.DB, orderID int64, statusIDs, subStatusIDs []int32) error {
	statusStr := formatArray(statusIDs)
	subStatusStr := formatArray(subStatusIDs)

	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_order
		SET status_ids = ?::int4[], sub_status_ids = ?::int4[], order_version = 2
		WHERE id = ?
	`, r.Schema), statusStr, subStatusStr, orderID).Error
}

func formatArray(ids []int32) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%d", id)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// ─── Helpers ───────────────────────────────────────────────────────────────

func joinIDs(ids []int64) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%d", id)
	}
	return strings.Join(parts, ", ")
}

func joinInt32s(ids []int32) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%d", id)
	}
	return strings.Join(parts, ", ")
}
