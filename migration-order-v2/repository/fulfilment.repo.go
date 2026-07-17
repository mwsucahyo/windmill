package repository

import (
	"fmt"
	"time"

	"gorm.io/gorm"
)

// ─── Models ────────────────────────────────────────────────────────────────

type Fulfillment struct {
	ID                 int64
	OrderID            int64
	ProcessingStatusID *int32
	ProcessingMethod   *string
	IsReplaced         *bool
}

type SubStatus struct {
	ID            int32 `gorm:"column:id"`
	OrderStatusID int32 `gorm:"column:order_status_id"`
}

// ─── Fulfillment Queries ───────────────────────────────────────────────────

func (r *Repository) QueryFulfillments(orderIDs []int64) (map[int64][]Fulfillment, error) {
	if len(orderIDs) == 0 {
		return nil, nil
	}
	query := fmt.Sprintf(`
		SELECT f.id, f.order_id, f.processing_status_id,
			   f.processing_method::text, f.is_replaced
		FROM %s.tr_fulfillment f
		WHERE f.is_replaced = false AND f.deleted_at IS NULL AND f.order_id IN (%s)
		ORDER BY f.id
	`, r.Schema, joinIDs(orderIDs))

	var ffs []Fulfillment
	err := r.DB.Raw(query).Scan(&ffs).Error
	if err != nil {
		return nil, fmt.Errorf("query fulfillments failed: %w", err)
	}

	m := make(map[int64][]Fulfillment)
	for _, f := range ffs {
		m[f.OrderID] = append(m[f.OrderID], f)
	}
	return m, nil
}

func (r *Repository) ResolveSubStatusByOrderStatus(tx *gorm.DB, orderStatusID, subStatusID int32) ([]SubStatus, error) {
	var sss []SubStatus
	err := tx.Raw(fmt.Sprintf(`
		SELECT id, order_status_id FROM %s.ms_order_sub_status
		WHERE order_status_id = ? AND id = ?
	`, r.Schema), orderStatusID, subStatusID).Scan(&sss).Error
	return sss, err
}

func (r *Repository) ResolveSubStatusByID(tx *gorm.DB, id int32) (*SubStatus, error) {
	var ss SubStatus
	err := tx.Raw(fmt.Sprintf(`
		SELECT id, order_status_id FROM %s.ms_order_sub_status WHERE id = ?
	`, r.Schema), id).Scan(&ss).Error
	if err != nil {
		return nil, err
	}
	return &ss, nil
}

func (r *Repository) GetLastFfCode(tx *gorm.DB) (string, error) {
	var code string
	err := tx.Raw(fmt.Sprintf(`
		SELECT code FROM %s.tr_fulfillment ORDER BY id DESC LIMIT 1
	`, r.Schema)).Scan(&code).Error
	if err != nil {
		return "", nil
	}
	return code, nil
}

func (r *Repository) GenerateFulfillmentSeq(tx *gorm.DB) (int64, error) {
	var seq int64
	err := tx.Raw(fmt.Sprintf("SELECT nextval('%s.tr_fulfillment_id_seq')", r.Schema)).Scan(&seq).Error
	return seq, err
}

// ─── Fulfillment Writes ────────────────────────────────────────────────────

type FulfillmentInsertData struct {
	Channel            string
	StoreName          string
	OfficeID           int32
	PaymentStatus      string
	PaymentDate        *time.Time
	ProcessingMethod   string
	ProcessingStatusID int32
	IsVisible          bool
	AwbNumber          string
	IsDropship         bool
	CourierServiceID   int32
	InsuranceFee       float64
	IsHasInsurance     bool
	ShippingFee        float64
	OrderShippingID    int64
	CourierServiceCode string
	AwbSource          *string
}

func (r *Repository) InsertFulfillment(tx *gorm.DB, o *Order, code string, data *FulfillmentInsertData) (int64, error) {
	query := fmt.Sprintf(`
		INSERT INTO %s.tr_fulfillment
			(code, order_id, channel, store_name, office_id,
			 payment_status, payment_date, processing_method, processing_status_id,
			 is_visible, order_number, order_reference, awb_number, is_dropship,
			 courier_service_id, insurance_fee, is_has_insurance, shipping_fee,
			 order_shipping_id, courier_service_code,
			 NULLIF(?, '')::fulfillment_awb_source,
			 expired_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?::text::%s.processing_method_enum,
				?, ?, ?, ?, ?, ?,
				?, ?, ?, ?,
				?, ?, ?,
				NOW() + INTERVAL '1 DAY', NOW(), NOW())
		RETURNING id
	`, r.Schema, r.Schema)

	var id int64
	err := tx.Raw(query,
		code, o.ID, data.Channel, data.StoreName,
		data.OfficeID, data.PaymentStatus, data.PaymentDate,
		data.ProcessingMethod, data.ProcessingStatusID,
		data.IsVisible, o.OrderNumber, o.ReferenceNumber,
		data.AwbNumber, data.IsDropship,
		data.CourierServiceID, data.InsuranceFee, data.IsHasInsurance, data.ShippingFee,
		data.OrderShippingID, data.CourierServiceCode, data.AwbSource,
	).Scan(&id).Error
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (r *Repository) InsertFulfillmentProducts(tx *gorm.DB, fulfillmentID int64,
	items []OrderItem, brandNames map[int32]string) ([]int64, error) {

	var productIDs []int64
	for _, item := range items {
		brandName := brandNames[item.BrandID]
		if brandName == "" {
			brandName = fmt.Sprintf("Brand%d", item.BrandID)
		}

		var id int64
		err := tx.Raw(fmt.Sprintf(`
			INSERT INTO %s.tr_fulfillment_product
				(fulfillment_id, variant_id, variant_sku, variant_name, qty,
				 product_name, brand_name, brand_id, price, sku_universal,
				 image_url, is_add_on, is_bundling, is_couple, is_pre_order,
				 order_item_id)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			RETURNING id
		`, r.Schema),
			fulfillmentID, item.VariantID, item.VariantSKU, item.VariantName,
			item.Qty, item.ProductName, brandName, item.BrandID, item.SellingPrice,
			item.SkuUniversal, item.ImageURL, item.IsAddOn, item.IsBundling,
			item.IsCouple, item.IsPreOrder, item.ID,
		).Scan(&id).Error
		if err != nil {
			return nil, fmt.Errorf("insert fulfillment product for item %d failed: %w", item.ID, err)
		}
		productIDs = append(productIDs, id)
	}
	return productIDs, nil
}

func (r *Repository) UpdateFulfillmentProductOrderItemID(tx *gorm.DB, fulfillmentID int64, items []OrderItem) error {
	for _, item := range items {
		res := tx.Exec(fmt.Sprintf(`
			UPDATE %s.tr_fulfillment_product
			SET order_item_id = ?
			WHERE fulfillment_id = ? AND variant_id = ? AND (order_item_id IS NULL OR order_item_id = 0)
		`, r.Schema),
			item.ID, fulfillmentID, item.VariantID,
		)
		if res.Error != nil {
			return fmt.Errorf("update fulfillment product order_item_id for variant %d failed: %w", item.VariantID, res.Error)
		}
		fmt.Printf("[DEBUG] UpdateFulfillmentProductOrderItemID: ff=%d, item=%d, variant=%d, rowsAffected=%d\n",
			fulfillmentID, item.ID, item.VariantID, res.RowsAffected)
	}
	return nil
}

type FulfillmentProductRow struct {
	ID          int64
	VariantID   int32
	OrderItemID int64
}

func (r *Repository) QueryFulfillmentProducts(tx *gorm.DB, fulfillmentID int64) ([]FulfillmentProductRow, error) {
	var rows []FulfillmentProductRow
	err := tx.Raw(fmt.Sprintf(`
		SELECT id, variant_id, COALESCE(order_item_id, 0) AS order_item_id
		FROM %s.tr_fulfillment_product
		WHERE fulfillment_id = ? ORDER BY id
	`, r.Schema), fulfillmentID).Scan(&rows).Error
	return rows, err
}

func (r *Repository) QueryCoveredVariants(tx *gorm.DB, orderID int64) (map[int64]bool, error) {
	type fpRow struct {
		OrderItemID int64
		VariantID   int32
	}
	var rows []fpRow
	err := tx.Raw(fmt.Sprintf(`
		SELECT COALESCE(fp.order_item_id, 0) AS order_item_id, fp.variant_id
		FROM %s.tr_fulfillment_product fp
		JOIN %s.tr_fulfillment f ON f.id = fp.fulfillment_id
		WHERE f.order_id = ? AND f.deleted_at IS NULL
	`, r.Schema, r.Schema), orderID).Scan(&rows).Error
	if err != nil {
		return nil, err
	}

	covered := make(map[int64]bool)
	variantQty := make(map[int32]int)

	for _, r := range rows {
		if r.OrderItemID > 0 {
			covered[r.OrderItemID] = true
		} else {
			variantQty[r.VariantID]++
		}
	}

	if len(variantQty) > 0 {
		var totals []struct {
			VariantID int32
			Qty       int
		}
		tx.Raw(fmt.Sprintf(`
			SELECT variant_id, SUM(qty)::int AS qty
			FROM %s.tr_order_item
			WHERE order_id = ? AND is_deleted = false AND deleted_at IS NULL
			GROUP BY variant_id
		`, r.Schema), orderID).Scan(&totals)

		for _, t := range totals {
			if variantQty[t.VariantID] >= t.Qty {
				var ids []int64
				tx.Raw(fmt.Sprintf(`
					SELECT id FROM %s.tr_order_item
					WHERE order_id = ? AND variant_id = ? AND is_deleted = false AND deleted_at IS NULL
				`, r.Schema), orderID, t.VariantID).Pluck("id", &ids)
				for _, id := range ids {
					covered[id] = true
				}
			}
		}
	}

	return covered, nil
}

type ItemCodeRow struct {
	ID            int64
	VariantID     int32
	OrderItemID   int64
	FulfillmentID int64
}

func (r *Repository) QueryItemCodesByFulfillment(tx *gorm.DB, fulfillmentID int64) ([]ItemCodeRow, error) {
	var rows []ItemCodeRow
	err := tx.Raw(fmt.Sprintf(`
		SELECT id, variant_id, COALESCE(order_item_id, 0) AS order_item_id, fulfillment_id
		FROM %s.tr_fulfillment_item_code
		WHERE fulfillment_id = ?
		ORDER BY id
	`, r.Schema), fulfillmentID).Scan(&rows).Error
	return rows, err
}

func (r *Repository) QueryUnmatchedItemCodes(tx *gorm.DB, orderID int64) ([]ItemCodeRow, error) {
	var rows []ItemCodeRow
	err := tx.Raw(fmt.Sprintf(`
		SELECT id, variant_id, COALESCE(order_item_id, 0) AS order_item_id, 0 AS fulfillment_id
		FROM %s.tr_fulfillment_item_code
		WHERE order_id = ? AND fulfillment_id IS NULL
		ORDER BY id
	`, r.Schema), orderID).Scan(&rows).Error
	return rows, err
}

func (r *Repository) UpdateItemCodeFulfillment(tx *gorm.DB, id, fulfillmentID, fpID, orderID, orderItemID int64) error {
	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_fulfillment_item_code
		SET fulfillment_id = ?, fulfillment_product_id = ?, order_id = ?, order_item_id = ?
		WHERE id = ? AND fulfillment_id IS NULL
	`, r.Schema), fulfillmentID, fpID, orderID, orderItemID, id).Error
}

func (r *Repository) UpdateItemCodeFulfillmentProduct(tx *gorm.DB, id, fpID, orderID, orderItemID int64) error {
	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_fulfillment_item_code
		SET fulfillment_product_id = ?, order_id = ?, order_item_id = ?
		WHERE id = ?
	`, r.Schema), fpID, orderID, orderItemID, id).Error
}

func (r *Repository) UpdateItemCodeValue(tx *gorm.DB, id int64, itemCode string) error {
	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_fulfillment_item_code
		SET item_code = ?
		WHERE id = ?
	`, r.Schema), itemCode, id).Error
}

// ─── Update per Processing Method ──────────────────────────────────────────

func (r *Repository) UpdateFulfillmentCarryOut(tx *gorm.DB, ffID int64, processingStatusID int32, isVisible bool) error {
	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_fulfillment
		SET processing_method = 'CARRY_OUT', processing_status_id = ?, is_visible = ?
		WHERE id = ?
	`, r.Schema), processingStatusID, isVisible, ffID).Error
}

func (r *Repository) UpdateFulfillmentHomeDelivery(tx *gorm.DB, ffID int64, processingStatusID int32, isVisible bool) error {
	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_fulfillment
		SET processing_method = 'HOME_DELIVERY', processing_status_id = ?, is_visible = ?
		WHERE id = ?
	`, r.Schema), processingStatusID, isVisible, ffID).Error
}

func (r *Repository) UpdateFulfillmentConsignment(tx *gorm.DB, ffID int64, processingStatusID int32, isVisible bool) error {
	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_fulfillment
		SET processing_method = 'CONSIGNMENT', processing_status_id = ?, is_visible = ?
		WHERE id = ?
	`, r.Schema), processingStatusID, isVisible, ffID).Error
}

func (r *Repository) UpdateFulfillmentPreOrder(tx *gorm.DB, ffID int64, processingStatusID int32, isVisible bool) error {
	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_fulfillment
		SET processing_method = 'PRE_ORDER', processing_status_id = ?, is_visible = ?
		WHERE id = ?
	`, r.Schema), processingStatusID, isVisible, ffID).Error
}

func (r *Repository) UpdateFulfillmentPickupInStore(tx *gorm.DB, ffID int64, processingStatusID int32, shippingMethod string) error {
	visible := shippingMethod != "OTHER_STORE"
	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_fulfillment
		SET processing_method = 'PICKUP_IN_STORE', processing_status_id = ?, is_visible = ?
		WHERE id = ?
	`, r.Schema), processingStatusID, visible, ffID).Error
}

func (r *Repository) UpdateFulfillmentDefault(tx *gorm.DB, ffID int64, processingStatusID int32, isVisible bool) error {
	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_fulfillment
		SET processing_status_id = ?, is_visible = ?
		WHERE id = ?
	`, r.Schema), processingStatusID, isVisible, ffID).Error
}
