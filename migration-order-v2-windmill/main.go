package inner

import (
	"context"
	"fmt"
	"strings"
	"time"

	wmill "github.com/windmill-labs/windmill-go-client"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	DefaultCatalystVoilaResource           = "u/mirza/catalyst_xms_postgresql_voila_dev"
	DefaultCatalystJamtanganResource       = "u/mirza/catalyst_xms_postgresql_jt_dev"
	DefaultMongoResource                   = "f/flows_engineering/xms_catalyst_mongo_dev"
	ProcessingStatusCompleted        int32 = 19
)

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

type OrderShipping struct {
	ID                 int64
	TrackingCode       string
	DropshipName       string
	CourierID          int32
	CourierServiceCode string
}

type Fulfillment struct {
	ID                 int64
	OrderID            int64
	ProcessingStatusID *int32
	ProcessingMethod   *string
	IsReplaced         *bool
}

type SubStatus struct {
	ID            int32
	OrderStatusID int32
}

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

type FulfillmentProductRow struct {
	ID          int64
	VariantID   int32
	OrderItemID int64
}

type ItemCodeRow struct {
	ID            int64
	VariantID     int32
	OrderItemID   int64
	FulfillmentID int64
}

type FilterParam struct {
	StartDate    string `bson:"start_date"`
	EndDate      string `bson:"end_date"`
	OrderNumbers string `bson:"order_numbers"`
}

type MigrationLog struct {
	OrderID          int64       `bson:"order_id"`
	OrderNumber      string      `bson:"order_number"`
	Schema           string      `bson:"schema"`
	Case             string      `bson:"case"`
	Status           string      `bson:"status"`
	Action           string      `bson:"action"`
	Detail           string      `bson:"detail"`
	ProcessingMethod string      `bson:"processing_method"`
	FulfillmentIDs   []int64     `bson:"fulfillment_ids"`
	OrderVersion     int         `bson:"order_version"`
	Filter           FilterParam `bson:"filter"`
	MigratedAt       time.Time   `bson:"migrated_at"`
}

type MigrationResult struct {
	OrderID     int64
	OrderNumber string
	Action      string
	Status      string
	Detail      string
}

type MongoRepository struct {
	client *mongo.Client
	dbName string
}

func newMongo(client *mongo.Client, dbName string) *MongoRepository {
	return &MongoRepository{client: client, dbName: dbName}
}

func (r *MongoRepository) saveMigrationLog(ctx context.Context, log *MigrationLog) error {
	coll := r.client.Database(r.dbName).Collection("migration_order_v2_log")
	_, err := coll.InsertOne(ctx, log)
	if err != nil {
		return fmt.Errorf("insert migration log failed: %w", err)
	}
	return nil
}

type Usecase struct {
	db           *gorm.DB
	mongoRepo    *MongoRepository
	schema       string
	startDate    string
	endDate      string
	orderNumbers string
}

func newUsecase(db *gorm.DB, mongoRepo *MongoRepository, schema, startDate, endDate, orderNumbers string) *Usecase {
	return &Usecase{
		db: db, mongoRepo: mongoRepo, schema: schema,
		startDate: startDate, endDate: endDate, orderNumbers: orderNumbers,
	}
}

func (u *Usecase) processOrders() ([]MigrationResult, error) {
	orders, err := queryOrders(u.db, u.schema, u.startDate, u.endDate, u.orderNumbers)
	if err != nil {
		return nil, err
	}
	if len(orders) == 0 {
		return nil, nil
	}

	ffMap, err := queryFulfillments(u.db, u.schema, orderIDs(orders))
	if err != nil {
		return nil, err
	}

	itemsByOrder, err := queryOrderItems(u.db, u.schema, orderIDs(orders))
	if err != nil {
		return nil, err
	}

	brandNames, err := resolveBrandNames(u.db, u.schema, itemsByOrder)
	if err != nil {
		return nil, err
	}

	consignByOrder, err := resolveConsignmentData(u.db, u.schema, orderIDs(orders))
	if err != nil {
		return nil, err
	}

	var results []MigrationResult
	for _, o := range orders {
		fulfillments := ffMap[o.ID]
		items := itemsByOrder[o.ID]
		consign := consignByOrder[o.ID]

		r := u.processOrder(&o, fulfillments, items, consign, brandNames)
		results = append(results, r)
	}

	return results, nil
}

func (u *Usecase) isRejectedNoFF(order *Order) bool {
	return order.StatusID == 6 || order.StatusID == 4 || order.StatusID == 8 || order.StatusID == 1
}

func (u *Usecase) processOrder(order *Order, fulfillments []Fulfillment,
	items []OrderItem, consign *ConsignmentData,
	brandNames map[int32]string) MigrationResult {

	r := MigrationResult{OrderID: order.ID, OrderNumber: order.OrderNumber}

	if len(fulfillments) == 0 && u.isRejectedNoFF(order) {
		r.Action = "SKIP"
		r.Status = "SKIPPED"
		r.Detail = "rejected/edited order, no fulfillment found"
		u.saveLog(r, "SKIP", nil, "")
		return r
	}

	tx := u.db.Begin()

	statusIDs := []int32{order.StatusID}
	subStatusIDs := []int32{ProcessingStatusCompleted}
	if err := updateOrder(tx, u.schema, order.ID, statusIDs, subStatusIDs); err != nil {
		tx.Rollback()
		r.Action = "UPDATE ORDER"
		r.Status = "ERROR"
		r.Detail = fmt.Sprintf("update order failed: %v", err)
		return r
	}

	var allFFIDs []int64
	caseStr := ""

	coveredVariants, err := queryCoveredVariants(tx, u.schema, order.ID)
	if err != nil {
		tx.Rollback()
		r.Action = "CREATE FF"
		r.Status = "ERROR"
		r.Detail = fmt.Sprintf("query covered variants failed: %v", err)
		return r
	}

	var uncoveredItems []OrderItem
	for _, item := range items {
		if !coveredVariants[item.ID] {
			uncoveredItems = append(uncoveredItems, item)
		}
	}

	if len(uncoveredItems) > 0 {
		var ffID int64
		isConsignOrder := uncoveredItems[0].IsConsign

		if order.ShippingMethod == "CARRY_OUT" && !isConsignOrder {
			ffID, err = u.processCarryOutCreate(tx, order, uncoveredItems, brandNames)
			caseStr = "CREATE_CARRY_OUT"
		} else if isConsignOrder {
			ffID, err = u.processConsignCreate(tx, order, uncoveredItems, consign, brandNames)
			caseStr = "CREATE_CONSIGN"
		} else {
			ffID, err = u.processHomeDeliveryCreate(tx, order, uncoveredItems, consign, brandNames)
			caseStr = "CREATE_HOME_DELIVERY"
		}
		if err != nil {
			tx.Rollback()
			r.Action = "CREATE FF"
			r.Status = "ERROR"
			r.Detail = err.Error()
			u.saveLog(r, caseStr, nil, "")
			return r
		}
		allFFIDs = append(allFFIDs, ffID)
	}

	if len(fulfillments) > 0 {
		if err := u.processUpdateFulfillments(tx, order, fulfillments, items); err != nil {
			tx.Rollback()
			r.Action = "UPDATE FF"
			r.Status = "ERROR"
			r.Detail = err.Error()
			u.saveLog(r, "UPDATE_FF", nil, "")
			return r
		}
		for _, f := range fulfillments {
			allFFIDs = append(allFFIDs, f.ID)
		}
		if caseStr == "" {
			caseStr = "UPDATE_FF"
		} else {
			caseStr = "MIXED_" + caseStr
		}
	}

	if len(allFFIDs) == 0 {
		tx.Rollback()
		r.Action = "SKIP"
		r.Status = "SKIPPED"
		r.Detail = "no items need fulfillment"
		u.saveLog(r, "SKIP", nil, "")
		return r
	}

	tx.Commit()
	r.Action = caseStr
	r.Status = "OK"
	r.Detail = fmt.Sprintf("fulfillments: %v", allFFIDs)
	u.saveLog(r, caseStr, allFFIDs, order.ShippingMethod)
	return r
}

func (u *Usecase) generateFulfillmentCode(tx *gorm.DB, o *Order) (string, error) {
	buCode := "V"
	if u.schema == "jamtangan" {
		buCode = "J"
	}

	lastCode, err := getLastFfCode(tx, u.schema)
	if err != nil {
		return "", fmt.Errorf("get last ff code failed: %w", err)
	}

	incremental := 1
	if lastCode != "" && len(lastCode) >= 4 {
		lastFour := lastCode[len(lastCode)-4:]
		fmt.Sscanf(lastFour, "%d", &incremental)
		incremental++
	}

	orderNum := normalizeDigits(o.OrderNumber, 3)
	yymmdd := time.Now().Format("060102")
	code := fmt.Sprintf("%s%s%s%04d", buCode, yymmdd, orderNum, incremental)

	return code, nil
}

func (u *Usecase) processCarryOutCreate(tx *gorm.DB, order *Order,
	items []OrderItem, brandNames map[int32]string) (int64, error) {

	code, err := u.generateFulfillmentCode(tx, order)
	if err != nil {
		return 0, err
	}

	storeName := resolveOfficeName(tx, u.schema, order.OfficeID)
	shipping := queryOrderShipping(tx, u.schema, order.ID)

	ffID, err := insertFulfillment(tx, u.schema, order, code, &FulfillmentInsertData{
		Channel:            "OFFLINE",
		StoreName:          storeName,
		OfficeID:           order.OfficeID,
		PaymentStatus:      order.PaymentProgress,
		PaymentDate:        order.ProcessedAt,
		ProcessingMethod:   "CARRY_OUT",
		ProcessingStatusID: ProcessingStatusCompleted,
		IsVisible:          false,
		AwbNumber:          shipping.TrackingCode,
		IsDropship:         shipping.DropshipName != "",
		CourierServiceID:   shipping.CourierID,
		InsuranceFee:       order.InsuranceFee,
		IsHasInsurance:     order.InsuranceFee > 0,
		ShippingFee:        order.ShippingFee,
		OrderShippingID:    shipping.ID,
		CourierServiceCode: shipping.CourierServiceCode,
	})
	if err != nil {
		return 0, err
	}

	productIDs, err := insertFulfillmentProducts(tx, u.schema, ffID, items, brandNames)
	if err != nil {
		return 0, err
	}
	productMap := make(map[int64]int64, len(items))
	for i, item := range items {
		if i < len(productIDs) {
			productMap[item.ID] = productIDs[i]
		}
	}
	if err := u.matchItemCodes(tx, ffID, order.ID, items, productMap); err != nil {
		return 0, err
	}

	return ffID, nil
}

func (u *Usecase) processHomeDeliveryCreate(tx *gorm.DB, order *Order,
	items []OrderItem, consign *ConsignmentData,
	brandNames map[int32]string) (int64, error) {

	code, err := u.generateFulfillmentCode(tx, order)
	if err != nil {
		return 0, err
	}

	officeID := order.OfficeID
	storeName := resolveOfficeName(tx, u.schema, order.OfficeID)
	if consign != nil {
		officeID = consign.OfficeID
		storeName = consign.StoreName
	}
	shipping := queryOrderShipping(tx, u.schema, order.ID)

	ffID, err := insertFulfillment(tx, u.schema, order, code, &FulfillmentInsertData{
		Channel:            order.SalesChannelCode,
		StoreName:          storeName,
		OfficeID:           officeID,
		PaymentStatus:      order.PaymentProgress,
		PaymentDate:        order.ProcessedAt,
		ProcessingMethod:   "HOME_DELIVERY",
		ProcessingStatusID: ProcessingStatusCompleted,
		IsVisible:          false,
		AwbNumber:          shipping.TrackingCode,
		IsDropship:         shipping.DropshipName != "",
		CourierServiceID:   shipping.CourierID,
		InsuranceFee:       order.InsuranceFee,
		IsHasInsurance:     order.InsuranceFee > 0,
		ShippingFee:        order.ShippingFee,
		OrderShippingID:    shipping.ID,
		CourierServiceCode: shipping.CourierServiceCode,
	})
	if err != nil {
		return 0, err
	}

	productIDs, err := insertFulfillmentProducts(tx, u.schema, ffID, items, brandNames)
	if err != nil {
		return 0, err
	}
	productMap := make(map[int64]int64, len(items))
	for i, item := range items {
		if i < len(productIDs) {
			productMap[item.ID] = productIDs[i]
		}
	}
	if err := u.matchItemCodes(tx, ffID, order.ID, items, productMap); err != nil {
		return 0, err
	}

	return ffID, nil
}

func (u *Usecase) processConsignCreate(tx *gorm.DB, order *Order,
	items []OrderItem, consign *ConsignmentData, brandNames map[int32]string) (int64, error) {

	code, err := u.generateFulfillmentCode(tx, order)
	if err != nil {
		return 0, err
	}

	officeID := order.OfficeID
	storeName := resolveOfficeName(tx, u.schema, order.OfficeID)
	if consign != nil {
		officeID = consign.OfficeID
		storeName = consign.StoreName
	}
	shipping := queryOrderShipping(tx, u.schema, order.ID)
	awbNumber := shipping.TrackingCode
	if consign != nil && consign.AwbNumber != "" {
		awbNumber = consign.AwbNumber
	}
	awbManual := "MANUAL"

	ffID, err := insertFulfillment(tx, u.schema, order, code, &FulfillmentInsertData{
		Channel:            order.SalesChannelCode,
		StoreName:          storeName,
		OfficeID:           officeID,
		PaymentStatus:      order.PaymentProgress,
		PaymentDate:        order.ProcessedAt,
		ProcessingMethod:   "HOME_DELIVERY",
		ProcessingStatusID: ProcessingStatusCompleted,
		IsVisible:          false,
		AwbNumber:          awbNumber,
		IsDropship:         shipping.DropshipName != "",
		CourierServiceID:   shipping.CourierID,
		InsuranceFee:       order.InsuranceFee,
		IsHasInsurance:     order.InsuranceFee > 0,
		ShippingFee:        order.ShippingFee,
		OrderShippingID:    shipping.ID,
		CourierServiceCode: shipping.CourierServiceCode,
		AwbSource:          &awbManual,
	})
	if err != nil {
		return 0, err
	}

	productIDs, err := insertFulfillmentProducts(tx, u.schema, ffID, items, brandNames)
	if err != nil {
		return 0, err
	}
	productMap := make(map[int64]int64, len(items))
	for i, item := range items {
		if i < len(productIDs) {
			productMap[item.ID] = productIDs[i]
		}
	}
	if err := u.matchItemCodes(tx, ffID, order.ID, items, productMap); err != nil {
		return 0, err
	}

	if consign != nil && consign.ItemCode != "" {
		codes, err := queryItemCodesByFulfillment(tx, u.schema, ffID)
		if err == nil {
			for _, c := range codes {
				updateItemCodeValue(tx, u.schema, c.ID, consign.ItemCode)
			}
		}
	}

	return ffID, nil
}

func (u *Usecase) processUpdateFulfillments(tx *gorm.DB, order *Order,
	fulfillments []Fulfillment, items []OrderItem) error {

	for _, f := range fulfillments {
		if err := u.updateFulfillmentByMethod(tx, order, &f); err != nil {
			return fmt.Errorf("update fulfillment %d failed: %w", f.ID, err)
		}
		if err := updateFulfillmentProductOrderItemID(tx, u.schema, f.ID, items); err != nil {
			return fmt.Errorf("update fulfillment product order_item_id for ff %d failed: %w", f.ID, err)
		}
		products, err := queryFulfillmentProducts(tx, u.schema, f.ID)
		if err != nil {
			return fmt.Errorf("query products for ff %d failed: %w", f.ID, err)
		}
		productMap := buildProductMap(products)
		if err := u.matchItemCodes(tx, f.ID, order.ID, items, productMap); err != nil {
			return fmt.Errorf("match item codes for ff %d failed: %w", f.ID, err)
		}
	}
	return nil
}

func (u *Usecase) updateFulfillmentByMethod(tx *gorm.DB, order *Order, f *Fulfillment) error {
	var method string = order.ShippingMethod

	isReplaced := false
	if f.IsReplaced != nil {
		isReplaced = *f.IsReplaced
	}

	processingStatusID := ProcessingStatusCompleted
	if isReplaced && f.ProcessingStatusID != nil {
		processingStatusID = *f.ProcessingStatusID
		if processingStatusID == 0 {
			processingStatusID = ProcessingStatusCompleted
		}
	}

	isVisible := determineIsVisible(method, order.ShippingMethod)

	switch method {
	case "CARRY_OUT":
		return updateFulfillmentCarryOut(tx, u.schema, f.ID, processingStatusID, isVisible)
	case "HOME_DELIVERY", "SHIPPING":
		return updateFulfillmentHomeDelivery(tx, u.schema, f.ID, processingStatusID, isVisible)
	case "CONSIGNMENT":
		return updateFulfillmentConsignment(tx, u.schema, f.ID, processingStatusID, isVisible)
	case "PRE_ORDER":
		return updateFulfillmentPreOrder(tx, u.schema, f.ID, processingStatusID, isVisible)
	case "PICKUP_IN_STORE":
		return updateFulfillmentPickupInStore(tx, u.schema, f.ID, processingStatusID, order.ShippingMethod)
	default:
		return updateFulfillmentDefault(tx, u.schema, f.ID, processingStatusID, isVisible)
	}
}

func (u *Usecase) matchItemCodes(tx *gorm.DB, fulfillmentID, orderID int64, items []OrderItem, productMap map[int64]int64) error {
	codes, err := queryItemCodesByFulfillment(tx, u.schema, fulfillmentID)
	if err != nil {
		return fmt.Errorf("query item codes for ff %d failed: %w", fulfillmentID, err)
	}
	if len(codes) == 0 {
		codes, err = queryUnmatchedItemCodes(tx, u.schema, orderID)
		if err != nil {
			return fmt.Errorf("query unmatched item codes failed: %w", err)
		}
	}

	used := make(map[int64]bool)
	for _, item := range items {
		fpID, ok := productMap[item.ID]
		if !ok {
			continue
		}

		var matchID int64
		var matchFfFilled bool
		for i := range codes {
			if used[codes[i].ID] {
				continue
			}
			if codes[i].OrderItemID == item.ID || codes[i].VariantID == item.VariantID {
				matchID = codes[i].ID
				matchFfFilled = codes[i].FulfillmentID != 0
				used[codes[i].ID] = true
				break
			}
		}
		if matchID == 0 {
			continue
		}

		if matchFfFilled {
			if err := updateItemCodeFulfillmentProduct(tx, u.schema, matchID, fpID, orderID, item.ID); err != nil {
				return fmt.Errorf("update item code %d failed: %w", matchID, err)
			}
		} else {
			if err := updateItemCodeFulfillment(tx, u.schema, matchID, fulfillmentID, fpID, orderID, item.ID); err != nil {
				return fmt.Errorf("update item code %d failed: %w", matchID, err)
			}
		}
	}
	return nil
}

func (u *Usecase) saveLog(r MigrationResult, ffCase string, fulfillmentIDs []int64, processingMethod string) {
	if u.mongoRepo == nil {
		return
	}

	doc := &MigrationLog{
		OrderID:          r.OrderID,
		OrderNumber:      r.OrderNumber,
		Schema:           u.schema,
		Case:             ffCase,
		Status:           r.Status,
		Action:           r.Action,
		Detail:           r.Detail,
		ProcessingMethod: processingMethod,
		FulfillmentIDs:   fulfillmentIDs,
		OrderVersion:     2,
		Filter: FilterParam{
			StartDate:    u.startDate,
			EndDate:      u.endDate,
			OrderNumbers: u.orderNumbers,
		},
		MigratedAt: time.Now().UTC(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := u.mongoRepo.saveMigrationLog(ctx, doc); err != nil {
		fmt.Printf("[WARN] failed to save migration log to mongo: %v\n", err)
	}
}

func queryOrders(db *gorm.DB, schema, startDate, endDate, orderNumbers string) ([]Order, error) {
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
	conditions = append(conditions, "o.completed_at + INTERVAL '5 days' < NOW()")

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
	`, schema, strings.Join(conditions, " AND "))

	var orders []Order
	err := db.Raw(query, args...).Debug().Scan(&orders).Error
	if err != nil {
		return nil, fmt.Errorf("query orders failed: %w", err)
	}
	return orders, nil
}

func queryOrderItems(db *gorm.DB, schema string, orderIDs []int64) (map[int64][]OrderItem, error) {
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
	`, schema, joinIDs(orderIDs))

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
	err := db.Raw(query).Scan(&raw).Error
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

func resolveBrandNames(db *gorm.DB, schema string, itemsByOrder map[int64][]OrderItem) (map[int32]string, error) {
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
	err := db.Raw(fmt.Sprintf(`
		SELECT id, name FROM %s.ms_brand WHERE id IN (%s)
	`, schema, joinInt32s(ids))).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("query brands failed: %w", err)
	}

	m := make(map[int32]string, len(rows))
	for _, row := range rows {
		m[row.ID] = row.Name
	}
	return m, nil
}

func resolveConsignmentData(db *gorm.DB, schema string, orderIDs []int64) (map[int64]*ConsignmentData, error) {
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
	err := db.Raw(fmt.Sprintf(`
		SELECT DISTINCT ON (oc.order_id) oc.order_id, oc.office_id, mo.name AS store_name,
			   COALESCE(oc.awb_number, '') AS awb_number,
			   COALESCE(oc.item_codes[1]::text, '') AS item_code
		FROM %s.tr_order_consign oc
		JOIN %s.ms_office mo ON mo.id = oc.office_id
		WHERE oc.order_id IN (%s)
	`, schema, schema, joinIDs(orderIDs))).Scan(&rows).Error
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

func resolveOfficeName(tx *gorm.DB, schema string, officeID int32) string {
	var name string
	tx.Raw(fmt.Sprintf("SELECT name FROM %s.ms_office WHERE id = ?", schema), officeID).Scan(&name)
	return name
}

func queryOrderShipping(tx *gorm.DB, schema string, orderID int64) *OrderShipping {
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
	`, schema), orderID).Scan(&s)
	return &s
}

func updateOrder(tx *gorm.DB, schema string, orderID int64, statusIDs, subStatusIDs []int32) error {
	statusStr := formatArray(statusIDs)
	subStatusStr := formatArray(subStatusIDs)

	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_order
		SET status_ids = ?::int4[], sub_status_ids = ?::int4[], order_version = 2
		WHERE id = ?
	`, schema), statusStr, subStatusStr, orderID).Error
}

func queryFulfillments(db *gorm.DB, schema string, orderIDs []int64) (map[int64][]Fulfillment, error) {
	if len(orderIDs) == 0 {
		return nil, nil
	}
	query := fmt.Sprintf(`
		SELECT f.id, f.order_id, f.processing_status_id,
			   f.processing_method::text, f.is_replaced
		FROM %s.tr_fulfillment f
		WHERE f.is_replaced = false AND f.deleted_at IS NULL AND f.order_id IN (%s)
		ORDER BY f.id
	`, schema, joinIDs(orderIDs))

	var ffs []Fulfillment
	err := db.Raw(query).Scan(&ffs).Error
	if err != nil {
		return nil, fmt.Errorf("query fulfillments failed: %w", err)
	}

	m := make(map[int64][]Fulfillment)
	for _, f := range ffs {
		m[f.OrderID] = append(m[f.OrderID], f)
	}
	return m, nil
}

func resolveSubStatusByOrderStatus(tx *gorm.DB, schema string, orderStatusID, subStatusID int32) ([]SubStatus, error) {
	var sss []SubStatus
	err := tx.Raw(fmt.Sprintf(`
		SELECT id, order_status_id FROM %s.ms_order_sub_status
		WHERE order_status_id = ? AND id = ?
	`, schema), orderStatusID, subStatusID).Scan(&sss).Error
	return sss, err
}

func resolveSubStatusByID(tx *gorm.DB, schema string, id int32) (*SubStatus, error) {
	var ss SubStatus
	err := tx.Raw(fmt.Sprintf(`
		SELECT id, order_status_id FROM %s.ms_order_sub_status WHERE id = ?
	`, schema), id).Scan(&ss).Error
	if err != nil {
		return nil, err
	}
	return &ss, nil
}

func getLastFfCode(tx *gorm.DB, schema string) (string, error) {
	var code string
	err := tx.Raw(fmt.Sprintf(`
		SELECT code FROM %s.tr_fulfillment ORDER BY id DESC LIMIT 1
	`, schema)).Scan(&code).Error
	if err != nil {
		return "", nil
	}
	return code, nil
}

func generateFulfillmentSeq(tx *gorm.DB, schema string) (int64, error) {
	var seq int64
	err := tx.Raw(fmt.Sprintf("SELECT nextval('%s.tr_fulfillment_id_seq')", schema)).Scan(&seq).Error
	return seq, err
}

func insertFulfillment(tx *gorm.DB, schema string, o *Order, code string, data *FulfillmentInsertData) (int64, error) {
	query := fmt.Sprintf(`
		INSERT INTO %s.tr_fulfillment
			(code, order_id, channel, store_name, office_id,
			 payment_status, payment_date, processing_method, processing_status_id,
			 is_visible, order_number, order_reference, awb_number, is_dropship,
			 courier_service_id, insurance_fee, is_has_insurance, shipping_fee,
			 order_shipping_id, courier_service_code, awb_source,
			 expired_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?::text::%s.processing_method_enum,
				?, ?, ?, ?, ?, ?,
				?, ?, ?, ?,
				?, ?, ?,
				NOW() + INTERVAL '1 DAY', NOW(), NOW())
		RETURNING id
	`, schema, schema)

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

func insertFulfillmentProducts(tx *gorm.DB, schema string, fulfillmentID int64,
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
		`, schema),
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

func updateFulfillmentProductOrderItemID(tx *gorm.DB, schema string, fulfillmentID int64, items []OrderItem) error {
	for _, item := range items {
		res := tx.Exec(fmt.Sprintf(`
			UPDATE %s.tr_fulfillment_product
			SET order_item_id = ?
			WHERE fulfillment_id = ? AND variant_id = ? AND (order_item_id IS NULL OR order_item_id = 0)
		`, schema),
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

func queryFulfillmentProducts(tx *gorm.DB, schema string, fulfillmentID int64) ([]FulfillmentProductRow, error) {
	var rows []FulfillmentProductRow
	err := tx.Raw(fmt.Sprintf(`
		SELECT id, variant_id, COALESCE(order_item_id, 0) AS order_item_id
		FROM %s.tr_fulfillment_product
		WHERE fulfillment_id = ? ORDER BY id
	`, schema), fulfillmentID).Scan(&rows).Error
	return rows, err
}

func queryCoveredVariants(tx *gorm.DB, schema string, orderID int64) (map[int64]bool, error) {
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
	`, schema, schema), orderID).Scan(&rows).Error
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
		`, schema), orderID).Scan(&totals)

		for _, t := range totals {
			if variantQty[t.VariantID] >= t.Qty {
				var ids []int64
				tx.Raw(fmt.Sprintf(`
					SELECT id FROM %s.tr_order_item
					WHERE order_id = ? AND variant_id = ? AND is_deleted = false AND deleted_at IS NULL
				`, schema), orderID, t.VariantID).Pluck("id", &ids)
				for _, id := range ids {
					covered[id] = true
				}
			}
		}
	}

	return covered, nil
}

func queryItemCodesByFulfillment(tx *gorm.DB, schema string, fulfillmentID int64) ([]ItemCodeRow, error) {
	var rows []ItemCodeRow
	err := tx.Raw(fmt.Sprintf(`
		SELECT id, variant_id, COALESCE(order_item_id, 0) AS order_item_id, fulfillment_id
		FROM %s.tr_fulfillment_item_code
		WHERE fulfillment_id = ?
		ORDER BY id
	`, schema), fulfillmentID).Scan(&rows).Error
	return rows, err
}

func queryUnmatchedItemCodes(tx *gorm.DB, schema string, orderID int64) ([]ItemCodeRow, error) {
	var rows []ItemCodeRow
	err := tx.Raw(fmt.Sprintf(`
		SELECT id, variant_id, COALESCE(order_item_id, 0) AS order_item_id, 0 AS fulfillment_id
		FROM %s.tr_fulfillment_item_code
		WHERE order_id = ? AND fulfillment_id IS NULL
		ORDER BY id
	`, schema), orderID).Scan(&rows).Error
	return rows, err
}

func updateItemCodeFulfillment(tx *gorm.DB, schema string, id, fulfillmentID, fpID, orderID, orderItemID int64) error {
	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_fulfillment_item_code
		SET fulfillment_id = ?, fulfillment_product_id = ?, order_id = ?, order_item_id = ?
		WHERE id = ? AND fulfillment_id IS NULL
	`, schema), fulfillmentID, fpID, orderID, orderItemID, id).Error
}

func updateItemCodeFulfillmentProduct(tx *gorm.DB, schema string, id, fpID, orderID, orderItemID int64) error {
	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_fulfillment_item_code
		SET fulfillment_product_id = ?, order_id = ?, order_item_id = ?
		WHERE id = ?
	`, schema), fpID, orderID, orderItemID, id).Error
}

func updateItemCodeValue(tx *gorm.DB, schema string, id int64, itemCode string) error {
	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_fulfillment_item_code
		SET item_code = ?
		WHERE id = ?
	`, schema), itemCode, id).Error
}

func updateFulfillmentCarryOut(tx *gorm.DB, schema string, ffID int64, processingStatusID int32, isVisible bool) error {
	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_fulfillment
		SET processing_method = 'CARRY_OUT', processing_status_id = ?, is_visible = ?
		WHERE id = ?
	`, schema), processingStatusID, isVisible, ffID).Error
}

func updateFulfillmentHomeDelivery(tx *gorm.DB, schema string, ffID int64, processingStatusID int32, isVisible bool) error {
	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_fulfillment
		SET processing_method = 'HOME_DELIVERY', processing_status_id = ?, is_visible = ?
		WHERE id = ?
	`, schema), processingStatusID, isVisible, ffID).Error
}

func updateFulfillmentConsignment(tx *gorm.DB, schema string, ffID int64, processingStatusID int32, isVisible bool) error {
	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_fulfillment
		SET processing_method = 'CONSIGNMENT', processing_status_id = ?, is_visible = ?
		WHERE id = ?
	`, schema), processingStatusID, isVisible, ffID).Error
}

func updateFulfillmentPreOrder(tx *gorm.DB, schema string, ffID int64, processingStatusID int32, isVisible bool) error {
	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_fulfillment
		SET processing_method = 'PRE_ORDER', processing_status_id = ?, is_visible = ?
		WHERE id = ?
	`, schema), processingStatusID, isVisible, ffID).Error
}

func updateFulfillmentPickupInStore(tx *gorm.DB, schema string, ffID int64, processingStatusID int32, shippingMethod string) error {
	visible := shippingMethod != "OTHER_STORE"
	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_fulfillment
		SET processing_method = 'PICKUP_IN_STORE', processing_status_id = ?, is_visible = ?
		WHERE id = ?
	`, schema), processingStatusID, visible, ffID).Error
}

func updateFulfillmentDefault(tx *gorm.DB, schema string, ffID int64, processingStatusID int32, isVisible bool) error {
	return tx.Exec(fmt.Sprintf(`
		UPDATE %s.tr_fulfillment
		SET processing_status_id = ?, is_visible = ?
		WHERE id = ?
	`, schema), processingStatusID, isVisible, ffID).Error
}

func formatArray(ids []int32) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%d", id)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

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

func normalizeDigits(s string, n int) string {
	if len(s) <= n {
		return fmt.Sprintf("%0*s", n, s)
	}
	return s[len(s)-n:]
}

func determineIsVisible(processingMethod, shippingMethod string) bool {
	switch processingMethod {
	case "CONSIGNMENT", "CARRY_OUT", "PRE_ORDER":
		return false
	case "HOME_DELIVERY", "SHIPPING":
		return true
	case "PICKUP_IN_STORE":
		return shippingMethod != "OTHER_STORE"
	default:
		return true
	}
}

func orderIDs(orders []Order) []int64 {
	ids := make([]int64, len(orders))
	for i, o := range orders {
		ids[i] = o.ID
	}
	return ids
}

func buildProductMap(products []FulfillmentProductRow) map[int64]int64 {
	m := make(map[int64]int64, len(products))
	for _, p := range products {
		if p.OrderItemID > 0 {
			m[p.OrderItemID] = p.ID
		}
	}
	return m
}

func resolveMongoURI(provided, resourcePath string) string {
	if strings.HasPrefix(provided, "mongodb://") || strings.HasPrefix(provided, "mongodb+srv://") {
		return provided
	}

	path := resourcePath
	if provided != "" {
		path = provided
	}

	res, err := wmill.GetResource(path)
	if err == nil {
		if m, ok := res.(map[string]interface{}); ok {
			db, _ := m["db"].(string)

			var user, pass string
			if cred, ok := m["credential"].(map[string]interface{}); ok {
				user, _ = cred["username"].(string)
				pass, _ = cred["password"].(string)
			}

			var host string
			var port interface{} = 27017
			if servers, ok := m["servers"].([]interface{}); ok && len(servers) > 0 {
				if s, ok := servers[0].(map[string]interface{}); ok {
					host, _ = s["host"].(string)
					port = s["port"]
				}
			}

			if host != "" {
				return fmt.Sprintf("mongodb://%s:%s@%s:%v/%s?authSource=admin&directConnection=true", user, pass, host, port, db)
			}
		}
	}

	if provided != "" && !strings.HasPrefix(provided, "f/") && !strings.HasPrefix(provided, "u/") {
		return provided
	}

	return ""
}

func extractDBName(uri string) string {
	if strings.HasPrefix(uri, "mongodb://") || strings.HasPrefix(uri, "mongodb+srv://") {
		lastSlash := strings.LastIndex(uri, "/")
		if lastSlash != -1 {
			dbPart := uri[lastSlash+1:]
			qIdx := strings.Index(dbPart, "?")
			if qIdx != -1 {
				dbPart = dbPart[:qIdx]
			}
			if dbPart != "" {
				return dbPart
			}
		}
	}
	return "voila"
}

func formatResults(results []MigrationResult, schema, startDate, endDate string) string {
	var success, failed, skipped int
	var rows []string

	for _, r := range results {
		switch r.Status {
		case "OK":
			success++
		case "ERROR":
			failed++
		case "SKIPPED":
			skipped++
		}
		rows = append(rows, fmt.Sprintf("| %d | %s | %s | %s | %s |",
			r.OrderID, r.OrderNumber, r.Action, r.Status, r.Detail))
	}

	total := len(results)
	out := fmt.Sprintf("##### Migration Order V2 — %s, %s to %s\n\n", schema, startDate, endDate)
	out += "| Order ID | Order Number | Action | Status | Detail |\n"
	out += "|---|---|---|---|---|\n"
	out += strings.Join(rows, "\n")
	out += fmt.Sprintf("\n\n**Summary:** %d processed, %d success, %d error, %d skipped",
		total, success, failed, skipped)

	return out
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
		m["user"], m["password"], m["host"], m["port"], m["dbname"],
	)
}

func connectDB(dsn string) (*gorm.DB, error) {
	config := &gorm.Config{}
	return gorm.Open(postgres.Open(dsn), config)
}

func Main(migrationParams struct {
	Schema       string `json:"schema"`
	OrderNumbers string `json:"order_numbers"`
	StartDate    string `json:"start_date"`
	EndDate      string `json:"end_date"`
}) (interface{}, error) {
	var (
		xmsCatalystDSN, mongoResourceOrURI string
	)
	catalystResource := DefaultCatalystVoilaResource
	if migrationParams.Schema == "jamtangan" {
		catalystResource = DefaultCatalystJamtanganResource
	}
	catalystDSN := resolveDSN(xmsCatalystDSN, catalystResource)
	if catalystDSN == "" {
		return nil, fmt.Errorf("catalyst dsn could not be resolved")
	}
	if migrationParams.Schema == "" {
		return nil, fmt.Errorf("schema is required")
	}
	if migrationParams.StartDate == "" && migrationParams.EndDate == "" && migrationParams.OrderNumbers == "" {
		return nil, fmt.Errorf("startDate, endDate, or orderNumbers are required")
	}

	db, err := connectDB(catalystDSN)
	if err != nil {
		return nil, fmt.Errorf("db error: %w", err)
	}

	var mongoClient *mongo.Client
	var mongoURI string
	if mongoResourceOrURI != "" {
		mongoURI = resolveMongoURI(mongoResourceOrURI, DefaultMongoResource)
		if mongoURI != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			mongoClient, err = mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
			cancel()
			if err != nil {
				return nil, fmt.Errorf("mongo connect error: %w", err)
			}
			defer mongoClient.Disconnect(context.Background())
		}
	}

	var mongoRepo *MongoRepository
	if mongoClient != nil {
		dbName := extractDBName(mongoURI)
		mongoRepo = newMongo(mongoClient, dbName)
	}

	uc := newUsecase(db, mongoRepo, migrationParams.Schema, migrationParams.StartDate, migrationParams.EndDate, migrationParams.OrderNumbers)

	results, err := uc.processOrders()
	if err != nil {
		return nil, err
	}
	if results == nil {
		return nil, nil
	}

	return formatResults(results, migrationParams.Schema, migrationParams.StartDate, migrationParams.EndDate), nil
}
