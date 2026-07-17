package usecase

import (
	"context"
	"fmt"
	"time"

	"windmill/migration-order-v2/repository"

	"gorm.io/gorm"
)

const (
	ProcessingStatusCompleted int32 = 19
)

// ─── Usecase ───────────────────────────────────────────────────────────────

type Usecase struct {
	repo         *repository.Repository
	mongoRepo    *repository.MongoRepository
	schema       string
	startDate    string
	endDate      string
	orderNumbers string
}

func New(repo *repository.Repository, mongoRepo *repository.MongoRepository, schema, startDate, endDate, orderNumbers string) *Usecase {
	return &Usecase{
		repo: repo, mongoRepo: mongoRepo, schema: schema,
		startDate: startDate, endDate: endDate, orderNumbers: orderNumbers,
	}
}

type MigrationResult struct {
	OrderID     int64
	OrderNumber string
	Action      string
	Status      string
	Detail      string
}

// ─── Process Orders ────────────────────────────────────────────────────────

func (u *Usecase) ProcessOrders(startDate, endDate, orderNumbers string) ([]MigrationResult, error) {
	orders, err := u.repo.QueryOrders(startDate, endDate, orderNumbers)
	if err != nil {
		return nil, err
	}
	if len(orders) == 0 {
		return nil, nil
	}

	ffMap, err := u.repo.QueryFulfillments(orderIDs(orders))
	if err != nil {
		return nil, err
	}

	itemsByOrder, err := u.repo.QueryOrderItems(orderIDs(orders))
	if err != nil {
		return nil, err
	}

	brandNames, err := u.repo.ResolveBrandNames(itemsByOrder)
	if err != nil {
		return nil, err
	}

	consignByOrder, err := u.repo.ResolveConsignmentData(orderIDs(orders))
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

// ─── Decision ──────────────────────────────────────────────────────────────

func (u *Usecase) isRejectedNoFF(o *repository.Order) bool {
	return o.StatusID == 6 || o.StatusID == 4 || o.StatusID == 8 || o.StatusID == 1
}

func (u *Usecase) processOrder(order *repository.Order, fulfillments []repository.Fulfillment,
	items []repository.OrderItem, consign *repository.ConsignmentData,
	brandNames map[int32]string) MigrationResult {

	r := MigrationResult{OrderID: order.ID, OrderNumber: order.OrderNumber}

	if len(fulfillments) == 0 && u.isRejectedNoFF(order) {
		r.Action = "SKIP"
		r.Status = "SKIPPED"
		r.Detail = "rejected/edited order, no fulfillment found"
		u.saveLog(r, "SKIP", nil, "")
		return r
	}

	tx := u.repo.DB.Begin()

	// Update order (always happens for all cases)
	statusIDs := []int32{order.StatusID}
	subStatusIDs := []int32{ProcessingStatusCompleted}
	if err := u.repo.UpdateOrder(tx, order.ID, statusIDs, subStatusIDs); err != nil {
		tx.Rollback()
		r.Action = "UPDATE ORDER"
		r.Status = "ERROR"
		r.Detail = fmt.Sprintf("update order failed: %v", err)
		return r
	}

	var allFFIDs []int64
	caseStr := ""

	// ── 1. CREATE new fulfillment for uncovered items ──
	coveredVariants, err := u.repo.QueryCoveredVariants(tx, order.ID)
	if err != nil {
		tx.Rollback()
		r.Action = "CREATE FF"
		r.Status = "ERROR"
		r.Detail = fmt.Sprintf("query covered variants failed: %v", err)
		return r
	}

	var uncoveredItems []repository.OrderItem
	for _, item := range items {
		if !coveredVariants[item.ID] {
			uncoveredItems = append(uncoveredItems, item)
		}
	}

	if len(uncoveredItems) > 0 {
		var ffID int64
		// Use first uncovered item to determine consign
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

	// ── 2. UPDATE existing fulfillments ──
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

func (u *Usecase) processOrderSkip(order *repository.Order) MigrationResult {
	r := MigrationResult{OrderID: order.ID, OrderNumber: order.OrderNumber}
	r.Action = "SKIP"
	r.Status = "SKIPPED"
	r.Detail = "rejected/edited order, no fulfillment found"
	u.saveLog(r, "SKIP", nil, "")
	return r
}

// ─── Fulfillment Code ──────────────────────────────────────────────────────

func (u *Usecase) generateFulfillmentCode(tx *gorm.DB, o *repository.Order) (string, error) {
	buCode := "V"
	if u.schema == "jamtangan" {
		buCode = "J"
	}

	lastCode, err := u.repo.GetLastFfCode(tx)
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

func normalizeDigits(s string, n int) string {
	if len(s) <= n {
		return fmt.Sprintf("%0*s", n, s)
	}
	return s[len(s)-n:]
}

// ─── Process CASE 1 ────────────────────────────────────────────────────────

func (u *Usecase) processCarryOutCreate(tx *gorm.DB, order *repository.Order,
	items []repository.OrderItem, brandNames map[int32]string) (int64, error) {

	code, err := u.generateFulfillmentCode(tx, order)
	if err != nil {
		return 0, err
	}

	storeName := u.repo.ResolveOfficeName(tx, order.OfficeID)
	shipping := u.repo.QueryOrderShipping(tx, order.ID)

	ffID, err := u.repo.InsertFulfillment(tx, order, code, &repository.FulfillmentInsertData{
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

	productIDs, err := u.repo.InsertFulfillmentProducts(tx, ffID, items, brandNames)
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

// ─── Process CASE 2 ────────────────────────────────────────────────────────

func (u *Usecase) processHomeDeliveryCreate(tx *gorm.DB, order *repository.Order,
	items []repository.OrderItem, consign *repository.ConsignmentData,
	brandNames map[int32]string) (int64, error) {

	code, err := u.generateFulfillmentCode(tx, order)
	if err != nil {
		return 0, err
	}

	officeID := order.OfficeID
	storeName := u.repo.ResolveOfficeName(tx, order.OfficeID)
	if consign != nil {
		officeID = consign.OfficeID
		storeName = consign.StoreName
	}
	shipping := u.repo.QueryOrderShipping(tx, order.ID)

	ffID, err := u.repo.InsertFulfillment(tx, order, code, &repository.FulfillmentInsertData{
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

	productIDs, err := u.repo.InsertFulfillmentProducts(tx, ffID, items, brandNames)
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

// ─── Process CONSIGN Create ─────────────────────────────────────────────────

func (u *Usecase) processConsignCreate(tx *gorm.DB, order *repository.Order,
	items []repository.OrderItem, consign *repository.ConsignmentData, brandNames map[int32]string) (int64, error) {

	code, err := u.generateFulfillmentCode(tx, order)
	if err != nil {
		return 0, err
	}

	officeID := order.OfficeID
	storeName := u.repo.ResolveOfficeName(tx, order.OfficeID)
	if consign != nil {
		officeID = consign.OfficeID
		storeName = consign.StoreName
	}
	shipping := u.repo.QueryOrderShipping(tx, order.ID)
	awbNumber := shipping.TrackingCode
	if consign != nil && consign.AwbNumber != "" {
		awbNumber = consign.AwbNumber
	}
	awbManual := "MANUAL"

	ffID, err := u.repo.InsertFulfillment(tx, order, code, &repository.FulfillmentInsertData{
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

	productIDs, err := u.repo.InsertFulfillmentProducts(tx, ffID, items, brandNames)
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

	// Set item_code from consign data
	if consign != nil && consign.ItemCode != "" {
		codes, err := u.repo.QueryItemCodesByFulfillment(tx, ffID)
		if err == nil {
			for _, c := range codes {
				u.repo.UpdateItemCodeValue(tx, c.ID, consign.ItemCode)
			}
		}
	}

	return ffID, nil
}

// ─── Process CASE 3 (UPDATE) ───────────────────────────────────────────────

func (u *Usecase) processUpdateFulfillments(tx *gorm.DB, order *repository.Order,
	fulfillments []repository.Fulfillment, items []repository.OrderItem) error {

	for _, f := range fulfillments {
		if err := u.updateFulfillmentByMethod(tx, order, &f); err != nil {
			return fmt.Errorf("update fulfillment %d failed: %w", f.ID, err)
		}
		// Fill order_item_id for existing fulfillment products
		if err := u.repo.UpdateFulfillmentProductOrderItemID(tx, f.ID, items); err != nil {
			return fmt.Errorf("update fulfillment product order_item_id for ff %d failed: %w", f.ID, err)
		}
		// Fill fulfillment_id and fulfillment_product_id in item codes
		products, err := u.repo.QueryFulfillmentProducts(tx, f.ID)
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

func (u *Usecase) updateFulfillmentByMethod(tx *gorm.DB, order *repository.Order, f *repository.Fulfillment) error {
	// Derive processing method from order's shipping method
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
		return u.repo.UpdateFulfillmentCarryOut(tx, f.ID, processingStatusID, isVisible)
	case "HOME_DELIVERY", "SHIPPING":
		return u.repo.UpdateFulfillmentHomeDelivery(tx, f.ID, processingStatusID, isVisible)
	case "CONSIGNMENT":
		return u.repo.UpdateFulfillmentConsignment(tx, f.ID, processingStatusID, isVisible)
	case "PRE_ORDER":
		return u.repo.UpdateFulfillmentPreOrder(tx, f.ID, processingStatusID, isVisible)
	case "PICKUP_IN_STORE":
		return u.repo.UpdateFulfillmentPickupInStore(tx, f.ID, processingStatusID, order.ShippingMethod)
	default:
		return u.repo.UpdateFulfillmentDefault(tx, f.ID, processingStatusID, isVisible)
	}
}

// ─── Match Item Codes Helper ───────────────────────────────────────────────

func (u *Usecase) matchItemCodes(tx *gorm.DB, fulfillmentID, orderID int64, items []repository.OrderItem, productMap map[int64]int64) error {
	// Try query by fulfillment_id first (UPDATE case), fallback to order_id (CREATE case)
	codes, err := u.repo.QueryItemCodesByFulfillment(tx, fulfillmentID)
	if err != nil {
		return fmt.Errorf("query item codes for ff %d failed: %w", fulfillmentID, err)
	}
	if len(codes) == 0 {
		codes, err = u.repo.QueryUnmatchedItemCodes(tx, orderID)
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
			if err := u.repo.UpdateItemCodeFulfillmentProduct(tx, matchID, fpID, orderID, item.ID); err != nil {
				return fmt.Errorf("update item code %d failed: %w", matchID, err)
			}
		} else {
			if err := u.repo.UpdateItemCodeFulfillment(tx, matchID, fulfillmentID, fpID, orderID, item.ID); err != nil {
				return fmt.Errorf("update item code %d failed: %w", matchID, err)
			}
		}
	}
	return nil
}

func buildProductMap(products []repository.FulfillmentProductRow) map[int64]int64 {
	m := make(map[int64]int64, len(products))
	for _, p := range products {
		if p.OrderItemID > 0 {
			m[p.OrderItemID] = p.ID
		}
	}
	return m
}

// ─── determineIsVisible ────────────────────────────────────────────────────

func (u *Usecase) saveLog(r MigrationResult, ffCase string, fulfillmentIDs []int64, processingMethod string) {
	if u.mongoRepo == nil {
		return
	}

	doc := &repository.MigrationLog{
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
		Filter: repository.FilterParam{
			StartDate:    u.startDate,
			EndDate:      u.endDate,
			OrderNumbers: u.orderNumbers,
		},
		MigratedAt: time.Now().UTC(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := u.mongoRepo.SaveMigrationLog(ctx, doc); err != nil {
		fmt.Printf("[WARN] failed to save migration log to mongo: %v\n", err)
	}
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

func orderIDs(orders []repository.Order) []int64 {
	ids := make([]int64, len(orders))
	for i, o := range orders {
		ids[i] = o.ID
	}
	return ids
}
