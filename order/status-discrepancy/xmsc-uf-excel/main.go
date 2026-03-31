package inner

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"

	wmill "github.com/windmill-labs/windmill-go-client"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// require gorm.io/driver/postgres v1.5.9
// require go.mongodb.org/mongo-driver v1.17.1
// require github.com/xuri/excelize/v2 v2.9.0

// --- Constants ---

const (
	XMS_CATALYST_BASE_URL = "https://xms.ctlyst.id"
	XMS_LEGACY_BASE_URL   = "https://xms.voila.id"

	DefaultCatalystResource = "u/mirza/catalyst_xms_postgresql_voila_prod"
	DefaultMongoResource    = "f/voila_anomalies/voila_mongodb_prod"

	DefaultSuccessMessage = ""
)

// --- Models (Postgres) ---

type OrderResult struct {
	ID             int64  `gorm:"column:id"`
	OrderNumber    string `gorm:"column:order_number"`
	OrderReference string `gorm:"column:reference_number"`
	StatusID       int    `gorm:"column:status_id"`
	StatusName     string `gorm:"column:status_name"`
}

// --- Models (Mongo) ---

type MongoOrder struct {
	OrderID        int64  `bson:"order_id"`
	OrderReference string `bson:"order_reference"`
	XmscOrderID    int64  `bson:"xmsc_order_id"`
	OrderStatus    struct {
		ID   int64  `bson:"id"`
		Code string `bson:"code"`
	} `bson:"order_status"`
}

type Discrepancy struct {
	OrderNumber        string
	OrderReference     string
	VoilaOrderID       int64
	CatalystStatus     int
	CatalystStatusName string
	MongoStatus        int64
	MongoStatusCode    string
}

// --- Main Entry ---

func Main(xmsCatalystDSN, mongoURI, startDate, endDate string) (interface{}, error) {
	// 1. Resolve Credentials
	catalystDSN := resolveDSN(xmsCatalystDSN, DefaultCatalystResource)
	resolvedMongoURI := resolveMongoURI(mongoURI, DefaultMongoResource)

	if catalystDSN == "" {
		return nil, fmt.Errorf("catalyst dsn could not be resolved")
	}
	if resolvedMongoURI == "" {
		return nil, fmt.Errorf("mongo uri could not be resolved")
	}

	// Parse dates
	start, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		return nil, fmt.Errorf("invalid startDate (expected YYYY-MM-DD): %v", err)
	}
	end, err := time.Parse("2006-01-02", endDate)
	if err != nil {
		return nil, fmt.Errorf("invalid endDate (expected YYYY-MM-DD): %v", err)
	}
	// Add 23:59:59 to end date to include the full day
	end = end.Add(23*time.Hour + 59*time.Minute + 59*time.Second)

	// 2. Connect to Catalyst (Postgres)
	db, err := connectDB(catalystDSN)
	if err != nil {
		return nil, fmt.Errorf("catalyst db error: %w", err)
	}

	// 3. Connect to Voila UF (MongoDB)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	mClient, err := mongo.Connect(ctx, options.Client().ApplyURI(resolvedMongoURI))
	if err != nil {
		return nil, fmt.Errorf("mongo connect error: %w", err)
	}
	defer mClient.Disconnect(ctx)

	// 4. Get Orders from Catalyst in the date range
	var orders []OrderResult
	err = db.Table("voila.tr_order o").
		Select("o.id, o.order_number, o.reference_number, o.status_id, ms.name as status_name").
		Joins("JOIN voila.ms_order_status ms ON ms.id = o.status_id").
		Where("o.status_id IN (4, 5) AND o.created_at >= ? AND o.created_at <= ?", start, end).
		Scan(&orders).Error

	if err != nil {
		return nil, fmt.Errorf("catalyst query error: %w", err)
	}

	if len(orders) == 0 {
		return "No orders found in range", nil
	}

	// 5. Compare with MongoDB
	dbName := extractDBName(resolvedMongoURI)
	mColl := mClient.Database(dbName).Collection("order")

	var diffs []Discrepancy
	for _, o := range orders {
		var mOrder MongoOrder
		filter := bson.M{"xmsc_order_id": o.ID}
		err := mColl.FindOne(ctx, filter).Decode(&mOrder)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				continue
			}
			return nil, fmt.Errorf("mongo error for order %s: %w", o.OrderNumber, err)
		}

		isDiscrepancy := false
		if o.StatusID == 5 { // Completed
			if mOrder.OrderStatus.ID != 5 && mOrder.OrderStatus.ID != 6 {
				isDiscrepancy = true
			}
		} else if o.StatusID == 4 { // Canceled
			if mOrder.OrderStatus.ID != 8 && mOrder.OrderStatus.ID != 7 && mOrder.OrderStatus.ID != 9 {
				isDiscrepancy = true
			}
		}

		if isDiscrepancy {
			orderRef := o.OrderReference
			if orderRef == "" {
				orderRef = mOrder.OrderReference
			}

			diffs = append(diffs, Discrepancy{
				OrderNumber:        o.OrderNumber,
				OrderReference:     orderRef,
				VoilaOrderID:       mOrder.OrderID,
				CatalystStatus:     o.StatusID,
				CatalystStatusName: o.StatusName,
				MongoStatus:        mOrder.OrderStatus.ID,
				MongoStatusCode:    mOrder.OrderStatus.Code,
			})
		}
	}

	if len(diffs) == 0 {
		return "No discrepancies found in range", nil
	}

	return exportToExcel(diffs)
}

// --- Helper Functions ---

func resolveDSN(provided, resourcePath string) string {
	if strings.Contains(provided, "@") || strings.Contains(provided, "host=") {
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

func resolveVariable(provided, variablePath string) string {
	if provided != "" && !strings.HasPrefix(provided, "f/") && !strings.HasPrefix(provided, "u/") {
		return provided
	}

	path := variablePath
	if provided != "" {
		path = provided
	}

	res, err := wmill.GetVariable(path)
	if err != nil {
		fmt.Printf("Warning: failed to get variable %s: %v\n", path, err)
		return provided
	}

	return res
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

	return resolveVariable(provided, path)
}

func extractDBName(uri string) string {
	lastSlash := strings.LastIndex(uri, "/")
	if lastSlash == -1 {
		return "voila"
	}

	dbPart := uri[lastSlash+1:]
	qIdx := strings.Index(dbPart, "?")
	if qIdx != -1 {
		dbPart = dbPart[:qIdx]
	}

	if dbPart == "" {
		return "voila"
	}
	return dbPart
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

func formatMongoCode(code string) string {
	if code == "" {
		return "Unknown"
	}
	parts := strings.Split(code, "_")
	for i, p := range parts {
		if len(p) > 0 {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, " ")
}

func exportToExcel(diffs []Discrepancy) (interface{}, error) {
	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	sheetName := "Discrepancy"
	f.SetSheetName("Sheet1", sheetName)

	headers := []string{"Order Number", "Order Reference", "XMS Catalyst Status", "Voila UF Status", "Catalyst Link", "Voila Link"}
	for i, h := range headers {
		cell, _ := excelize.CoordinatesToCellName(i+1, 1)
		f.SetCellValue(sheetName, cell, h)
	}

	headerStyle, _ := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{Bold: true},
		Fill: excelize.Fill{Type: "pattern", Color: []string{"#CCCCCC"}, Pattern: 1},
	})
	f.SetRowStyle(sheetName, 1, 1, headerStyle)

	sort.Slice(diffs, func(i, j int) bool {
		return diffs[i].OrderNumber < diffs[j].OrderNumber
	})

	for i, d := range diffs {
		row := i + 2
		catLink := fmt.Sprintf("%s/voila/order/order-detail/%s", XMS_CATALYST_BASE_URL, d.OrderNumber)
		voilaLink := fmt.Sprintf("%s/order/%d", XMS_LEGACY_BASE_URL, d.VoilaOrderID)

		catStatus := fmt.Sprintf("%s (%d)", d.CatalystStatusName, d.CatalystStatus)
		mongoStatus := fmt.Sprintf("%s (%d)", formatMongoCode(d.MongoStatusCode), d.MongoStatus)

		f.SetCellValue(sheetName, fmt.Sprintf("A%d", row), d.OrderNumber)
		f.SetCellValue(sheetName, fmt.Sprintf("B%d", row), d.OrderReference)
		f.SetCellValue(sheetName, fmt.Sprintf("C%d", row), catStatus)
		f.SetCellValue(sheetName, fmt.Sprintf("D%d", row), mongoStatus)
		f.SetCellValue(sheetName, fmt.Sprintf("E%d", row), catLink)
		f.SetCellValue(sheetName, fmt.Sprintf("F%d", row), voilaLink)

		f.SetCellHyperLink(sheetName, fmt.Sprintf("A%d", row), catLink, "External")
		f.SetCellHyperLink(sheetName, fmt.Sprintf("B%d", row), voilaLink, "External")
	}

	f.SetColWidth(sheetName, "A", "F", 25)

	buffer, err := f.WriteToBuffer()
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}
