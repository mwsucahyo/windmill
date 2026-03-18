package inner

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	wmill "github.com/windmill-labs/windmill-go-client"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// require gorm.io/gorm v1.25.12
// require gorm.io/driver/postgres v1.5.9
// require go.mongodb.org/mongo-driver v1.17.1

// --- Constants ---

const (
	XMS_CATALYST_BASE_URL = "https://stg-catalyst-xms-web.machtwatch.net"
	VOILA_UF_WEB_BASE_URL = "https://stg-fe-xms.machtwatch.net"

	DefaultCatalystResource = "u/mirza/catalyst_xms_postgresql_voila_stg"
	DefaultMongoResource    = "f/voila_anomalies/voila_mongodb_stg"

	LookbackDuration = 24 * time.Hour
)

// --- Models (Postgres) ---

type OrderResult struct {
	ID              int64  `gorm:"column:id"`
	OrderNumber     string `gorm:"column:order_number"`
	OrderReference  string `gorm:"column:reference_number"`
	ProvinceName    string `gorm:"column:province_name"`
	DistrictName    string `gorm:"column:district_name"`
	SubdistrictName string `gorm:"column:subdistrict_name"`
	PostalCode      string `gorm:"column:postal_code"`
}

// --- Models (Mongo) ---

type MongoOrder struct {
	OrderID        int64  `bson:"order_id"`
	OrderReference string `bson:"order_reference"`
	XmscOrderID    int64  `bson:"xmsc_order_id"`
	Address        struct {
		ProvinceName    string      `bson:"province_name"`
		DistrictName    string      `bson:"district_name"`
		SubdistrictName string      `bson:"subdistrict_name"`
		PostalCode      interface{} `bson:"postal_code"` // Can be NumberLong or String
	} `bson:"address"`
}

type Discrepancy struct {
	OrderNumber  string
	VoilaOrderID int64
	Field        string
	CatalystVal  string
	MongoVal     string
}

// --- Main Entry ---

func Main(xmsCatalystDSN, mongoURI string) (interface{}, error) {
	// 1. Resolve Credentials
	catalystDSN := resolveDSN(xmsCatalystDSN, DefaultCatalystResource)
	resolvedMongoURI := resolveMongoURI(mongoURI, DefaultMongoResource)

	if catalystDSN == "" {
		return nil, fmt.Errorf("catalyst dsn could not be resolved")
	}
	if resolvedMongoURI == "" {
		return nil, fmt.Errorf("mongo uri could not be resolved")
	}

	// 2. Connect to Catalyst (Postgres)
	db, err := connectDB(catalystDSN)
	if err != nil {
		return nil, fmt.Errorf("catalyst db error: %w", err)
	}

	// 3. Connect to Voila UF (MongoDB)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mClient, err := mongo.Connect(ctx, options.Client().ApplyURI(resolvedMongoURI))
	if err != nil {
		return nil, fmt.Errorf("mongo connect error: %w", err)
	}
	defer mClient.Disconnect(ctx)

	// 4. Get Recent Orders from Catalyst
	var orders []OrderResult
	err = db.Table("voila.tr_order o").
		Select("o.id, o.order_number, o.reference_number, tos.province_name, tos.district_name, tos.subdistrict_name, tos.postal_code").
		Joins("JOIN voila.tr_order_shipping tos ON tos.order_id = o.id").
		Where("o.created_at >= ? AND tos.is_replaced = ?", time.Now().Add(-LookbackDuration), false).
		Scan(&orders).Error

	if err != nil {
		return nil, fmt.Errorf("catalyst query error: %w", err)
	}

	if len(orders) == 0 {
		return "No orders found in the last 1 hour.", nil
	}

	// 5. Compare with MongoDB
	// Extract database name from URI, fallback to "voila" if not found
	dbName := extractDBName(resolvedMongoURI)
	mColl := mClient.Database(dbName).Collection("order")

	var diffs []Discrepancy

	for _, o := range orders {
		var mOrder MongoOrder
		// Use OrderNumber (string) instead of ID (int64) to avoid BSON type mismatch (int32 vs int64)
		filter := bson.M{"xmsc_order_id": o.ID}
		err := mColl.FindOne(ctx, filter).Decode(&mOrder)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				fmt.Printf("Order not found in Mongo---- OrderID: %d, OrderNumber: %s\n", o.ID, o.OrderNumber)
				continue
			}
			return nil, fmt.Errorf("mongo error for order %s: %w", o.OrderNumber, err)
		}

		// Comparison
		compare(o.OrderNumber, mOrder.OrderID, "Order Reference", o.OrderReference, mOrder.OrderReference, &diffs)
		compare(o.OrderNumber, mOrder.OrderID, "Province", o.ProvinceName, mOrder.Address.ProvinceName, &diffs)
		compare(o.OrderNumber, mOrder.OrderID, "District", o.DistrictName, mOrder.Address.DistrictName, &diffs)
		compare(o.OrderNumber, mOrder.OrderID, "Subdistrict", o.SubdistrictName, mOrder.Address.SubdistrictName, &diffs)

		// Postal code handling (mongo can be int or string)
		mPostal := fmt.Sprintf("%v", mOrder.Address.PostalCode)
		compare(o.OrderNumber, mOrder.OrderID, "Postal Code", o.PostalCode, mPostal, &diffs)
	}

	if len(diffs) == 0 {
		return "Success: No address discrepancies found between XMS Catalyst & Voila UF.", nil
	}

	return formatMarkdown(diffs), nil
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
	// 1. If provided URI directly
	if strings.HasPrefix(provided, "mongodb://") || strings.HasPrefix(provided, "mongodb+srv://") {
		return provided
	}

	path := resourcePath
	if provided != "" {
		path = provided
	}

	// 2. Try as Resource (Windmill)
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
				// Construct URI
				return fmt.Sprintf("mongodb://%s:%s@%s:%v/%s?authSource=admin", user, pass, host, port, db)
			}
		}
	}

	// 3. Fallback to Variable (Plain text)
	return resolveVariable(provided, path)
}

func extractDBName(uri string) string {
	// Normalizes URI: take everything after the last '/' before '?'
	lastSlash := strings.LastIndex(uri, "/")
	if lastSlash == -1 {
		return "voila" // fallback
	}

	dbPart := uri[lastSlash+1:]
	qIdx := strings.Index(dbPart, "?")
	if qIdx != -1 {
		dbPart = dbPart[:qIdx]
	}

	if dbPart == "" {
		return "voila" // fallback
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

func compare(orderNum string, voilaOrderID int64, field, catVal, mVal string, diffs *[]Discrepancy) {
	cStr := strings.TrimSpace(strings.ToLower(catVal))
	mStr := strings.TrimSpace(strings.ToLower(mVal))

	if cStr != mStr {
		*diffs = append(*diffs, Discrepancy{
			OrderNumber:  orderNum,
			VoilaOrderID: voilaOrderID,
			Field:        field,
			CatalystVal:  catVal,
			MongoVal:     mVal,
		})
	}
}

func formatMarkdown(diffs []Discrepancy) string {
	var sb strings.Builder
	sb.WriteString("##### Hi @channel, Ada perbedaan data alamat antara XMS Catalyst & Voila UF, minta tolong dicek yah..\n")
	sb.WriteString("| Order Number | Field | XMS Catalyst | Voila UF |\n")
	sb.WriteString("| :--- | :--- | :--- | :--- |\n")

	// Sort by order number for readability
	sort.Slice(diffs, func(i, j int) bool {
		if diffs[i].OrderNumber != diffs[j].OrderNumber {
			return diffs[i].OrderNumber < diffs[j].OrderNumber
		}
		return diffs[i].Field < diffs[j].Field
	})

	for _, d := range diffs {
		catLink := fmt.Sprintf("%s/voila/order/order-detail/%s", XMS_CATALYST_BASE_URL, d.OrderNumber)
		voilaLink := fmt.Sprintf("%s/order/%d", VOILA_UF_WEB_BASE_URL, d.VoilaOrderID)

		catVal := fmt.Sprintf("[%s](%s)", d.CatalystVal, catLink)
		mongoVal := fmt.Sprintf("[%s](%s)", d.MongoVal, voilaLink)

		sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s |\n",
			d.OrderNumber, d.Field, catVal, mongoVal))
	}
	return sb.String()
}
