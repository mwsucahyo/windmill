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
	XMS_CATALYST_BASE_URL = "https://xms.ctlyst.id"
	XMS_LEGACY_BASE_URL   = "https://xms.voila.id"

	DefaultCatalystResource = "u/mirza/catalyst_xms_postgresql_voila_prod"
	DefaultMongoResource    = "f/voila_anomalies/voila_mongodb_prod"

	LookbackDuration = 30 * time.Minute

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
	OrderID     int64 `bson:"order_id"`
	XmscOrderID int64 `bson:"xmsc_order_id"`
	OrderStatus struct {
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

	// 4. Get Completed/Canceled Orders from Catalyst (status_id in (4, 5)) in the last interval
	var orders []OrderResult
	err = db.Table("voila.tr_order o").
		Select("o.id, o.order_number, o.reference_number, o.status_id, ms.name as status_name").
		Joins("JOIN voila.ms_order_status ms ON ms.id = o.status_id").
		Where("o.status_id IN (4, 5) AND o.created_at >= ?", time.Now().Add(-LookbackDuration)).
		Scan(&orders).Error

	if err != nil {
		return nil, fmt.Errorf("catalyst query error: %w", err)
	}

	if len(orders) == 0 {
		return DefaultSuccessMessage, nil
	}

	// 5. Compare with MongoDB
	dbName := extractDBName(resolvedMongoURI)
	mColl := mClient.Database(dbName).Collection("order")

	var diffs []Discrepancy

	for _, o := range orders {
		var mOrder MongoOrder
		// Filter by Catalyst ID matching Mongo xmsc_order_id
		filter := bson.M{"xmsc_order_id": o.ID}
		err := mColl.FindOne(ctx, filter).Decode(&mOrder)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				// We don't log "not found" as a status discrepancy, but maybe it should be?
				// For now, let's just skip as per target "status discrepancy"
				continue
			}
			return nil, fmt.Errorf("mongo error for order %s: %w", o.OrderNumber, err)
		}

		// Discrepancy check
		isDiscrepancy := false
		if o.StatusID == 5 { // Completed
			if mOrder.OrderStatus.ID != 5 && mOrder.OrderStatus.ID != 6 {
				isDiscrepancy = true
			}
		} else if o.StatusID == 4 { // Canceled
			if mOrder.OrderStatus.ID != 8 && mOrder.OrderStatus.ID != 7 {
				isDiscrepancy = true
			}
		}

		if isDiscrepancy {
			diffs = append(diffs, Discrepancy{
				OrderNumber:        o.OrderNumber,
				OrderReference:     o.OrderReference,
				VoilaOrderID:       mOrder.OrderID,
				CatalystStatus:     o.StatusID,
				CatalystStatusName: o.StatusName,
				MongoStatus:        mOrder.OrderStatus.ID,
				MongoStatusCode:    mOrder.OrderStatus.Code,
			})
		}
	}

	if len(diffs) == 0 {
		return DefaultSuccessMessage, nil
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

func formatMarkdown(diffs []Discrepancy) string {
	var sb strings.Builder
	sb.WriteString("##### Hi @channel, Ada perbedaan status order completed / canceled antara XMS Catalyst & Voila UF, minta tolong dicek yah..\n")
	sb.WriteString("| Order Number | Order Reference | XMS Catalyst Status | Voila UF Status |\n")
	sb.WriteString("| :--- | :--- | :--- | :--- |\n")

	sort.Slice(diffs, func(i, j int) bool {
		return diffs[i].OrderNumber < diffs[j].OrderNumber
	})

	for _, d := range diffs {
		catLink := fmt.Sprintf("%s/voila/order/order-detail/%s", XMS_CATALYST_BASE_URL, d.OrderNumber)
		voilaLink := fmt.Sprintf("%s/order/%d", XMS_LEGACY_BASE_URL, d.VoilaOrderID)

		orderNum := fmt.Sprintf("[%s](%s)", d.OrderNumber, catLink)
		orderRef := fmt.Sprintf("[%s](%s)", d.OrderReference, voilaLink)

		catStatus := fmt.Sprintf("%s (%d)", d.CatalystStatusName, d.CatalystStatus)
		mongoStatus := fmt.Sprintf("%s (%d)", formatMongoCode(d.MongoStatusCode), d.MongoStatus)

		sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s |\n",
			orderNum, orderRef, catStatus, mongoStatus))
	}
	return sb.String()
}

func formatMongoCode(code string) string {
	if code == "" {
		return "Unknown"
	}
	// title case and replace underscore with space
	parts := strings.Split(code, "_")
	for i, p := range parts {
		if len(p) > 0 {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, " ")
}
