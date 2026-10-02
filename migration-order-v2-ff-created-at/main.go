package inner

import (
	"context"
	"fmt"
	"strings"
	"time"

	wmill "github.com/windmill-labs/windmill-go-client"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type ResourceConfig struct {
	CatalystVoilaResource     string
	CatalystJamtanganResource string
	MongoResource             string
}

var resourceByEnv = map[string]ResourceConfig{
	"dev": {
		CatalystVoilaResource:     "u/mirza/catalyst_xms_postgresql_voila_dev",
		CatalystJamtanganResource: "u/mirza/catalyst_xms_postgresql_jt_dev",
		MongoResource:             "f/flows_engineering/xms_catalyst_mongo_dev",
	},
	"stg": {
		CatalystVoilaResource:     "u/mirza/catalyst_xms_postgresql_voila_stg",
		CatalystJamtanganResource: "u/mirza/catalyst_xms_postgresql_jt_stg",
		MongoResource:             "f/flows_engineering/xms_catalyst_mongo_stg",
	},
	"prod": {
		CatalystVoilaResource:     "u/mirza/catalyst_xms_postgresql_voila_prod",
		CatalystJamtanganResource: "u/mirza/catalyst_xms_postgresql_jt_prod",
		MongoResource:             "f/flows_engineering/xms_catalyst_mongo_prod",
	},
}

// createFFCases are the migration_order_v2_log case values that produce newly
// created tr_fulfillment rows.
var createFFCases = []string{
	"CREATE_CARRY_OUT",
	"CREATE_CONSIGN",
	"CREATE_HOME_DELIVERY",
	"MIXED_CREATE_CONSIGN",
	"MIXED_CREATE_HOME_DELIVERY",
}

// createdAtGrace is the window before a log's migrated_at within which a
// fulfillment is considered migration-created. Pre-existing fulfillments belong
// to orders completed at least 5 days before migration, so they fall far outside
// this window.
const createdAtGrace = 24 * time.Hour

// defaultLogLimit caps how many pending logs a single run processes when the
// caller does not pass a limit. Pass a negative limit to disable the cap.
const defaultLogLimit = 100

const logCollection = "migration_order_v2_log"

type OrderTimestamp struct {
	ID          int64
	ProcessedAt *time.Time
	CompletedAt *time.Time
	CreatedAt   *time.Time
}

type migrationLog struct {
	ID             primitive.ObjectID `bson:"_id"`
	OrderID        int64              `bson:"order_id"`
	OrderNumber    string             `bson:"order_number"`
	Schema         string             `bson:"schema"`
	Case           string             `bson:"case"`
	Status         string             `bson:"status"`
	FulfillmentIDs []int64            `bson:"fulfillment_ids"`
	MigratedAt     time.Time          `bson:"migrated_at"`
}

type mongoRepository struct {
	client *mongo.Client
	dbName string
}

func newMongo(client *mongo.Client, dbName string) *mongoRepository {
	return &mongoRepository{client: client, dbName: dbName}
}

func (r *mongoRepository) findPendingLogs(ctx context.Context, filter bson.M, limit int64) ([]migrationLog, error) {
	coll := r.client.Database(r.dbName).Collection(logCollection)
	opts := options.Find().SetSort(bson.M{"migrated_at": 1})
	if limit > 0 {
		opts.SetLimit(limit)
	}
	cur, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("query migration logs failed: %w", err)
	}
	defer cur.Close(ctx)

	var logs []migrationLog
	if err := cur.All(ctx, &logs); err != nil {
		return nil, fmt.Errorf("decode migration logs failed: %w", err)
	}
	return logs, nil
}

func (r *mongoRepository) markFixed(ctx context.Context, id primitive.ObjectID, count int, to time.Time) error {
	coll := r.client.Database(r.dbName).Collection(logCollection)
	_, err := coll.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": bson.M{
		"created_at_fixed_at":    time.Now().UTC(),
		"created_at_fixed_count": count,
		"created_at_fixed_to":    to,
	}})
	if err != nil {
		return fmt.Errorf("mark log fixed failed: %w", err)
	}
	return nil
}

type Usecase struct {
	db           *gorm.DB
	mongoRepo    *mongoRepository
	schema       string
	orderNumbers []string
	ffCase       string
	limit        int64
}

func (u *Usecase) buildFilter() bson.M {
	filter := bson.M{
		"schema":              u.schema,
		"status":              "OK",
		"created_at_fixed_at": bson.M{"$exists": false},
	}
	if u.ffCase != "" {
		filter["case"] = u.ffCase
	} else {
		cases := make([]interface{}, len(createFFCases))
		for i, c := range createFFCases {
			cases[i] = c
		}
		filter["case"] = bson.M{"$in": cases}
	}
	if len(u.orderNumbers) > 0 {
		filter["order_number"] = bson.M{"$in": u.orderNumbers}
	}
	return filter
}

func (u *Usecase) Run() (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	logs, err := u.mongoRepo.findPendingLogs(ctx, u.buildFilter(), u.limit)
	if err != nil {
		return nil, err
	}

	caseLabel := u.ffCase
	if caseLabel == "" {
		caseLabel = "ALL_CREATE_CASES"
	}
	orderLabel := "ALL"
	if len(u.orderNumbers) > 0 {
		orderLabel = strings.Join(u.orderNumbers, ",")
	}

	limitLabel := "ALL"
	if u.limit > 0 {
		limitLabel = fmt.Sprintf("%d", u.limit)
	}

	if len(logs) == 0 {
		return fmt.Sprintf("##### Fix FF created_at — %s\n\nNo pending migration logs found (case=%s, order_number=%s, limit=%s).",
			u.schema, caseLabel, orderLabel, limitLabel), nil
	}

	var updatedOrders, updatedFFs, skipped int
	var rows []string

	for _, lg := range logs {
		o, err := queryOrderTimestamp(u.db, u.schema, lg.OrderID)
		if err != nil {
			fmt.Printf("[WARN] query order %d failed: %v\n", lg.OrderID, err)
			skipped++
			continue
		}
		resolved := resolveOrderTimestamp(o)
		if resolved == nil {
			fmt.Printf("[WARN] order %d has no processed_at/completed_at/created_at, skipped\n", lg.OrderID)
			skipped++
			continue
		}

		since := lg.MigratedAt.Add(-createdAtGrace)
		n, err := updateFulfillmentCreatedAt(u.db, u.schema, lg.FulfillmentIDs, since, *resolved)
		if err != nil {
			fmt.Printf("[WARN] update ff created_at for order %d failed: %v\n", lg.OrderID, err)
			skipped++
			continue
		}
		if n == 0 {
			continue
		}
		if err := u.mongoRepo.markFixed(ctx, lg.ID, int(n), *resolved); err != nil {
			fmt.Printf("[WARN] %v\n", err)
		}

		updatedOrders++
		updatedFFs += int(n)
		rows = append(rows, fmt.Sprintf("| %d | %s | %s | %d |", lg.OrderID, lg.OrderNumber, lg.Case, n))
	}

	out := fmt.Sprintf("##### Fix FF created_at — %s, case=%s, order_number=%s, limit=%s\n\n", u.schema, caseLabel, orderLabel, limitLabel)
	out += "| Order ID | Order Number | Case | Updated FF |\n"
	out += "|---|---|---|---|\n"
	out += strings.Join(rows, "\n")
	out += fmt.Sprintf("\n\n**Summary:** %d logs scanned, %d orders updated, %d fulfillments updated, %d skipped",
		len(logs), updatedOrders, updatedFFs, skipped)
	if u.limit > 0 && int64(len(logs)) == u.limit {
		out += fmt.Sprintf("\n\nLimit reached (%d) — run again to continue with the next batch.", u.limit)
	}
	return out, nil
}

func queryOrderTimestamp(db *gorm.DB, schema string, orderID int64) (*OrderTimestamp, error) {
	var o OrderTimestamp
	err := db.Raw(fmt.Sprintf(`
		SELECT id, processed_at, completed_at, created_at
		FROM %s.tr_order
		WHERE id = ?
	`, schema), orderID).Scan(&o).Error
	if err != nil {
		return nil, err
	}
	return &o, nil
}

func resolveOrderTimestamp(o *OrderTimestamp) *time.Time {
	if o.ProcessedAt != nil && !o.ProcessedAt.IsZero() {
		return o.ProcessedAt
	}
	if o.CompletedAt != nil && !o.CompletedAt.IsZero() {
		return o.CompletedAt
	}
	if o.CreatedAt != nil && !o.CreatedAt.IsZero() {
		return o.CreatedAt
	}
	return nil
}

func updateFulfillmentCreatedAt(db *gorm.DB, schema string, ffIDs []int64, since, createdAt time.Time) (int64, error) {
	if len(ffIDs) == 0 {
		return 0, nil
	}
	res := db.Exec(fmt.Sprintf(`
		UPDATE %s.tr_fulfillment
		SET created_at = ?
		WHERE id IN (%s) AND created_at >= ?
	`, schema, joinIDs(ffIDs)), createdAt, since)
	if res.Error != nil {
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

func splitOrderNumbers(raw string) []string {
	var out []string
	for _, n := range strings.Split(raw, ",") {
		n = strings.TrimSpace(n)
		if n != "" {
			out = append(out, n)
		}
	}
	return out
}

func joinIDs(ids []int64) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%d", id)
	}
	return strings.Join(parts, ",")
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
	Environment  string `json:"environment"`
	Schema       string `json:"schema"`
	OrderNumbers string `json:"order_numbers"`
	Case         string `json:"case"`
	Limit        int    `json:"limit"`
}, xmsCatalystDSN, mongoResourceOrURI string) (interface{}, error) {
	environment := migrationParams.Environment
	if environment == "" {
		environment = "dev"
	}
	// limit > 0: batasi log per run; limit == 0: default; limit < 0: tanpa batas.
	limit64 := int64(migrationParams.Limit)
	switch {
	case limit64 == 0:
		limit64 = defaultLogLimit
	case limit64 < 0:
		limit64 = 0
	}
	resources, ok := resourceByEnv[environment]
	if !ok {
		return nil, fmt.Errorf("unsupported environment %q, must be dev, stg, or prod", environment)
	}
	schema := migrationParams.Schema
	if schema == "" {
		return nil, fmt.Errorf("schema is required")
	}

	catalystResource := resources.CatalystVoilaResource
	if schema == "jamtangan" {
		catalystResource = resources.CatalystJamtanganResource
	}

	catalystDSN := resolveDSN(xmsCatalystDSN, catalystResource)
	if catalystDSN == "" {
		return nil, fmt.Errorf("catalyst dsn could not be resolved")
	}

	mongoURI := resolveMongoURI(mongoResourceOrURI, resources.MongoResource)
	if mongoURI == "" {
		return nil, fmt.Errorf("mongo uri could not be resolved (required for logs and flagging)")
	}

	db, err := connectDB(catalystDSN)
	if err != nil {
		return nil, fmt.Errorf("db error: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, fmt.Errorf("mongo connect error: %w", err)
	}
	defer mongoClient.Disconnect(context.Background())

	uc := &Usecase{
		db:           db,
		mongoRepo:    newMongo(mongoClient, extractDBName(mongoURI)),
		schema:       schema,
		orderNumbers: splitOrderNumbers(migrationParams.OrderNumbers),
		ffCase:       strings.TrimSpace(migrationParams.Case),
		limit:        limit64,
	}
	return uc.Run()
}
