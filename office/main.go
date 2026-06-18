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

// require gorm.io/driver/postgres v1.5.9
// require go.mongodb.org/mongo-driver v1.17.1

const (
	CatalystResourcePath = "u/mirza/catalyst_xms_postgresql_voila_prod"
	LegacyResourcePath   = "u/mirza/voila_postgresql_prod"
	DefaultMongoResource = "f/voila_anomalies/voila_mongodb_prod"
	CatalystSchema       = "voila"
)

// --- Models (Postgres) ---

type CatalystOffice struct {
	ID   int    `gorm:"column:id"`
	Name string `gorm:"column:name"`
}

func (CatalystOffice) TableName() string {
	return "ms_office"
}

type LegacyOffice struct {
	ID   int    `gorm:"column:id"`
	Name string `gorm:"column:name"`
}

func (LegacyOffice) TableName() string {
	return "ms_office"
}

// --- Model (Mongo) ---

type MongoOffice struct {
	OfficeID int    `bson:"office_id"`
	Name     string `bson:"name"`
}

// --- Diff ---

type OfficeDiff struct {
	ID           int
	CatalystName string
	LegacyName   string
	MongoName    string
	DiffType     string
}

// --- Main Entry ---

func Main(xmsCatalystDSN, xmsLegacyDSN, mongoURI string) (interface{}, error) {
	fmt.Println("[INFO] Starting office consistency check (3-way: XMS Catalyst ↔ XMS Legacy ↔ Mongo Voila UF)...")

	catalystDSN := resolveDSN(xmsCatalystDSN, CatalystResourcePath)
	legacyDSN := resolveDSN(xmsLegacyDSN, LegacyResourcePath)
	resolvedMongoURI := resolveMongoURI(mongoURI, DefaultMongoResource)

	if catalystDSN == "" {
		return nil, fmt.Errorf("catalyst dsn could not be resolved")
	}
	if legacyDSN == "" {
		return nil, fmt.Errorf("legacy dsn could not be resolved")
	}
	if resolvedMongoURI == "" {
		return nil, fmt.Errorf("mongo uri could not be resolved")
	}

	// 1. Connect Catalyst Postgres
	catalystDB, err := connectPG(catalystDSN, true)
	if err != nil {
		return nil, fmt.Errorf("catalyst db error: %w", err)
	}

	// 2. Connect Legacy Postgres
	legacyDB, err := connectPG(legacyDSN, false)
	if err != nil {
		return nil, fmt.Errorf("legacy db error: %w", err)
	}

	// 3. Connect MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	mClient, err := mongo.Connect(ctx, options.Client().ApplyURI(resolvedMongoURI))
	if err != nil {
		return nil, fmt.Errorf("mongo connect error: %w", err)
	}
	defer mClient.Disconnect(ctx)

	// 4. Fetch from all sources
	catalystOffices, err := fetchCatalystOffices(catalystDB)
	if err != nil {
		return nil, err
	}
	fmt.Printf("[INFO] XMS Catalyst Offices: %d\n", len(catalystOffices))

	legacyOffices, err := fetchLegacyOffices(legacyDB)
	if err != nil {
		return nil, err
	}
	fmt.Printf("[INFO] Legacy PG Offices: %d\n", len(legacyOffices))

	mongoOffices, err := fetchMongoOffices(ctx, mClient, resolvedMongoURI)
	if err != nil {
		return nil, err
	}
	fmt.Printf("[INFO] Mongo Offices: %d\n", len(mongoOffices))

	// 5. Compare
	diffs := compareOffices(catalystOffices, legacyOffices, mongoOffices)

	if len(diffs) == 0 {
		fmt.Println("[INFO] No discrepancies found across all 3 sources")
		return nil, nil
	}

	fmt.Printf("[WARN] Found %d discrepancies\n", len(diffs))
	return formatMarkdown(diffs), nil
}

// --- Resolve Credentials ---

func resolveDSN(provided, resourcePath string) string {
	if strings.HasPrefix(provided, "postgres://") {
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

	resVar, err := wmill.GetVariable(path)
	if err == nil {
		return resVar
	}

	return ""
}

// --- Database Connections ---

func connectPG(dsn string, isCatalyst bool) (*gorm.DB, error) {
	config := &gorm.Config{}

	if isCatalyst {
		config.NamingStrategy = schema.NamingStrategy{
			TablePrefix: "",
		}

		if !strings.Contains(dsn, "search_path") {
			path := fmt.Sprintf("search_path=%s", CatalystSchema)

			if strings.Contains(dsn, "?") {
				dsn += "&" + path
			} else {
				dsn += "?" + path
			}
		}
	}

	return gorm.Open(postgres.Open(dsn), config)
}

// --- Fetch Functions ---

func fetchCatalystOffices(db *gorm.DB) (map[int]CatalystOffice, error) {
	var offices []CatalystOffice

	err := db.
		Table("ms_office").
		Select("id, name").
		Find(&offices).Error

	if err != nil {
		return nil, fmt.Errorf("fetch catalyst offices: %w", err)
	}

	result := make(map[int]CatalystOffice)
	for _, o := range offices {
		result[o.ID] = o
	}

	return result, nil
}

func fetchLegacyOffices(db *gorm.DB) (map[int]LegacyOffice, error) {
	var offices []LegacyOffice

	err := db.
		Table("public.ms_office").
		Select("id, name").
		Find(&offices).Error

	if err != nil {
		return nil, fmt.Errorf("fetch legacy offices: %w", err)
	}

	result := make(map[int]LegacyOffice)
	for _, o := range offices {
		result[o.ID] = o
	}

	return result, nil
}

func fetchMongoOffices(ctx context.Context, client *mongo.Client, uri string) (map[int]MongoOffice, error) {
	dbName := extractDBName(uri)
	coll := client.Database(dbName).Collection("office")

	cursor, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("mongo office find error: %w", err)
	}
	defer cursor.Close(ctx)

	result := make(map[int]MongoOffice)
	for cursor.Next(ctx) {
		var o MongoOffice
		if err := cursor.Decode(&o); err != nil {
			return nil, fmt.Errorf("mongo decode error: %w", err)
		}
		result[o.OfficeID] = o
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("mongo cursor error: %w", err)
	}

	return result, nil
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

// --- Comparison ---

func compareOffices(
	catalyst map[int]CatalystOffice,
	legacy map[int]LegacyOffice,
	mongo map[int]MongoOffice,
) []OfficeDiff {

	var diffs []OfficeDiff
	allIDs := make(map[int]struct{})

	for id := range catalyst {
		allIDs[id] = struct{}{}
	}
	for id := range legacy {
		allIDs[id] = struct{}{}
	}
	for id := range mongo {
		allIDs[id] = struct{}{}
	}

	for id := range allIDs {
		_, inCat := catalyst[id]
		_, inLeg := legacy[id]
		_, inMon := mongo[id]

		// Skip if in all 3 — no discrepancy
		if inCat && inLeg && inMon {
			continue
		}

		diff := OfficeDiff{
			ID: id,
		}

		if c, ok := catalyst[id]; ok {
			diff.CatalystName = c.Name
		}
		if l, ok := legacy[id]; ok {
			diff.LegacyName = l.Name
		}
		if m, ok := mongo[id]; ok {
			diff.MongoName = m.Name
		}

		switch {
		case !inCat && !inLeg && inMon:
			diff.DiffType = "ONLY_IN_MONGO_VOILA_UF"
		case !inCat && inLeg && !inMon:
			diff.DiffType = "ONLY_IN_XMS_LEGACY"
		case inCat && !inLeg && !inMon:
			diff.DiffType = "ONLY_IN_XMS_CATALYST"
		case inCat && inLeg && !inMon:
			diff.DiffType = "NOT_IN_MONGO_VOILA_UF"
		case inCat && !inLeg && inMon:
			diff.DiffType = "NOT_IN_XMS_LEGACY"
		case !inCat && inLeg && inMon:
			diff.DiffType = "NOT_IN_XMS_CATALYST"
		}

		diffs = append(diffs, diff)
	}

	sort.Slice(diffs, func(i, j int) bool {
		return diffs[i].ID < diffs[j].ID
	})

	return diffs
}

// --- Output ---

func formatMarkdown(diffs []OfficeDiff) string {
	var sb strings.Builder

	sb.WriteString("##### Hi @channel, Ada perbedaan data Office antara XMS Catalyst ↔ XMS Legacy ↔ MongoDB Voila UF, minta tolong cek yah..\n\n")

	sb.WriteString("| ID | Catalyst Name | Legacy Name | Mongo Name | Status |\n")
	sb.WriteString("| :--- | :--- | :--- | :--- | :--- |\n")

	for _, d := range diffs {
		sb.WriteString(fmt.Sprintf(
			"| %d | %s | %s | %s | %s |\n",
			d.ID,
			emptyIfBlank(d.CatalystName),
			emptyIfBlank(d.LegacyName),
			emptyIfBlank(d.MongoName),
			d.DiffType,
		))
	}

	return sb.String()
}

func emptyIfBlank(v string) string {
	if v == "" {
		return "-"
	}
	return v
}
