package inner

import (
	"context"
	"fmt"
	"strings"
	"time"

	"windmill/migration-order-v2/repository"
	"windmill/migration-order-v2/usecase"

	wmill "github.com/windmill-labs/windmill-go-client"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	DefaultCatalystResource = "u/mirza/catalyst_xms_postgresql_voila_prod"
	DefaultMongoResource    = "f/voila_anomalies/voila_mongodb_prod"
)

func Main(xmsCatalystDSN, schema, startDate, endDate, orderNumbers, mongoResourceOrURI string) (interface{}, error) {
	catalystDSN := resolveDSN(xmsCatalystDSN, DefaultCatalystResource)
	if catalystDSN == "" {
		return nil, fmt.Errorf("catalyst dsn could not be resolved")
	}
	if schema == "" {
		return nil, fmt.Errorf("schema is required")
	}
	if startDate == "" && endDate == "" && orderNumbers == "" {
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

	repo := repository.New(db, schema)
	var mongoRepo *repository.MongoRepository
	if mongoClient != nil {
		dbName := extractDBName(mongoURI)
		mongoRepo = repository.NewMongo(mongoClient, dbName)
	}

	uc := usecase.New(repo, mongoRepo, schema, startDate, endDate, orderNumbers)

	results, err := uc.ProcessOrders(startDate, endDate, orderNumbers)
	if err != nil {
		return nil, err
	}
	if results == nil {
		return nil, nil
	}

	return formatResults(results, schema, startDate, endDate), nil
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

func formatResults(results []usecase.MigrationResult, schema, startDate, endDate string) string {
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
