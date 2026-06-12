package inner

import (
	"fmt"
	"sort"
	"strings"

	wmill "github.com/windmill-labs/windmill-go-client"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

const (
	CatalystResourcePath = "u/mirza/catalyst_xms_postgresql_voila_prod"
	LegacyResourcePath   = "u/mirza/voila_postgresql_prod"
	CatalystSchema       = "voila"
)

type CatalystBrand struct {
	ID       int    `gorm:"column:id"`
	Name     string `gorm:"column:name"`
	LuxLevel string `gorm:"column:lux_level"`
}

func (CatalystBrand) TableName() string {
	return "ms_brand"
}

type LegacyBrand struct {
	ID       int    `gorm:"column:id"`
	Name     string `gorm:"column:name"`
	LuxLevel int    `gorm:"column:lux_level"`
}

func (LegacyBrand) TableName() string {
	return "ms_brand"
}

type BrandDiff struct {
	ID                int
	DB1Name           string
	DB2Name           string
	DB1LuxLevel       string
	DB2LuxLevelRaw    int
	DB2LuxLevelMapped string
	DiffType          string
}

func Main(xmsCatalystDSN, xmsLegacyDSN string) (interface{}, error) {
	fmt.Println("[INFO] Starting brand consistency check...")

	catalystDSN := resolveDSN(xmsCatalystDSN, CatalystResourcePath)
	legacyDSN := resolveDSN(xmsLegacyDSN, LegacyResourcePath)

	if catalystDSN == "" || legacyDSN == "" {
		return nil, fmt.Errorf("failed to resolve database credentials")
	}

	catalystDB, err := connectDB(catalystDSN, true)
	if err != nil {
		return nil, fmt.Errorf("catalyst db error: %w", err)
	}

	legacyDB, err := connectDB(legacyDSN, false)
	if err != nil {
		return nil, fmt.Errorf("legacy db error: %w", err)
	}

	catalystBrands, err := fetchCatalystBrands(catalystDB)
	if err != nil {
		return nil, err
	}

	legacyBrands, err := fetchLegacyBrands(legacyDB)
	if err != nil {
		return nil, err
	}

	fmt.Printf("[INFO] Catalyst Brands: %d\n", len(catalystBrands))
	fmt.Printf("[INFO] Legacy Brands: %d\n", len(legacyBrands))

	diffs := compareBrands(catalystBrands, legacyBrands)

	if len(diffs) == 0 {
		fmt.Println("[INFO] No discrepancies found")
		return nil, nil
	}

	fmt.Printf("[WARN] Found %d discrepancies\n", len(diffs))

	return formatMarkdown(diffs), nil
}

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

func connectDB(dsn string, isCatalyst bool) (*gorm.DB, error) {
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

func fetchCatalystBrands(db *gorm.DB) (map[int]CatalystBrand, error) {
	var brands []CatalystBrand

	err := db.
		Table("ms_brand").
		Select("id, name, lux_level::text as lux_level").
		Find(&brands).Error

	if err != nil {
		return nil, fmt.Errorf("fetch catalyst brands: %w", err)
	}

	result := make(map[int]CatalystBrand)

	for _, b := range brands {
		result[b.ID] = b
	}

	return result, nil
}

func fetchLegacyBrands(db *gorm.DB) (map[int]LegacyBrand, error) {
	var brands []LegacyBrand

	err := db.
		Table("public.ms_brand").
		Find(&brands).Error

	if err != nil {
		return nil, fmt.Errorf("fetch legacy brands: %w", err)
	}

	result := make(map[int]LegacyBrand)

	for _, b := range brands {
		result[b.ID] = b
	}

	return result, nil
}

func mapLuxLevel(level int) string {
	switch level {
	case 0:
		return "MULTIBRAND"
	case 1:
		return "LUXURY"
	case 101:
		return "MULTIBRAND"
	default:
		return "UNKNOWN"
	}
}

func compareBrands(
	catalyst map[int]CatalystBrand,
	legacy map[int]LegacyBrand,
) []BrandDiff {

	var diffs []BrandDiff
	allIDs := make(map[int]struct{})

	for id := range catalyst {
		allIDs[id] = struct{}{}
	}

	for id := range legacy {
		allIDs[id] = struct{}{}
	}

	for id := range allIDs {

		db1, inDB1 := catalyst[id]
		db2, inDB2 := legacy[id]

		diff := BrandDiff{
			ID: id,
		}

		if inDB1 {
			diff.DB1Name = db1.Name
			diff.DB1LuxLevel = db1.LuxLevel
		}

		if inDB2 {
			diff.DB2Name = db2.Name
			diff.DB2LuxLevelRaw = db2.LuxLevel
			diff.DB2LuxLevelMapped = mapLuxLevel(db2.LuxLevel)
		}

		switch {
		case !inDB1:
			diff.DiffType = "ONLY_IN_DB2"

		case !inDB2:
			diff.DiffType = "ONLY_IN_DB1"

		case db1.Name != db2.Name:
			diff.DiffType = "NAME_DIFF"

		case db1.LuxLevel != mapLuxLevel(db2.LuxLevel):
			diff.DiffType = "LUX_LEVEL_DIFF"

		default:
			continue
		}

		diffs = append(diffs, diff)
	}

	sort.Slice(diffs, func(i, j int) bool {
		return diffs[i].ID < diffs[j].ID
	})

	return diffs
}

func formatMarkdown(diffs []BrandDiff) string {
	var sb strings.Builder

	sb.WriteString("##### Hi @channel, Ada perbedaan data Brand antara Catalyst & Legacy, minta tolong cek yah..\n\n")

	sb.WriteString("| ID | Catalyst Name | Legacy Name | Catalyst Lux | Legacy Lux | Diff Type |\n")
	sb.WriteString("| :--- | :--- | :--- | :--- | :--- | :--- |\n")

	for _, d := range diffs {
		sb.WriteString(fmt.Sprintf(
			"| %d | %s | %s | %s | %s | %s |\n",
			d.ID,
			emptyIfBlank(d.DB1Name),
			emptyIfBlank(d.DB2Name),
			emptyIfBlank(d.DB1LuxLevel),
			emptyIfBlank(d.DB2LuxLevelMapped),
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
