package inner

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	wmill "github.com/windmill-labs/windmill-go-client"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// require gorm.io/gorm v1.25.12
// require gorm.io/driver/postgres v1.5.9

// --- Constants ---

const (
	XMS_CATALYST_BASE_URL = "https://stg-catalyst-xms-web.machtwatch.net"
	VOILA_UF_BASE_URL     = "https://stg-voila-web.machtwatch.net"

	DefaultCatalystResource = "u/mirza/catalyst_xms_postgresql_voila_stg"

	DefaultVaultAddrVariable        = "f/voila_anomalies/vault_addr"
	DefaultVaultGithubTokenVariable = "f/voila_anomalies/vault_github_token"

	DefaultESUserVariable    = "f/voila_anomalies/voila_es_username_stg"
	DefaultESPassVariable    = "f/voila_anomalies/voila_es_password_stg"
	DefaultESURLVariable     = "f/voila_anomalies/voila_es_base_url_stg"
	DefaultVaultPathVariable = "f/voila_anomalies/vault_path_product_api_stg"

	VaultKeyIndex    = "ELASTIC_PRODUCT_INDEX"
	VaultKeyUsername = "ELASTIC_USERNAME"
	VaultKeyPassword = "ELASTIC_PASSWORD"

	// Hardcoded fallback for Vault Addr if variable is empty
	FallbackVaultAddr = "http://xxx.id:8200"

	StockMovementLookback = 600 * time.Hour
)

// --- Models ---

type StockMovement struct {
	VariantID int `gorm:"column:variant_id"`
}

func (StockMovement) TableName() string { return "voila.tr_stock_movement_history" }

type ProductMapRow struct {
	VariantID int `gorm:"column:variant_id"`
	ProductID int `gorm:"column:product_id"`
}

type ESResponse struct {
	Hits struct {
		Hits []struct {
			Source struct {
				ID             int `json:"id"`
				IsOutOfStock   int `json:"is_out_of_stock"`
				PreOrderStatus int `json:"pre_order_status"`
				Variants       []struct {
					ID    int    `json:"id"`
					SKU   string `json:"sku"`
					Stock int    `json:"stock"`
				} `json:"variants"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

type Discrepancy struct {
	ProductID   int
	SKU         string
	StatusXMSC  string // "Ready" or "OOS"
	StatusES    string
	CatalystQty int
}

// --- Main Entry ---

func Main(xmsCatalystDSN, esURL string) (interface{}, error) {
	// 1. Resolve Credentials & URL
	catalystDSN := resolveDSN(xmsCatalystDSN, DefaultCatalystResource)

	// Try resolve from Env/Windmill first
	esUser := resolveVariable(os.Getenv("ES_USERNAME"), DefaultESUserVariable)
	esPass := resolveVariable(os.Getenv("ES_PASSWORD"), DefaultESPassVariable)
	baseURL := resolveVariable(esURL, DefaultESURLVariable)
	vAddr := resolveVariable(os.Getenv("VAULT_ADDR"), DefaultVaultAddrVariable)
	if vAddr == "" {
		vAddr = FallbackVaultAddr
	}
	vGithubToken := resolveVariable(os.Getenv("VAULT_GITHUB_TOKEN"), DefaultVaultGithubTokenVariable)
	vPath := resolveVariable(os.Getenv("VAULT_PATH"), DefaultVaultPathVariable)

	// 1b. Overwrite with Vault data if available
	targetURL := baseURL
	if vAddr != "" && vGithubToken != "" {
		vaultData := getVaultData(vAddr, vGithubToken, vPath)
		if len(vaultData) > 0 {
			if val, ok := vaultData[VaultKeyUsername].(string); ok && val != "" {
				esUser = val
			}
			if val, ok := vaultData[VaultKeyPassword].(string); ok && val != "" {
				esPass = val
			}
			if val, ok := vaultData[VaultKeyIndex].(string); ok && val != "" {
				targetURL = fmt.Sprintf("%s/%s/_search", strings.TrimSuffix(baseURL, "/"), val)
			}
		}
	}

	if targetURL == baseURL && baseURL != "" && !strings.Contains(targetURL, "/_search") {
		targetURL = strings.TrimSuffix(baseURL, "/") + "/_search"
	}

	if esUser == "" || esPass == "" || targetURL == "" {
		return nil, fmt.Errorf("ES credentials or URL not resolved")
	}

	// 2. Connect to Catalyst
	db, err := connectDB(catalystDSN)
	if err != nil {
		return nil, fmt.Errorf("catalyst db error: %v", err)
	}

	// 3. Get Recent Movements (1 hour)
	movements, err := getRecentMovements(db)
	if err != nil {
		return nil, err
	}
	if len(movements) == 0 {
		return "No stock movements in the last hour.", nil
	}

	// 4. Get Product IDs for these Variants
	vIDs := make([]int, 0)
	for _, m := range movements {
		vIDs = append(vIDs, m.VariantID)
	}

	productMap, err := getProductIDs(db, vIDs)
	if err != nil {
		return nil, err
	}

	// Gather unique product IDs
	uniqueProductIDs := make(map[int]bool)
	productIDs := make([]int, 0)
	for _, pid := range productMap {
		if !uniqueProductIDs[pid] {
			uniqueProductIDs[pid] = true
			productIDs = append(productIDs, pid)
		}
	}

	// 5. Fetch XMSC Stock (total for the whole product)
	catProductStocks, err := fetchCatalystStockAtProductLevel(db, productIDs)
	if err != nil {
		return nil, err
	}

	// 6. Fetch ES Status
	esStatusMap, err := fetchESStatusByProductIDs(targetURL, esUser, esPass, productIDs)
	if err != nil {
		return nil, err
	}

	// 7. Compare
	diffs := compareProductStatus(catProductStocks, esStatusMap)
	if len(diffs) == 0 {
		return "Success: Product status matches between XMS Catalyst & Voila UF.", nil
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
		return provided
	}
	return res
}

func getVaultData(addr, githubToken, path string) map[string]interface{} {
	if addr == "" || githubToken == "" {
		return nil
	}
	loginURL := fmt.Sprintf("%s/v1/auth/github/login", strings.TrimSuffix(addr, "/"))
	loginBody, _ := json.Marshal(map[string]string{"token": githubToken})
	resp, err := http.Post(loginURL, "application/json", bytes.NewBuffer(loginBody))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var activeToken string
	if resp.StatusCode == http.StatusOK {
		var loginRes struct {
			Auth struct {
				ClientToken string `json:"client_token"`
			} `json:"auth"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&loginRes); err == nil {
			activeToken = loginRes.Auth.ClientToken
		}
	}
	if activeToken == "" {
		return nil
	}

	url := fmt.Sprintf("%s/v1/%s", strings.TrimSuffix(addr, "/"), path)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("X-Vault-Token", activeToken)
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err = client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil
	}
	defer resp.Body.Close()

	var result struct {
		Data struct {
			Data map[string]interface{} `json:"data"`
		} `json:"data"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	return result.Data.Data
}

func connectDB(dsn string) (*gorm.DB, error) {
	config := &gorm.Config{NamingStrategy: schema.NamingStrategy{TablePrefix: ""}}
	if !strings.Contains(dsn, "search_path") {
		delim := "?"
		if strings.Contains(dsn, "?") {
			delim = "&"
		}
		dsn += delim + "search_path=voila"
	}
	return gorm.Open(postgres.Open(dsn), config)
}

func getRecentMovements(db *gorm.DB) ([]StockMovement, error) {
	var movements []StockMovement
	err := db.Table("voila.tr_stock_movement_history").
		Select("DISTINCT variant_id").
		Where("qty_column = ? AND created_at >= ?", "qty_available", time.Now().Add(-StockMovementLookback)).
		Find(&movements).Error
	return movements, err
}

func getProductIDs(db *gorm.DB, vIDs []int) (map[int]int, error) {
	var rows []ProductMapRow
	err := db.Table("voila.ms_product_variant").
		Select("id as variant_id, product_id").
		Where("id IN ?", vIDs).
		Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	res := make(map[int]int)
	for _, r := range rows {
		res[r.VariantID] = r.ProductID
	}
	return res, nil
}

type ProductStockResult struct {
	ProductID  int    `gorm:"column:product_id"`
	SKU        string `gorm:"column:variant_sku"`
	TotalStock int    `gorm:"column:total_stock"`
}

func fetchCatalystStockAtProductLevel(db *gorm.DB, productIDs []int) (map[int]ProductStockResult, error) {
	var results []ProductStockResult

	// Using MIN(variant_sku) for products with multiple variants
	err := db.Table("voila.ms_product_variant_stock mpvs").
		Select("mpv.product_id, MIN(mpv.variant_sku) as variant_sku, SUM(mpvs.qty_available) as total_stock").
		Joins("JOIN voila.ms_product_variant mpv ON mpv.id = mpvs.variant_id").
		Where("mpv.product_id IN ? AND mpvs.is_deleted = ?", productIDs, false).
		Group("mpv.product_id").
		Scan(&results).Error

	if err != nil {
		return nil, err
	}

	res := make(map[int]ProductStockResult)
	for _, r := range results {
		res[r.ProductID] = r
	}
	return res, nil
}

type ESProductInfo struct {
	IsOutOfStock   int
	PreOrderStatus int
}

func fetchESStatusByProductIDs(url, user, pass string, productIDs []int) (map[int]ESProductInfo, error) {
	idStrings := make([]string, len(productIDs))
	for i, id := range productIDs {
		idStrings[i] = fmt.Sprintf("%d", id)
	}

	queryBody := map[string]interface{}{
		"size": 1000,
		"query": map[string]interface{}{
			"ids": map[string]interface{}{
				"values": idStrings,
			},
		},
	}

	jsonBody, _ := json.Marshal(queryBody)
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	req.SetBasicAuth(user, pass)
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ES error (%d): %s", resp.StatusCode, string(body))
	}

	var esRes ESResponse
	json.NewDecoder(resp.Body).Decode(&esRes)

	res := make(map[int]ESProductInfo)
	for _, hit := range esRes.Hits.Hits {
		res[hit.Source.ID] = ESProductInfo{
			IsOutOfStock:   hit.Source.IsOutOfStock,
			PreOrderStatus: hit.Source.PreOrderStatus,
		}
	}
	return res, nil
}

func compareProductStatus(cat map[int]ProductStockResult, es map[int]ESProductInfo) []Discrepancy {
	var diffs []Discrepancy

	for pid, catData := range cat {
		esInfo, exists := es[pid]
		if !exists {
			continue // Product not found in ES for some reason
		}

		// Filter: only check if pre_order_status == 0 in ES
		if esInfo.PreOrderStatus != 0 {
			continue
		}

		// Discrepancy logic:
		// 1. Stock > 0 but ES says OOS=true (should be false)
		// 2. Stock == 0 but ES says OOS=false (should be true)

		statusXMSC := "Ready"
		if catData.TotalStock == 0 {
			statusXMSC = "OOS"
		}

		statusES := "Ready"
		if esInfo.IsOutOfStock != 0 {
			statusES = "OOS"
		}

		if (catData.TotalStock > 0 && esInfo.IsOutOfStock != 0) || (catData.TotalStock == 0 && esInfo.IsOutOfStock == 0) {
			fmt.Println("esInfo.IsOutOfStock", esInfo.IsOutOfStock)
			fmt.Println("esInfo.PreOrderStatus", esInfo.PreOrderStatus)

			diffs = append(diffs, Discrepancy{
				ProductID:   pid,
				SKU:         catData.SKU,
				StatusXMSC:  statusXMSC,
				StatusES:    statusES,
				CatalystQty: catData.TotalStock,
			})
		}
	}

	sort.Slice(diffs, func(i, j int) bool {
		return diffs[i].ProductID < diffs[j].ProductID
	})
	return diffs
}

func formatMarkdown(diffs []Discrepancy) string {
	var sb strings.Builder
	sb.WriteString("##### Hi @channel, Ada perbedaan status produk antara XMS Catalyst & Voila UF, minta tolong dicek yah..\n")
	sb.WriteString("| Product ID | SKU | XMS Catalyst (Stock) | XMS Catalyst (Status) | Voila UF (Status) |\n")
	sb.WriteString("| :--- | :--- | :---: | :---: | :---: |\n")

	for _, d := range diffs {
		catLink := fmt.Sprintf("[%d](%s/voila/stock/office/%d)", d.CatalystQty, XMS_CATALYST_BASE_URL, d.ProductID)
		voilaLink := fmt.Sprintf("[%s](%s/products/%d)", d.StatusES, VOILA_UF_BASE_URL, d.ProductID)

		sb.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %s |\n",
			d.ProductID, d.SKU, catLink, d.StatusXMSC, voilaLink))
	}
	return sb.String()
}
