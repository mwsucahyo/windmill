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

	StockMovementLookback = 24 * time.Hour
)

// --- Models ---

type StockMovement struct {
	VariantID int `gorm:"column:variant_id"`
	OfficeID  int `gorm:"column:office_id"`
}

func (StockMovement) TableName() string { return "voila.tr_stock_movement_history" }

type CatalystStockRow struct {
	ProductID int `gorm:"column:product_id"`
	TotalQty  int `gorm:"column:total_qty"`
}

type ProductMapRow struct {
	VariantID int `gorm:"column:variant_id"`
	ProductID int `gorm:"column:product_id"`
}

type ESResponse struct {
	Hits struct {
		Hits []struct {
			Source struct {
				ID       int    `json:"id"`
				Name     string `json:"name"`
				Variants []struct {
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
	VariantID   int
	SKU         string
	CatalystQty int
	ESQty       int
	Diff        int
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
				// Construct target URL: [base]/[index]/_search
				targetURL = fmt.Sprintf("%s/%s/_search", strings.TrimSuffix(baseURL, "/"), val)
			}
		}
	}

	// Final check: if targetURL was not set by Vault index logic, ensure it has /_search
	if targetURL == baseURL && baseURL != "" && !strings.Contains(targetURL, "/_search") {
		targetURL = strings.TrimSuffix(baseURL, "/") + "/_search"
	}

	if esUser == "" || esPass == "" || targetURL == "" {
		return nil, fmt.Errorf("ES credentials (user/pass) or URL could not be resolved from Env, Windmill, or Vault")
	}

	// 2. Connect to Catalyst
	db, err := connectDB(catalystDSN)
	if err != nil {
		return nil, fmt.Errorf("catalyst db error: %w", err)
	}

	// 3. Get Recent Movements from Catalyst (last 24h)
	movements, err := getRecentMovements(db)
	if err != nil {
		return nil, err
	}
	if len(movements) == 0 {
		return "No stock movements in the last 24 hours.", nil
	}

	// 4. Get Product IDs for these Variants
	variantIDs := make([]int, 0)
	for _, m := range movements {
		variantIDs = append(variantIDs, m.VariantID)
	}

	productMap, err := getProductIDs(db, variantIDs)
	if err != nil {
		return nil, err
	}

	productIDs := make([]int, 0)
	uniqueProductIDs := make(map[int]bool)
	for _, pid := range productMap {
		if !uniqueProductIDs[pid] {
			uniqueProductIDs[pid] = true
			productIDs = append(productIDs, pid)
		}
	}

	// 5. Fetch Stock from Catalyst at Variant Level for all variants belonging to these products
	// (Sum across all offices)
	catStock, err := fetchCatalystStockAtVariantLevel(db, productIDs)
	if err != nil {
		return nil, err
	}

	// 6. Fetch Stock from ES
	esStockMap, err := fetchESStockByProductIDs(targetURL, esUser, esPass, productIDs)
	if err != nil {
		return nil, err
	}

	// 7. Compare
	// We iterate through all variants found in these products to ensure nothing is missed
	diffs := compareStocksAtVariantLevel(catStock, esStockMap)
	if len(diffs) == 0 {
		return "Success: No stock discrepancies found between XMS Catalyst & ES for moved products.", nil
	}

	return formatMarkdown(diffs, targetURL), nil
}

// --- Helper Functions ---

func resolveDSN(provided, resourcePath string) string {
	if strings.Contains(provided, "@") || strings.Contains(provided, "host=") {
		return provided
	}

	res, err := wmill.GetResource(resourcePath)
	if err != nil {
		// Mute warning if it's just a protocol error (running locally)
		if !strings.Contains(err.Error(), "unsupported protocol scheme") {
			fmt.Printf("Warning: failed to get resource %s: %v\n", resourcePath, err)
		}
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
	// 1. Env First: if it's a plain value, use it
	if provided != "" && !strings.HasPrefix(provided, "f/") && !strings.HasPrefix(provided, "u/") {
		return provided
	}

	path := variablePath
	if provided != "" {
		path = provided
	}

	// 2. Windmill Second
	res, err := wmill.GetVariable(path)
	if err != nil {
		// Mute warning if it's just a protocol error (running locally)
		if !strings.Contains(err.Error(), "unsupported protocol scheme") {
			fmt.Printf("Warning: failed to get variable %s: %v\n", path, err)
		}
		return provided
	}

	return res
}

func getVaultData(addr, githubToken, path string) map[string]interface{} {
	if addr == "" || githubToken == "" {
		return nil
	}

	var activeToken string
	// Login using Github token
	loginURL := fmt.Sprintf("%s/v1/auth/github/login", strings.TrimSuffix(addr, "/"))
	loginBody, _ := json.Marshal(map[string]string{"token": githubToken})

	resp, err := http.Post(loginURL, "application/json", bytes.NewBuffer(loginBody))
	if err != nil {
		fmt.Printf("Warning: Vault Github login failed: %v\n", err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var loginRes struct {
			Auth struct {
				ClientToken string `json:"client_token"`
			} `json:"auth"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&loginRes); err == nil {
			activeToken = loginRes.Auth.ClientToken
		}
	} else {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Warning: Vault Github login failed status %d: %s\n", resp.StatusCode, string(body))
	}

	if activeToken == "" {
		return nil
	}

	url := fmt.Sprintf("%s/v1/%s", strings.TrimSuffix(addr, "/"), path)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("X-Vault-Token", activeToken)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err = client.Do(req)
	if err != nil {
		fmt.Printf("Warning: Vault request failed: %v\n", err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Warning: Vault returned status %d. Body: %s\n", resp.StatusCode, string(body))
		return nil
	}

	var result struct {
		Data struct {
			Data map[string]interface{} `json:"data"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil
	}

	return result.Data.Data
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

func getRecentMovements(db *gorm.DB) ([]StockMovement, error) {
	var movements []StockMovement
	err := db.Table("voila.tr_stock_movement_history").
		Select("DISTINCT variant_id").
		Where("qty_column = ? AND created_at >= ?", "qty_available", time.Now().Add(-StockMovementLookback)).
		Find(&movements).Error
	return movements, err
}

func getProductIDs(db *gorm.DB, variantIDs []int) (map[int]int, error) {
	var rows []ProductMapRow
	err := db.Table("voila.ms_product_variant").
		Select("id as variant_id, product_id").
		Where("id IN ?", variantIDs).
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

type VariantStockInfo struct {
	ProductID int
	VariantID int
	SKU       string
	Stock     int
}

func fetchCatalystStockAtVariantLevel(db *gorm.DB, productIDs []int) (map[int]VariantStockInfo, error) {
	var results []struct {
		ProductID int    `gorm:"column:product_id"`
		VariantID int    `gorm:"column:variant_id"`
		SKU       string `gorm:"column:variant_sku"`
		Stock     int    `gorm:"column:total_stock"`
	}

	err := db.Table("voila.ms_product_variant_stock mpvs").
		Select("mpv.product_id, mpvs.variant_id, mpv.variant_sku, SUM(mpvs.qty_available) as total_stock").
		Joins("JOIN voila.ms_product_variant mpv ON mpv.id = mpvs.variant_id").
		Where("mpv.product_id IN ? AND mpvs.is_deleted = ?", productIDs, false).
		Group("mpv.product_id, mpvs.variant_id, mpv.variant_sku").
		Scan(&results).Error

	if err != nil {
		return nil, err
	}

	res := make(map[int]VariantStockInfo)
	for _, r := range results {
		res[r.VariantID] = VariantStockInfo{
			ProductID: r.ProductID,
			VariantID: r.VariantID,
			SKU:       r.SKU,
			Stock:     r.Stock,
		}
	}
	return res, nil
}

func fetchESStockByProductIDs(url, user, pass string, productIDs []int) (map[int]VariantStockInfo, error) {
	// Convert IDs to strings for "ids" query
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
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(user, pass)
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ES error (%d). Response: %s", resp.StatusCode, string(body))
	}

	var esRes ESResponse
	if err := json.Unmarshal(body, &esRes); err != nil {
		return nil, fmt.Errorf("failed to decode ES response: %w", err)
	}

	res := make(map[int]VariantStockInfo)
	for _, hit := range esRes.Hits.Hits {
		pid := hit.Source.ID
		for _, v := range hit.Source.Variants {
			res[v.ID] = VariantStockInfo{
				ProductID: pid,
				VariantID: v.ID,
				SKU:       v.SKU,
				Stock:     v.Stock,
			}
		}
	}
	return res, nil
}

func compareStocksAtVariantLevel(cat map[int]VariantStockInfo, es map[int]VariantStockInfo) []Discrepancy {
	var diffs []Discrepancy

	// Track all unique variant IDs from both sources
	allVariantIDs := make(map[int]bool)
	for id := range cat {
		allVariantIDs[id] = true
	}
	for id := range es {
		allVariantIDs[id] = true
	}

	for vid := range allVariantIDs {
		catInfo := cat[vid]
		esInfo := es[vid]

		if catInfo.Stock != esInfo.Stock {
			pid := catInfo.ProductID
			if pid == 0 {
				pid = esInfo.ProductID
			}
			sku := catInfo.SKU
			if sku == "" {
				sku = esInfo.SKU
			}

			diffs = append(diffs, Discrepancy{
				ProductID:   pid,
				VariantID:   vid,
				SKU:         sku,
				CatalystQty: catInfo.Stock,
				ESQty:       esInfo.Stock,
				Diff:        catInfo.Stock - esInfo.Stock,
			})
		}
	}

	sort.Slice(diffs, func(i, j int) bool {
		if diffs[i].ProductID != diffs[j].ProductID {
			return diffs[i].ProductID < diffs[j].ProductID
		}
		return diffs[i].VariantID < diffs[j].VariantID
	})
	return diffs
}

func formatMarkdown(diffs []Discrepancy, esURL string) string {
	var sb strings.Builder
	sb.WriteString("##### Hi @channel, Ada perbedaan stock antara XMS Catalyst & Voila UF, minta tolong dicek yah..\n")
	sb.WriteString("| Product ID | Variant ID | SKU | XMS Catalyst | Voila UF | Diff |\n")
	sb.WriteString("| :--- | :--- | :--- | :---: | :---: | :---: |\n")
	for _, d := range diffs {
		catVal := fmt.Sprintf("[%d](%s/voila/stock/office/%d)", d.CatalystQty, XMS_CATALYST_BASE_URL, d.ProductID)
		voilaVal := fmt.Sprintf("[%d](%s/products/%d)", d.ESQty, VOILA_UF_BASE_URL, d.ProductID)

		sb.WriteString(fmt.Sprintf("| %d | %d | %s | %s | %s | **%d** |\n",
			d.ProductID, d.VariantID, d.SKU, catVal, voilaVal, d.Diff))
	}
	return sb.String()
}
