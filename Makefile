# --- Variables ---
GRAFANA_DIR = grafana
LOG_DIR = logs
LOG_FILE = $(LOG_DIR)/stock.log
SCRIPT_DIR = stock-discrepancy/xmsc-xmsl/cmd

# --- Phony Targets ---
.PHONY: setup up down run tail clean restart help

# --- Help ---
help:
	@echo "Pelengkap Monitoring Stock Sync (Makefile)"
	@echo "----------------------------------------"
	@echo "make setup    - Menyiapkan folder log"
	@echo "make up       - Menjalankan Grafana & Loki stack (Docker)"
	@echo "make down      - Mematikan Grafana stack"
	@echo "make restart   - Restart Grafana stack"
	@echo "make run      - Jalankan script & kirim log ke Loki"
	@echo "make tail     - Pantau log di terminal"
	@echo "make clean    - Hapus file log lama"

# --- Setup ---
setup:
	mkdir -p $(LOG_DIR)

# --- Docker Control ---
up: setup
	cd $(GRAFANA_DIR) && docker-compose up -d

down:
	cd $(GRAFANA_DIR) && docker-compose down

restart: down up

# --- Script Execution ---
run: setup
	@echo "Menjalankan stock sync... Log dikirim ke logs/stock.log"
	go run $(SCRIPT_DIR)/main.go >> $(LOG_FILE)

# --- Logging ---
tail:
	tail -f $(LOG_FILE)

clean:
	rm -rf $(LOG_DIR)/*.log
	@echo "Logs cleaned."
