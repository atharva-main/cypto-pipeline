# Makefile for Crypto Trading Pipeline

.PHONY: help setup start stop logs health monitor reset clean build

# Default target
help: ## Show this help message
	@echo "🚀 Crypto Trading Pipeline Commands"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

# setup: ## Initial setup of the project
# 	@echo "🛠️  Setting up the project..."
# 	@chmod +x scripts/*.sh
# 	@./scripts/setup.sh

build: ## Build all Docker images
	@echo "🔨 Building Docker images..."
	@docker-compose build

start: ## Start the pipeline
	@echo "🚀 Starting pipeline..."
	@./scripts/start.sh

stop: ## Stop the pipeline
	@echo "🛑 Stopping pipeline..."
	@./scripts/stop.sh

restart: stop start ## Restart the pipeline

logs: ## Show all logs
	@./scripts/logs.sh

logs-producer: ## Show producer logs only
	@./scripts/logs.sh producer

logs-processor: ## Show processor logs only
	@./scripts/logs.sh processor

logs-kafka: ## Show Kafka logs only
	@./scripts/logs.sh kafka

health: ## Check pipeline health
	@./scripts/health-check.sh

monitor: ## Start real-time monitoring
	@./scripts/monitor.sh

reset: ## Reset the pipeline (WARNING: Deletes all data)
	@./scripts/reset.sh

clean: ## Clean up containers and volumes
	@echo "🧹 Cleaning up..."
	@docker-compose down -v --remove-orphans
	@docker system prune -f

status: ## Show service status
	@docker-compose ps

shell-postgres: ## Connect to PostgreSQL shell
	@docker-compose exec postgres psql -U pipeline_user -d crypto_pipeline

shell-kafka: ## Connect to Kafka container shell
	@docker-compose exec kafka bash

topics: ## List Kafka topics
	@docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

create-topic: ## Create a new Kafka topic (usage: make create-topic TOPIC=mytopic)
	@docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic $(TOPIC) --partitions 3 --replication-factor 1

describe-topic: ## Describe a Kafka topic (usage: make describe-topic TOPIC=raw_data)
	@docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic $(TOPIC)

consume: ## Consume from a Kafka topic (usage: make consume TOPIC=raw_data)
	@docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic $(TOPIC) --from-beginning

db-stats: ## Show database statistics
	@echo "📊 Database Statistics:"
	@docker-compose exec postgres psql -U pipeline_user -d crypto_pipeline -c "\
		SELECT \
			'Raw Trades (Last Hour)' as metric, \
			COUNT(*) as count \
		FROM raw_trades \
		WHERE trade_time >= NOW() - INTERVAL '1 hour' \
		UNION ALL \
		SELECT \
			'Processed Trades (Last Hour)' as metric, \
			COUNT(*) as count \
		FROM processed_trades \
		WHERE window_start >= NOW() - INTERVAL '1 hour' \
		UNION ALL \
		SELECT \
			'Total Symbols' as metric, \
			COUNT(DISTINCT symbol) as count \
		FROM raw_trades;"

backup-db: ## Backup database
	@echo "💾 Backing up database..."
	@mkdir -p backups
	@docker-compose exec postgres pg_dump -U pipeline_user crypto_pipeline > backups/backup_$(shell date +%Y%m%d_%H%M%S).sql
	@echo "✅ Backup saved to backups/"

restore-db: ## Restore database (usage: make restore-db FILE=backup.sql)
	@echo "🔄 Restoring database from $(FILE)..."
	@docker-compose exec -T postgres psql -U pipeline_user -d crypto_pipeline < $(FILE)
	@echo "✅ Database restored"

grafana-reset: ## Reset Grafana admin password
	@echo "🔐 Resetting Grafana password..."
	@docker-compose exec grafana grafana-cli admin reset-admin-password admin123
	@echo "✅ Password reset to: admin123"

update: ## Pull latest images and restart
	@echo "📥 Updating images..."
	@docker-compose pull
	@docker-compose up -d

dev-setup: ## Setup development environment
	@echo "🔧 Setting up development environment..."
	@python -m pip install -r requirements-dev.txt
	@pre-commit install

test: ## Run tests
	@echo "🧪 Running tests..."
	@python -m pytest tests/ -v

lint: ## Run linting
	@echo "🔍 Running linting..."
	@flake8 src/
	@black --check src/

format: ## Format code
	@echo "✨ Formatting code..."
	@black src/
	@isort src/

# Development commands
dev-producer: ## Run producer locally for development
	@cd src/producer && python binance_producer.py

dev-processor: ## Run processor locally for development
	@cd src/processor && python stream_processor.py

# Monitoring commands
metrics: ## Show key metrics
	@echo "📈 Key Metrics (Last 10 minutes):"
	@docker-compose exec postgres psql -U pipeline_user -d crypto_pipeline -c "\
		SELECT \
			symbol, \
			COUNT(*) as trades, \
			ROUND(AVG(price)::numeric, 2) as avg_price, \
			ROUND(SUM(quantity)::numeric, 2) as volume \
		FROM raw_trades \
		WHERE trade_time >= NOW() - INTERVAL '10 minutes' \
		GROUP BY symbol \
		ORDER BY trades DESC;"

prices: ## Show latest prices
	@echo "💰 Latest Prices:"
	@docker-compose exec postgres psql -U pipeline_user -d crypto_pipeline -c "\
		SELECT symbol, current_price, last_updated \
		FROM latest_market_data \
		ORDER BY symbol;"

# URLs
urls: ## Show service URLs
	@echo "🌐 Service URLs:"
	@echo "  • Grafana Dashboard: http://localhost:3000 (admin/admin123)"
	@echo "  • Kafka UI: http://localhost:8080"
	@echo "  • PostgreSQL: localhost:5432 (pipeline_user/pipeline_password)"

# Quick status check
quick-check: ## Quick health and status check
	@echo "⚡ Quick Status Check:"
	@echo ""
	@echo "Services:"
	@docker-compose ps --format "table {{.Name}}\t{{.Status}}" | head -10
	@echo ""
	@echo "Recent Activity:"
	@docker-compose exec postgres psql -U pipeline_user -d crypto_pipeline -t -c "SELECT COUNT(*) || ' trades in last 5 min' FROM raw_trades WHERE trade_time >= NOW() - INTERVAL '5 minutes';" 2>/dev/null | xargs
	@echo ""
	@echo "Latest Update:"
	@docker-compose exec postgres psql -U pipeline_user -d crypto_pipeline -t -c "SELECT 'Last trade: ' || MAX(trade_time) FROM raw_trades;" 2>/dev/null | xargs