#!/bin/bash
# scripts/stop.sh
# Stop the crypto trading pipeline

set -e

echo "🛑 Stopping Crypto Trading Pipeline..."

# Stop all services
docker-compose down

echo "✅ Pipeline stopped successfully!"
