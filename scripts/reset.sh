#!/bin/bash
# scripts/reset.sh
# Reset the pipeline (clean start)

set -e

echo "🔄 Resetting Crypto Trading Pipeline..."

# Confirm reset
read -p "This will delete all data and restart the pipeline. Continue? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Reset cancelled."
    exit 1
fi

# Stop services
echo "🛑 Stopping services..."
docker-compose down

# Remove volumes
echo "🗑️  Removing data volumes..."
docker-compose down -v

# Remove logs
echo "🗑️  Clearing logs..."
rm -rf logs/*

# Rebuild and start
echo "🔨 Rebuilding and starting..."
docker-compose build
docker-compose up -d

echo "✅ Pipeline reset complete!"
