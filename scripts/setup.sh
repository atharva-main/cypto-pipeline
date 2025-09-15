#!/bin/bash
# scripts/setup.sh
# Main setup script for the crypto trading pipeline

set -e

echo "🚀 Setting up Crypto Trading Pipeline..."

# Create necessary directories
echo "📁 Creating project directories..."
mkdir -p logs
mkdir -p data
mkdir -p grafana/dashboards
mkdir -p grafana/provisioning/datasources
mkdir -p grafana/provisioning/dashboards

# Copy dashboard configuration
echo "📊 Setting up Grafana dashboard..."
cat > grafana/dashboards/crypto-dashboard.json << 'EOF'
{
  "dashboard": {
    "id": null,
    "title": "Crypto Trading Pipeline Dashboard",
    "tags": ["crypto", "trading", "real-time"],
    "timezone": "browser",
    "refresh": "5s",
    "time": {
      "from": "now-30m",
      "to": "now"
    },
    "panels": [
      {
        "id": 1,
        "title": "Live Prices",
        "type": "stat",
        "targets": [
          {
            "rawSql": "SELECT symbol, current_price FROM latest_market_data ORDER BY symbol",
            "format": "table"
          }
        ],
        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0}
      }
    ]
  }
}
EOF

# Check Docker installation
echo "🐳 Checking Docker installation..."
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Build Docker images
echo "🔨 Building Docker images..."
docker-compose build

echo "✅ Setup complete!"
echo ""
echo "📋 Next steps:"
echo "1. Run: ./scripts/start.sh to start the pipeline"
echo "2. Access Grafana at http://localhost:3000 (admin/admin123)"
echo "3. Access Kafka UI at http://localhost:8080"
echo "4. Monitor logs: ./scripts/logs.sh"
