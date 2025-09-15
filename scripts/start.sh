#!/bin/bash
# scripts/start.sh
# Start the crypto trading pipeline

set -e

echo "🚀 Starting Crypto Trading Pipeline..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Start services
echo "📦 Starting services..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 30

# Check service health
echo "🔍 Checking service health..."
docker-compose ps

# Show service URLs
echo ""
echo "✅ Pipeline started successfully!"
echo ""
echo "🌐 Service URLs:"
echo "  • Grafana Dashboard: http://localhost:3000 (admin/admin123)"
echo "  • Kafka UI: http://localhost:8080"
echo "  • PostgreSQL: localhost:5432 (pipeline_user/pipeline_password)"
echo ""
echo "📊 To view logs:"
echo "  • All services: docker-compose logs -f"
echo "  • Producer only: docker-compose logs -f data-producer"
echo "  • Processor only: docker-compose logs -f stream-processor"
echo ""
echo "🛑 To stop: ./scripts/stop.sh"
