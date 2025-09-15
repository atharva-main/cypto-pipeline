#!/bin/bash
# scripts/health-check.sh
# Health check script for the pipeline

set -e

echo "🔍 Checking Pipeline Health..."

# Check if containers are running
echo "📦 Container Status:"
docker-compose ps

echo ""
echo "🔍 Service Health Checks:"

# Check Kafka
echo -n "• Kafka: "
if docker-compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &> /dev/null; then
    echo "✅ Healthy"
else
    echo "❌ Unhealthy"
fi

# Check PostgreSQL
echo -n "• PostgreSQL: "
if docker-compose exec -T postgres pg_isready -U pipeline_user -d crypto_pipeline &> /dev/null; then
    echo "✅ Healthy"
else
    echo "❌ Unhealthy"
fi

# Check Grafana
echo -n "• Grafana: "
if curl -s http://localhost:3000/api/health &> /dev/null; then
    echo "✅ Healthy"
else
    echo "❌ Unhealthy"
fi

# Check data flow
echo ""
echo "📊 Data Flow Check:"
echo -n "• Raw trades in last 5 minutes: "
RAW_COUNT=$(docker-compose exec -T postgres psql -U pipeline_user -d crypto_pipeline -t -c "SELECT COUNT(*) FROM raw_trades WHERE trade_time >= NOW() - INTERVAL '5 minutes';" 2>/dev/null | xargs || echo "0")
echo "$RAW_COUNT"

echo -n "• Processed trades in last 5 minutes: "
PROCESSED_COUNT=$(docker-compose exec -T postgres psql -U pipeline_user -d crypto_pipeline -t -c "SELECT COUNT(*) FROM processed_trades WHERE window_start >= NOW() - INTERVAL '5 minutes';" 2>/dev/null | xargs || echo "0")
echo "$PROCESSED_COUNT"

if [ "$RAW_COUNT" -gt "0" ] && [ "$PROCESSED_COUNT" -gt "0" ]; then
    echo "✅ Data pipeline is working correctly!"
else
    echo "⚠️  Data pipeline may have issues. Check logs with ./scripts/logs.sh"
fi