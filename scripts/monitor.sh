#!/bin/bash
# scripts/monitor.sh
# Real-time monitoring for the crypto trading pipeline

show_dashboard() {
    echo "📊 Crypto Trading Pipeline Monitor"
    echo "Press Ctrl+C to exit"
    echo ""

    while true; do
        clear
        echo "=== CRYPTO TRADING PIPELINE MONITOR ==="
        echo "$(date)"
        echo ""
        
        # Service status
        echo "🔍 SERVICE STATUS:"
        docker-compose ps --format "table {{.Name}}\t{{.Status}}"
        echo ""
        
        # Database stats
        echo "📊 DATABASE STATS (Last 5 minutes):"
        RAW_COUNT=$(docker-compose exec -T postgres psql -U pipeline_user -d crypto_pipeline -t -c "SELECT COUNT(*) FROM raw_trades WHERE trade_time >= NOW() - INTERVAL '5 minutes';" 2>/dev/null | xargs || echo "0")
        PROCESSED_COUNT=$(docker-compose exec -T postgres psql -U pipeline_user -d crypto_pipeline -t -c "SELECT COUNT(*) FROM processed_trades WHERE window_start >= NOW() - INTERVAL '5 minutes';" 2>/dev/null | xargs || echo "0")
        
        echo "Raw trades: $RAW_COUNT"
        echo "Processed windows: $PROCESSED_COUNT"
        echo ""
        
        # Latest prices
        echo "💰 LATEST PRICES:"
        docker-compose exec -T postgres psql -U pipeline_user -d crypto_pipeline -t -c "SELECT symbol, current_price, last_updated FROM latest_market_data ORDER BY symbol;" 2>/dev/null | head -10
        echo ""
        
        # Resource usage
        echo "💻 RESOURCE USAGE:"
        docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}" | head -6
        
        sleep 10
    done
}

show_logs() {
    SERVICE=$1
    case $SERVICE in
        processor)
            echo "📊 Showing processor logs..."
            docker-compose logs -f stream-processor
            ;;
        producer)
            echo "📡 Showing producer logs..."
            docker-compose logs -f binance-producer
            ;;
        db)
            echo "🗄️ Showing database logs..."
            docker-compose logs -f postgres
            ;;
        *)
            echo "❌ Unknown service: $SERVICE"
            echo "Available options: processor | producer | db"
            exit 1
            ;;
    esac
}

# Entry point
if [ -z "$1" ]; then
    show_dashboard
else
    show_logs "$1"
fi
