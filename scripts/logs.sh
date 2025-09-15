#!/bin/bash
# scripts/logs.sh
# View pipeline logs

if [ "$1" == "producer" ]; then
    echo "📊 Showing producer logs..."
    docker-compose logs -f data-producer
elif [ "$1" == "kafka" ]; then
    echo "📊 Showing kafka logs..."
    docker-compose logs -f kafka
elif [ "$1" == "postgres" ]; then
    echo "📊 Showing postgres logs..."
    docker-compose logs -f postgres
elif [ "$1" == "grafana" ]; then
    echo "📊 Showing grafana logs..."
    docker-compose logs -f grafana
else
    echo "📊 Showing all logs..."
    docker-compose logs -f
fi