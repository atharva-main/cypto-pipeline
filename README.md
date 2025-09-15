# 🚀 Real-Time Crypto Trading Data Pipeline

A comprehensive real-time data pipeline that streams live cryptocurrency trading data from Binance, processes it through Apache Kafka, performs ETL transformations, stores results in PostgreSQL, and visualizes everything in Grafana.

## 🏗️ Architecture

```
Binance WebSocket → Kafka (raw_data) → Stream Processor → Kafka (processed_data) → PostgreSQL → Grafana
```

### Components:

1. **Data Producer**: WebSocket connection to Binance streaming live trade data to Kafka
2. **Stream Processor**: Consumes raw data, performs ETL transformations, and aggregates metrics
3. **Apache Kafka**: Message broker for reliable data streaming
4. **PostgreSQL**: Data warehouse for raw and processed trading data
5. **Grafana**: Real-time dashboard for data visualization
6. **Kafka UI**: Web interface for Kafka monitoring

## 📋 Features

- ✅ Real-time streaming from Binance WebSocket API
- ✅ Fault-tolerant message processing with Kafka
- ✅ ETL pipeline with data validation and transformation
- ✅ 1-minute rolling window aggregations (OHLCV data)
- ✅ PostgreSQL storage with optimized schema
- ✅ Real-time Grafana dashboards
- ✅ Docker-based deployment
- ✅ Comprehensive logging and monitoring
- ✅ Health checks and alerting
- ✅ Horizontal scalability

## 🛠️ Prerequisites

- Docker 20.10+
- Docker Compose 2.0+
- At least 4GB RAM
- 2GB free disk space

## 🚀 Quick Start

### 1. Clone and Setup

```bash
git clone <repository-url>
cd crypto-trading-pipeline
chmod +x scripts/*.sh
./scripts/setup.sh
```

### 2. Start the Pipeline

```bash
./scripts/start.sh
```

### 3. Access Services

- **Grafana Dashboard**: http://localhost:3000 (admin/admin123)
- **Kafka UI**: http://localhost:8080
- **PostgreSQL**: localhost:5432 (pipeline_user/pipeline_password)

### 4. Monitor the Pipeline

```bash
# View all logs
./scripts/logs.sh

# View specific service logs
./scripts/logs.sh producer
./scripts/logs.sh processor

# Real-time monitoring
./scripts/monitor.sh

# Health check
./scripts/health-check.sh
```

### 5. Stop the Pipeline

```bash
./scripts/stop.sh
```

## 📊 Data Flow

### Raw Data Schema (`raw_trades`)
```sql
CREATE TABLE raw_trades (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    price DECIMAL(20, 8) NOT NULL,
    quantity DECIMAL(20, 8) NOT NULL,
    trade_time TIMESTAMP WITH TIME ZONE NOT NULL,
    trade_id BIGINT,
    is_buyer_maker BOOLEAN,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

### Processed Data Schema (`processed_trades`)
```sql
CREATE TABLE processed_trades (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    avg_price DECIMAL(20, 8) NOT NULL,
    total_volume DECIMAL(20, 8) NOT NULL,
    trade_count INTEGER NOT NULL,
    min_price DECIMAL(20, 8) NOT NULL,
    max_price DECIMAL(20, 8) NOT NULL,
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    window_end TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker endpoints | `kafka:29092` |
| `POSTGRES_HOST` | PostgreSQL host | `postgres` |
| `POSTGRES_DB` | Database name | `crypto_pipeline` |
| `BINANCE_SYMBOLS` | Trading pairs to stream | `btcusdt,ethusdt,adausdt` |
| `WINDOW_SIZE_MINUTES` | Aggregation window size | `1` |
| `LOG_LEVEL` | Logging level | `INFO` |

### Custom Configuration

Edit `config/pipeline.yml` to customize:
- Kafka topics and consumer groups
- Database connection settings
- Binance symbols to track
- Processing window sizes
- Logging configuration

## 📈 Dashboard Metrics

The Grafana dashboard displays:

1. **Live Price Updates**: Real-time price feed for all symbols
2. **Price Trends**: 1-minute average price charts
3. **Volume Analysis**: Trading volume trends and patterns
4. **Trade Activity**: Recent trades and transaction counts
5. **Market Summary**: 24h price changes and statistics

## 🔍 Monitoring & Observability

### Health Checks
```bash
# Check all services
./scripts/health-check.sh

# Check data flow
docker-compose exec postgres psql -U pipeline_user -d crypto_pipeline -c "SELECT COUNT(*) FROM raw_trades WHERE trade_time >= NOW() - INTERVAL '1 hour';"
```

### Logs
- Producer logs: `/logs/producer.log`
- Processor logs: `/logs/processor.log`
- Docker logs: `docker-compose logs -f [service]`

### Key Metrics to Monitor
- Message throughput (trades/second)
- Processing latency
- Database connection pool usage
- Kafka consumer lag
- Memory and CPU usage

## 🚨 Troubleshooting

### Common Issues

1. **Kafka Connection Failed**
   ```bash
   # Check if Kafka is ready
   docker-compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
   ```

2. **Database Connection Issues**
   ```bash
   # Check PostgreSQL
   docker-compose exec postgres pg_isready -U pipeline_user -d crypto_pipeline
   ```

3. **No Data in Dashboard**
   ```bash
   # Check if producer is running and receiving data
   ./scripts/logs.sh producer
   
   # Check if processor is consuming
   ./scripts/logs.sh processor
   ```

4. **WebSocket Connection Issues**
   - Check internet connectivity
   - Binance API might be rate limiting
   - Restart the producer service

### Reset Pipeline
```bash
# Complete reset (WARNING: Deletes all data)
./scripts/reset.sh
```

## 🔧 Development

### Project Structure
```
crypto-trading-pipeline/
├── docker-compose.yml          # Service orchestration
├── sql/init.sql               # Database schema
├── src/
│   ├── producer/              # Data producer (Binance → Kafka)
│   └── processor/             # Stream processor (Kafka → PostgreSQL)
├── config/                    # Configuration files
├── scripts/                   # Management scripts
├── grafana/                   # Dashboard and provisioning
├── logs/                      # Application logs
└── README.md                  # This file
```

### Adding New Data Sources

1. Create new producer in `src/producer/`
2. Update `docker-compose.yml` with new service
3. Modify database schema if needed
4. Update Grafana dashboards

### Scaling Considerations

- **Horizontal Scaling**: Add more Kafka partitions and consumer instances
- **Database**: Consider TimescaleDB for time-series optimization
- **Caching**: Add Redis for frequently accessed data
- **Load Balancing**: Use multiple WebSocket connections

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes and test thoroughly
4. Submit a pull request

## 📜 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Binance for providing excellent WebSocket API
- Apache Kafka for reliable streaming
- Grafana for beautiful visualizations
- PostgreSQL for robust data storage

## 📞 Support

For issues and questions:
1. Check the troubleshooting section
2. Review logs with `./scripts/logs.sh`
3. Run health checks with `./scripts/health-check.sh`
4. Open an issue on GitHub

---

**Happy Trading! 📈**