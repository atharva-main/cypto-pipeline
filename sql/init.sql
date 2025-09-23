-- Database initialization script for crypto trading pipeline

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create raw trades table
CREATE TABLE IF NOT EXISTS raw_trades (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    price DECIMAL(20, 8) NOT NULL,
    quantity DECIMAL(20, 8) NOT NULL,
    trade_time TIMESTAMP WITH TIME ZONE NOT NULL,
    trade_id BIGINT,
    is_buyer_maker BOOLEAN,
    raw_data JSONB,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()  -- renamed to match Python
);

-- Create processed trades table with aggregated data
CREATE TABLE IF NOT EXISTS processed_trades (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    avg_price DECIMAL(20, 8) NOT NULL,
    total_volume DECIMAL(20, 8) NOT NULL,
    trade_count INTEGER NOT NULL,
    min_price DECIMAL(20, 8) NOT NULL,
    max_price DECIMAL(20, 8) NOT NULL,
    vwap DECIMAL(20, 8),                  -- added for processor
    total_volume_usd DECIMAL(30, 10),     -- added for processor
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    window_end TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create market summary table for dashboard (simplified to match processor)
CREATE TABLE IF NOT EXISTS market_summary (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    current_price DECIMAL(20, 8) NOT NULL,
    volume_24h DECIMAL(20, 8),
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_raw_trades_symbol_time ON raw_trades(symbol, trade_time);
CREATE INDEX IF NOT EXISTS idx_raw_trades_time ON raw_trades(trade_time DESC);

CREATE INDEX IF NOT EXISTS idx_processed_trades_symbol_window ON processed_trades(symbol, window_start);
CREATE INDEX IF NOT EXISTS idx_processed_trades_window_start ON processed_trades(window_start DESC);

CREATE INDEX IF NOT EXISTS idx_market_summary_symbol ON market_summary(symbol);
CREATE INDEX IF NOT EXISTS idx_market_summary_updated ON market_summary(last_updated DESC);

-- Create a view for latest market data
CREATE OR REPLACE VIEW latest_market_data AS
SELECT DISTINCT ON (symbol)
    symbol,
    current_price,
    volume_24h,
    last_updated
FROM market_summary
ORDER BY symbol, last_updated DESC;

-- Create a view for recent trading activity
CREATE OR REPLACE VIEW recent_trading_activity AS
SELECT 
    symbol,
    COUNT(*) as trade_count,
    AVG(price) as avg_price,
    SUM(quantity) as total_volume,
    MIN(price) as min_price,
    MAX(price) as max_price,
    MIN(trade_time) as first_trade,
    MAX(trade_time) as last_trade
FROM raw_trades
WHERE trade_time >= NOW() - INTERVAL '1 hour'
GROUP BY symbol
ORDER BY total_volume DESC;

-- Insert some initial test data (optional)
INSERT INTO market_summary (symbol, current_price, volume_24h, last_updated)
VALUES 
    ('BTCUSDT', 45000.00, 1000.50, NOW()),
    ('ETHUSDT', 3000.00, 800.25, NOW()),
    ('ADAUSDT', 0.45, 5000.75, NOW())
ON CONFLICT DO NOTHING;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO pipeline_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO pipeline_user;
