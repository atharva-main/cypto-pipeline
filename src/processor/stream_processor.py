# src/processor/stream_processor.py
import json
import logging
import os
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from threading import Thread
from typing import Dict, List, Optional

import psycopg2 # type: ignore
from kafka import KafkaConsumer, KafkaProducer # type: ignore
from kafka.errors import KafkaError # type: ignore
from psycopg2.extras import RealDictCursor # type: ignore

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/processor.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class StreamProcessor:
    """
    Stream processor for crypto trading data pipeline
    Consumes from Kafka, performs ETL, and loads data into PostgreSQL
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.consumer = None
        self.producer = None
        self.db_connection = None
        self.running = True
        
        # In-memory window storage for aggregations
        self.trade_windows = defaultdict(list)
        self.window_size_minutes = config.get('window_size_minutes', 1)
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
    
    def _initialize_kafka_consumer(self):
        """Initialize Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                self.config['kafka']['raw_topic'],
                bootstrap_servers=self.config['kafka']['bootstrap_servers'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                group_id='stream-processor',
                auto_offset_reset='latest',
                enable_auto_commit=False,
                max_poll_records=100,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            logger.info("Kafka consumer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise
    
    def _initialize_kafka_producer(self):
        """Initialize Kafka producer for processed data"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config['kafka']['bootstrap_servers'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                retries=3,
                acks='all'
            )
            logger.info("Kafka producer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    def _initialize_database(self):
        """Initialize PostgreSQL connection"""
        try:
            db_config = self.config['database']
            self.db_connection = psycopg2.connect(
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['database'],
                user=db_config['user'],
                password=db_config['password']
            )
            self.db_connection.autocommit = True
            logger.info("Database connection initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database connection: {e}")
            raise
    
    def _validate_trade_data(self, trade_data: Dict) -> bool:
        """Validate incoming trade data"""
        required_fields = ['symbol', 'price', 'quantity', 'trade_time']
        
        try:
            # Check required fields
            for field in required_fields:
                if field not in trade_data:
                    logger.warning(f"Missing required field: {field}")
                    return False
            
            # Validate data types and ranges
            if not isinstance(trade_data['price'], (int, float)) or trade_data['price'] <= 0:
                logger.warning(f"Invalid price: {trade_data['price']}")
                return False
            
            if not isinstance(trade_data['quantity'], (int, float)) or trade_data['quantity'] <= 0:
                logger.warning(f"Invalid quantity: {trade_data['quantity']}")
                return False
            
            # Validate symbol format
            symbol = trade_data['symbol']
            if not symbol or not isinstance(symbol, str) or len(symbol) < 6:
                logger.warning(f"Invalid symbol: {symbol}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating trade data: {e}")
            return False
    
    def _transform_trade_data(self, trade_data: Dict) -> Dict:
        """Transform raw trade data"""
        try:
            transformed = {
                'symbol': trade_data['symbol'].upper(),
                'price': float(trade_data['price']),
                'quantity': float(trade_data['quantity']),
                'trade_time': trade_data['trade_time'],
                'trade_id': trade_data.get('trade_id'),
                'is_buyer_maker': trade_data.get('is_buyer_maker', False),
                'volume_usd': float(trade_data['price']) * float(trade_data['quantity']),
                'processed_at': datetime.utcnow().isoformat()
            }
            
            # Add technical indicators if needed
            transformed['price_rounded'] = round(transformed['price'], 2)
            
            return transformed
            
        except Exception as e:
            logger.error(f"Error transforming trade data: {e}")
            return None
    
    def _store_raw_trade(self, trade_data: Dict):
        """Store raw trade data in PostgreSQL"""
        try:
            cursor = self.db_connection.cursor()
            
            insert_query = """
                INSERT INTO raw_trades 
                (symbol, price, quantity, trade_time, trade_id, is_buyer_maker, raw_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_query, (
                trade_data['symbol'],
                trade_data['price'],
                trade_data['quantity'],
                trade_data['trade_time'],
                trade_data.get('trade_id'),
                trade_data.get('is_buyer_maker', False),
                json.dumps(trade_data)
            ))
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error storing raw trade: {e}")
    
    def _add_to_window(self, trade_data: Dict):
        """Add trade data to aggregation window"""
        symbol = trade_data['symbol']
        trade_time = datetime.fromisoformat(trade_data['trade_time'].replace('Z', '+00:00'))
        
        # Add to current window
        self.trade_windows[symbol].append({
            'price': trade_data['price'],
            'quantity': trade_data['quantity'],
            'volume_usd': trade_data['volume_usd'],
            'trade_time': trade_time,
            'is_buyer_maker': trade_data.get('is_buyer_maker', False)
        })
        
        # Clean old data (keep only last 2 minutes for safety)
        cutoff_time = datetime.utcnow() - timedelta(minutes=2)
        self.trade_windows[symbol] = [
            trade for trade in self.trade_windows[symbol]
            if trade['trade_time'] > cutoff_time
        ]
    
    def _calculate_window_aggregations(self, symbol: str, window_start: datetime, window_end: datetime) -> Optional[Dict]:
        """Calculate aggregations for a time window"""
        try:
            trades = self.trade_windows.get(symbol, [])
            
            # Filter trades in window
            window_trades = [
                trade for trade in trades
                if window_start <= trade['trade_time'] < window_end
            ]
            
            if not window_trades:
                return None
            
            prices = [trade['price'] for trade in window_trades]
            volumes = [trade['quantity'] for trade in window_trades]
            volume_usd = [trade['volume_usd'] for trade in window_trades]
            
            aggregation = {
                'symbol': symbol,
                'avg_price': sum(prices) / len(prices),
                'total_volume': sum(volumes),
                'total_volume_usd': sum(volume_usd),
                'trade_count': len(window_trades),
                'min_price': min(prices),
                'max_price': max(prices),
                'window_start': window_start.isoformat(),
                'window_end': window_end.isoformat(),
                'vwap': sum(p * v for p, v in zip(prices, volumes)) / sum(volumes) if sum(volumes) > 0 else 0,
                'buy_volume': sum(trade['quantity'] for trade in window_trades if not trade['is_buyer_maker']),
                'sell_volume': sum(trade['quantity'] for trade in window_trades if trade['is_buyer_maker'])
            }
            
            return aggregation
            
        except Exception as e:
            logger.error(f"Error calculating window aggregations: {e}")
            return None
    
    def _store_processed_data(self, aggregation: Dict):
        """Store processed aggregation data"""
        try:
            cursor = self.db_connection.cursor()
            
            insert_query = """
                INSERT INTO processed_trades 
                (symbol, avg_price, total_volume, trade_count, min_price, max_price, window_start, window_end)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_query, (
                aggregation['symbol'],
                aggregation['avg_price'],
                aggregation['total_volume'],
                aggregation['trade_count'],
                aggregation['min_price'],
                aggregation['max_price'],
                aggregation['window_start'],
                aggregation['window_end']
            ))
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error storing processed data: {e}")
    
    def _send_to_processed_topic(self, aggregation: Dict):
        """Send processed data to Kafka topic"""
        try:
            future = self.producer.send(
                topic=self.config['kafka']['processed_topic'],
                key=aggregation['symbol'],
                value=aggregation
            )
            future.get(timeout=10)  # Wait for send to complete
            
        except Exception as e:
            logger.error(f"Error sending to processed topic: {e}")
    
    def _process_window_aggregations(self):
        """Background thread to process window aggregations"""
        while self.running:
            try:
                current_time = datetime.utcnow()
                window_start = current_time.replace(second=0, microsecond=0) - timedelta(minutes=self.window_size_minutes)
                window_end = current_time.replace(second=0, microsecond=0)
                
                # Process aggregations for each symbol
                for symbol in list(self.trade_windows.keys()):
                    aggregation = self._calculate_window_aggregations(symbol, window_start, window_end)
                    if aggregation:
                        # Store in database
                        self._store_processed_data(aggregation)
                        # Send to Kafka processed topic
                        self._send_to_processed_topic(aggregation)
                        logger.debug(f"Processed window for {symbol}: {aggregation['trade_count']} trades")
                
                # Wait for next minute
                time.sleep(60)
                
            except Exception as e:
                logger.error(f"Error in window aggregation thread: {e}")
                time.sleep(10)
    
    def _update_market_summary(self, trade_data: Dict):
        """Update market summary table with latest trade data"""
        try:
            cursor = self.db_connection.cursor()
            
            # Upsert market summary
            upsert_query = """
                INSERT INTO market_summary (symbol, current_price, volume_24h, last_updated)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (symbol) 
                DO UPDATE SET
                    current_price = EXCLUDED.current_price,
                    volume_24h = COALESCE(market_summary.volume_24h, 0) + %s,
                    last_updated = EXCLUDED.last_updated
                WHERE market_summary.symbol = EXCLUDED.symbol
            """
            
            cursor.execute(upsert_query, (
                trade_data['symbol'],
                trade_data['price'],
                trade_data['quantity'],
                trade_data['trade_time'],
                trade_data['quantity']
            ))
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error updating market summary: {e}")
    
    def run(self):
        """Main processing loop"""
        logger.info("Starting stream processor...")
        
        # Initialize components
        self._initialize_kafka_consumer()
        self._initialize_kafka_producer()
        self._initialize_database()
        
        # Start background aggregation thread
        aggregation_thread = Thread(target=self._process_window_aggregations, daemon=True)
        aggregation_thread.start()
        
        logger.info("Stream processor started successfully")
        
        try:
            while self.running:
                try:
                    # Poll for messages
                    message_batch = self.consumer.poll(timeout_ms=1000)
                    
                    if message_batch:
                        for topic_partition, messages in message_batch.items():
                            for message in messages:
                                try:
                                    # Validate message
                                    if not self._validate_trade_data(message.value):
                                        continue
                                    
                                    # Transform data
                                    transformed_data = self._transform_trade_data(message.value)
                                    if not transformed_data:
                                        continue
                                    
                                    # Store raw trade
                                    self._store_raw_trade(transformed_data)
                                    
                                    # Add to aggregation window
                                    self._add_to_window(transformed_data)
                                    
                                    # Update market summary
                                    self._update_market_summary(transformed_data)
                                    
                                    logger.debug(f"Processed trade: {transformed_data['symbol']} @ {transformed_data['price']}")
                                    
                                except Exception as e:
                                    logger.error(f"Error processing message: {e}")
                        
                        # Commit offsets
                        self.consumer.commit()
                
                except Exception as e:
                    logger.error(f"Error in main processing loop: {e}")
                    time.sleep(5)
        
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
        
        finally:
            # Cleanup
            self.running = False
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.flush()
                self.producer.close()
            if self.db_connection:
                self.db_connection.close()
            
            logger.info("Stream processor shutdown complete")


def load_config() -> Dict:
    """Load configuration from environment variables"""
    return {
        'kafka': {
            'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'raw_topic': os.getenv('KAFKA_TOPIC_RAW', 'raw_data'),
            'processed_topic': os.getenv('KAFKA_TOPIC_PROCESSED', 'processed_data')
        },
        'database': {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'database': os.getenv('POSTGRES_DB', 'crypto_pipeline'),
            'user': os.getenv('POSTGRES_USER', 'pipeline_user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'pipeline_password')
        },
        'window_size_minutes': int(os.getenv('WINDOW_SIZE_MINUTES', 1))
    }


def main():
    """Main entry point"""
    config = load_config()
    processor = StreamProcessor(config)
    
    try:
        processor.run()
    except Exception as e:
        logger.error(f"Stream processor failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()