# src/processor/stream_processor.py
import json
import logging
import os
import sys
import time
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from threading import Thread, Lock
from typing import Dict, Optional

import psycopg2  # type: ignore
from psycopg2.pool import SimpleConnectionPool  # type: ignore
from kafka import KafkaConsumer, KafkaProducer  # type: ignore
from kafka.errors import KafkaError  # type: ignore
from psycopg2.extras import RealDictCursor  # type: ignore

# Ensure log directory exists (best-effort)
try:
    os.makedirs('/app/logs', exist_ok=True)
except Exception:
    pass

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
    Stream processor for crypto trading data pipeline.
    Consumes from Kafka, performs ETL, and loads data into PostgreSQL.
    """

    def __init__(self, config: Dict):
        self.config = config
        self.consumer: Optional[KafkaConsumer] = None
        self.producer: Optional[KafkaProducer] = None
        self.db_pool: Optional[SimpleConnectionPool] = None
        self.running = True

        # In-memory window storage for aggregations
        self.trade_windows = defaultdict(list)
        self.trade_windows_lock = Lock()

        self.window_size_minutes = int(config.get('window_size_minutes', 1))
        # retention buffer (minutes) to keep extra trade history for late-arriving records
        self.window_retention_buffer_minutes = max(1, self.window_size_minutes)

        # Kafka group id config
        self.consumer_group_id = config.get('consumer_group_id', 'stream-processor')

    #
    # Database helpers (pooled, resilient)
    #
    def _initialize_database(self):
        """Initialize PostgreSQL connection pool"""
        try:
            db_config = self.config['database']
            # small pool: adjust min/max according to throughput & environment
            self.db_pool = SimpleConnectionPool(
                minconn=1,
                maxconn=5,
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['database'],
                user=db_config['user'],
                password=db_config['password']
            )
            logger.info("Database connection pool initialized successfully")
        except Exception:
            logger.exception("Failed to initialize database connection pool")
            raise

    def _get_db_conn(self, retries: int = 3, backoff: float = 0.5):
        """Get a connection from the pool with basic retry/backoff."""
        if not self.db_pool:
            raise RuntimeError("DB pool not initialized")

        last_exc = None
        for attempt in range(retries):
            try:
                conn = self.db_pool.getconn()
                # ensure autocommit for simple inserts; you may switch to explicit transactions if desired
                conn.autocommit = True
                return conn
            except Exception as e:
                last_exc = e
                logger.warning(f"Failed to get DB connection (attempt {attempt + 1}/{retries}): {e}")
                time.sleep(backoff * (2 ** attempt))
        logger.exception("Exhausted retries getting DB connection")
        raise last_exc

    def _put_db_conn(self, conn):
        """Return connection to the pool."""
        try:
            if self.db_pool and conn:
                self.db_pool.putconn(conn)
        except Exception:
            logger.exception("Error returning DB connection to pool")

    #
    # Kafka initialization
    #
    def _initialize_kafka_consumer(self):
        """Initialize Kafka consumer"""
        try:
            kafka_cfg = self.config['kafka']
            self.consumer = KafkaConsumer(
                kafka_cfg['raw_topic'],
                bootstrap_servers=kafka_cfg['bootstrap_servers'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                group_id=self.consumer_group_id,
                auto_offset_reset='latest',
                enable_auto_commit=False,
                max_poll_records=500,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            logger.info("Kafka consumer initialized successfully")
        except Exception:
            logger.exception("Failed to initialize Kafka consumer")
            raise

    def _initialize_kafka_producer(self):
        """Initialize Kafka producer for processed data"""
        try:
            kafka_cfg = self.config['kafka']
            self.producer = KafkaProducer(
                bootstrap_servers=kafka_cfg['bootstrap_servers'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                retries=5,
                acks='all',
            )
            logger.info("Kafka producer initialized successfully")
        except Exception:
            logger.exception("Failed to initialize Kafka producer")
            raise

    #
    # Validation / transformation
    #
    def _validate_trade_data(self, trade_data: Dict) -> bool:
        """Validate incoming trade data"""
        required_fields = ['symbol', 'price', 'quantity', 'trade_time']
        try:
            for field in required_fields:
                if field not in trade_data:
                    logger.warning("Missing required field: %s", field)
                    return False

            if not isinstance(trade_data['price'], (int, float)) or float(trade_data['price']) <= 0:
                logger.warning("Invalid price: %s", trade_data.get('price'))
                return False

            if not isinstance(trade_data['quantity'], (int, float)) or float(trade_data['quantity']) <= 0:
                logger.warning("Invalid quantity: %s", trade_data.get('quantity'))
                return False

            symbol = trade_data['symbol']
            if not symbol or not isinstance(symbol, str) or len(symbol) < 3:
                logger.warning("Invalid symbol: %s", symbol)
                return False

            return True
        except Exception:
            logger.exception("Exception validating trade data")
            return False

    def _parse_trade_time(self, trade_time_str: str) -> datetime:
        """
        Parse ISO8601 trade time string; expected format includes 'Z' or timezone offset.
        Returns timezone-aware UTC datetime.
        """
        # Accept "2023-01-01T00:00:00Z" or "2023-01-01T00:00:00+00:00"
        try:
            if trade_time_str.endswith('Z'):
                dt = datetime.fromisoformat(trade_time_str.replace('Z', '+00:00'))
            else:
                dt = datetime.fromisoformat(trade_time_str)
            return dt.astimezone(timezone.utc)
        except Exception:
            # fallback: try parsing milliseconds epoch if given as int-like
            try:
                ms = int(trade_time_str)
                return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
            except Exception:
                logger.exception("Failed to parse trade_time: %s", trade_time_str)
                # return current time as fallback (but we prefer not to)
                return datetime.now(timezone.utc)

    def _transform_trade_data(self, trade_data: Dict) -> Optional[Dict]:
        """Transform raw trade data into normalized shape"""
        try:
            trade_time_dt = self._parse_trade_time(trade_data['trade_time'])
            price = float(trade_data['price'])
            quantity = float(trade_data['quantity'])
            transformed = {
                'symbol': trade_data['symbol'].upper(),
                'price': price,
                'quantity': quantity,
                'trade_time': trade_time_dt,  # keep as datetime internally
                'trade_id': trade_data.get('trade_id'),
                'is_buyer_maker': bool(trade_data.get('is_buyer_maker', False)),
                'volume_usd': price * quantity,
                'processed_at': datetime.now(timezone.utc)
            }
            transformed['price_rounded'] = round(transformed['price'], 2)
            return transformed
        except Exception:
            logger.exception("Error transforming trade data")
            return None

    #
    # Storage
    #
    def _store_raw_trade(self, transformed: Dict) -> bool:
        """Store raw trade data in PostgreSQL. Returns True on success."""
        conn = None
        try:
            conn = self._get_db_conn()
            cur = conn.cursor()
            insert_query = """
                INSERT INTO raw_trades 
                (symbol, price, quantity, trade_time, trade_id, is_buyer_maker, raw_data, processed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(insert_query, (
                transformed['symbol'],
                transformed['price'],
                transformed['quantity'],
                transformed['trade_time'].isoformat(),
                transformed.get('trade_id'),
                transformed.get('is_buyer_maker', False),
                json.dumps({
                    'symbol': transformed['symbol'],
                    'price': transformed['price'],
                    'quantity': transformed['quantity'],
                    'trade_id': transformed.get('trade_id')
                }),
                transformed['processed_at'].isoformat()
            ))
            cur.close()
            return True
        except Exception:
            logger.exception("Error storing raw trade for %s", transformed.get('symbol'))
            return False
        finally:
            if conn:
                self._put_db_conn(conn)

    def _store_processed_data(self, aggregation: Dict) -> bool:
        """Store processed aggregation data in PostgreSQL. Returns True on success."""
        conn = None
        try:
            conn = self._get_db_conn()
            cur = conn.cursor()
            insert_query = """
                INSERT INTO processed_trades 
                (symbol, avg_price, total_volume, trade_count, min_price, max_price, window_start, window_end, vwap, total_volume_usd)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(insert_query, (
                aggregation['symbol'],
                aggregation['avg_price'],
                aggregation['total_volume'],
                aggregation['trade_count'],
                aggregation['min_price'],
                aggregation['max_price'],
                aggregation['window_start'],
                aggregation['window_end'],
                aggregation.get('vwap'),
                aggregation.get('total_volume_usd')
            ))
            cur.close()
            return True
        except Exception:
            logger.exception("Error storing processed aggregation for %s", aggregation.get('symbol'))
            return False
        finally:
            if conn:
                self._put_db_conn(conn)

    def _update_market_summary(self, transformed: Dict) -> bool:
        """
        Update market summary table with latest trade data.
        NOTE: This function currently increments volume_24h cumulatively; for a rolling 24h
        you'd typically recompute separately or maintain a time-series.
        """
        conn = None
        try:
            conn = self._get_db_conn()
            cur = conn.cursor()
            upsert_query = """
                INSERT INTO market_summary (symbol, current_price, volume_24h, last_updated)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (symbol)
                DO UPDATE SET
                    current_price = EXCLUDED.current_price,
                    volume_24h = COALESCE(market_summary.volume_24h, 0) + EXCLUDED.volume_24h,
                    last_updated = EXCLUDED.last_updated
            """
            cur.execute(upsert_query, (
                transformed['symbol'],
                transformed['price'],
                transformed['quantity'],
                transformed['trade_time'].isoformat()
            ))
            cur.close()
            return True
        except Exception:
            logger.exception("Error updating market summary for %s", transformed.get('symbol'))
            return False
        finally:
            if conn:
                self._put_db_conn(conn)

    #
    # Windowing / aggregation
    #
    def _add_to_window(self, transformed: Dict):
        """Add trade data to aggregation window in a thread-safe manner"""
        with self.trade_windows_lock:
            symbol = transformed['symbol']
            self.trade_windows[symbol].append({
                'price': transformed['price'],
                'quantity': transformed['quantity'],
                'volume_usd': transformed['volume_usd'],
                'trade_time': transformed['trade_time'],
                'is_buyer_maker': transformed['is_buyer_maker']
            })

            # Retain trades up to window_size + buffer minutes
            retention_cutoff = datetime.now(timezone.utc) - timedelta(
                minutes=self.window_size_minutes + self.window_retention_buffer_minutes
            )
            self.trade_windows[symbol] = [
                t for t in self.trade_windows[symbol]
                if t['trade_time'] > retention_cutoff
            ]

    def _calculate_window_aggregations(self, symbol: str, window_start: datetime, window_end: datetime) -> Optional[Dict]:
        """Calculate aggregations for a time window (expects timezone-aware datetimes)"""
        try:
            with self.trade_windows_lock:
                trades = list(self.trade_windows.get(symbol, []))

            window_trades = [
                trade for trade in trades
                if window_start <= trade['trade_time'] < window_end
            ]

            if not window_trades:
                return None

            prices = [trade['price'] for trade in window_trades]
            volumes = [trade['quantity'] for trade in window_trades]
            volume_usd = [trade['volume_usd'] for trade in window_trades]

            total_volume = sum(volumes)
            aggregation = {
                'symbol': symbol,
                'avg_price': sum(prices) / len(prices),
                'total_volume': total_volume,
                'total_volume_usd': sum(volume_usd),
                'trade_count': len(window_trades),
                'min_price': min(prices),
                'max_price': max(prices),
                'window_start': window_start.isoformat(),
                'window_end': window_end.isoformat(),
                'vwap': (sum(p * v for p, v in zip(prices, volumes)) / total_volume) if total_volume > 0 else 0,
                'buy_volume': sum(trade['quantity'] for trade in window_trades if not trade['is_buyer_maker']),
                'sell_volume': sum(trade['quantity'] for trade in window_trades if trade['is_buyer_maker'])
            }
            return aggregation
        except Exception:
            logger.exception("Error calculating window aggregations for %s", symbol)
            return None

    def _send_to_processed_topic(self, aggregation: Dict):
        """Send processed data to Kafka processed topic (non-blocking)"""
        if not self.producer:
            logger.error("Producer not initialized, cannot send processed data")
            return

        try:
            future = self.producer.send(
                topic=self.config['kafka']['processed_topic'],
                key=aggregation['symbol'],
                value=aggregation
            )

            # Use callbacks to observe success/failure but do not block processing
            def on_success(metadata):
                logger.debug("Processed message sent to %s:%s offset=%s", metadata.topic, metadata.partition, metadata.offset)

            def on_error(exc):
                logger.error("Failed to send processed message for %s: %s", aggregation.get('symbol'), exc)

            future.add_callback(on_success)
            future.add_errback(on_error)

        except Exception:
            logger.exception("Exception while sending to processed topic for %s", aggregation.get('symbol'))

    #
    # Background aggregation thread
    #
    def _process_window_aggregations(self):
        """Background thread that aggregates trades into windows aligned on minute boundaries"""
        logger.info("Window aggregation thread started")
        while self.running:
            try:
                now = datetime.now(timezone.utc)
                # Align window_end to the previous minute boundary (exclusive)
                window_end = now.replace(second=0, microsecond=0)
                window_start = window_end - timedelta(minutes=self.window_size_minutes)

                # For every symbol present, compute aggregation for the last complete window
                symbols = []
                with self.trade_windows_lock:
                    symbols = list(self.trade_windows.keys())

                for symbol in symbols:
                    aggregation = self._calculate_window_aggregations(symbol, window_start, window_end)
                    if aggregation:
                        stored = self._store_processed_data(aggregation)
                        if stored:
                            self._send_to_processed_topic(aggregation)
                            logger.debug("Processed window for %s: %s trades", symbol, aggregation['trade_count'])

                # Sleep until the next minute boundary (align precisely)
                next_boundary = window_end + timedelta(minutes=1)
                sleep_seconds = (next_boundary - datetime.now(timezone.utc)).total_seconds()
                if sleep_seconds > 0:
                    # add a small jitter to avoid thundering herd in multi-instance setups
                    jitter = random.uniform(0, 0.5)
                    time.sleep(sleep_seconds + jitter)
            except Exception:
                logger.exception("Error in window aggregation thread")
                time.sleep(5)
        logger.info("Window aggregation thread exiting")

    #
    # Main run loop
    #
    def run(self):
        logger.info("Starting stream processor...")

        # Initialize subsystems
        self._initialize_database()
        self._initialize_kafka_producer()
        self._initialize_kafka_consumer()

        # Start aggregation thread
        aggregation_thread = Thread(target=self._process_window_aggregations, daemon=True)
        aggregation_thread.start()

        logger.info("Stream processor started successfully")
        try:
            while self.running:
                try:
                    # poll returns dict: {TopicPartition: [messages]}
                    message_batch = self.consumer.poll(timeout_ms=1000, max_records=500)
                    if not message_batch:
                        continue

                    for tp, messages in message_batch.items():
                        for message in messages:
                            try:
                                value = message.value
                                # Validate
                                if not self._validate_trade_data(value):
                                    continue

                                transformed = self._transform_trade_data(value)
                                if not transformed:
                                    continue

                                # Store raw trade (persist); only commit offsets after successful persistence
                                stored = self._store_raw_trade(transformed)
                                if not stored:
                                    # if we couldn't store, do not commit offset so message can be retried
                                    logger.error("Failed to persist trade for %s, skipping commit", transformed.get('symbol'))
                                    continue

                                # Add to in-memory window
                                self._add_to_window(transformed)

                                # Update market summary (best-effort). If it fails we still continue, but do not drop message.
                                updated = self._update_market_summary(transformed)
                                if not updated:
                                    logger.warning("Market summary update failed for %s", transformed.get('symbol'))

                                logger.debug("Processed trade: %s @ %s", transformed['symbol'], transformed['price'])

                            except Exception:
                                logger.exception("Error processing individual message")

                    # Commit offsets after batch processed successfully
                    try:
                        self.consumer.commit()
                    except Exception:
                        logger.exception("Failed to commit consumer offsets")

                except Exception:
                    logger.exception("Error in main processing loop")
                    time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
        finally:
            # shutdown
            logger.info("Shutting down stream processor")
            self.running = False

            # close consumer
            try:
                if self.consumer:
                    self.consumer.close()
            except Exception:
                logger.exception("Exception closing consumer")

            # flush and close producer (with timeout)
            try:
                if self.producer:
                    try:
                        self.producer.flush(timeout=10)
                    except Exception:
                        logger.exception("Producer flush failed")
                    try:
                        self.producer.close()
                    except Exception:
                        logger.exception("Producer close failed")
            except Exception:
                logger.exception("Exception shutting down producer")

            # close DB pool
            try:
                if self.db_pool:
                    self.db_pool.closeall()
            except Exception:
                logger.exception("Exception closing DB pool")

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
        'window_size_minutes': int(os.getenv('WINDOW_SIZE_MINUTES', 1)),
        'consumer_group_id': os.getenv('KAFKA_CONSUMER_GROUP', 'stream-processor')
    }


def main():
    """Main entry point — set signal handlers here in the main thread"""
    import signal

    config = load_config()
    processor = StreamProcessor(config)

    def _handle_signal(signum, frame):
        logger.info("Received signal %s, requesting processor shutdown", signum)
        processor.running = False

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        processor.run()
    except Exception:
        logger.exception("Stream processor failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
