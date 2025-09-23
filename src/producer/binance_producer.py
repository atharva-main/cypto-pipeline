# src/producer/binance_producer.py
import asyncio
import json
import logging
import os
import sys
import random
from datetime import datetime
from typing import Dict, List, Optional

import websockets  # type: ignore
from kafka import KafkaProducer  # type: ignore
from kafka.errors import KafkaError  # type: ignore

# Configure logging
log_handlers = [logging.StreamHandler(sys.stdout)]
try:
    # Only add file handler if logs directory exists or can be created
    log_dir = '/app/logs'
    os.makedirs(log_dir, exist_ok=True)
    log_handlers.append(logging.FileHandler('/app/logs/producer.log'))
except (PermissionError, OSError):
    # Fall back to stdout only if file logging fails
    pass

logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=log_handlers
)

logger = logging.getLogger(__name__)


class BinanceWebSocketProducer:
    """
    Binance WebSocket producer that streams live trade data to Kafka.
    Signal handling is intentionally NOT done in this class so it can be used
    from non-main threads or tested more easily.
    """

    def __init__(
        self,
        kafka_servers: str = 'localhost:9092',
        kafka_topic: str = 'raw_data',
        symbols: List[str] = None,
    ):
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.symbols = symbols or ['btcusdt', 'ethusdt', 'adausdt', 'bnbusdt', 'solusdt']
        self.producer: Optional[KafkaProducer] = None
        self.websocket = None
        self.running = True

        # Binance WebSocket URL
        self.ws_url = "wss://stream.binance.com:9443/ws"

    def stop(self):
        """Signal the producer to stop and close resources."""
        logger.info("Stop requested — shutting down producer...")
        self.running = False

    def _initialize_kafka_producer(self):
        """Initialize Kafka producer with proper configuration"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                retries=3,
                batch_size=16384,
                linger_ms=10,
                buffer_memory=33554432,
                compression_type='gzip',
                acks='all'
            )
            logger.info("Kafka producer initialized successfully")
        except Exception as e:
            logger.exception(f"Failed to initialize Kafka producer: {e}")
            raise

    def _create_subscription_message(self) -> Dict:
        """Create WebSocket subscription message for multiple symbols"""
        streams = [f"{symbol}@trade" for symbol in self.symbols]
        return {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 1
        }

    def _process_trade_message(self, message: Dict) -> Optional[Dict]:
        """Process and validate trade message from Binance"""

        try:
            # Binance user data / aggregated streams may send many message types.
            # We explicitly require a trade event.
            data = message.get('data') or message
            if data.get('e') != 'trade':
                # Not a trade event — ignore
                return None

            # Defensive get with explicit conversions
            symbol = data.get('s')
            price = float(data.get('p'))
            quantity = float(data.get('q'))
            trade_time_ms = int(data.get('T'))
            event_time_ms = int(data.get('E'))
            trade_id = data.get('t')
            is_buyer_maker = data.get('m')

            trade_data = {
                'symbol': symbol,
                # Use UTC timestamps for distributed systems; include Z to mark UTC.
                'price': price,
                'quantity': quantity,
                'trade_time': datetime.utcfromtimestamp(trade_time_ms / 1000).isoformat() + 'Z',
                'trade_id': trade_id,
                'is_buyer_maker': is_buyer_maker,
                'event_time': datetime.utcfromtimestamp(event_time_ms / 1000).isoformat() + 'Z',
                'raw_timestamp': trade_time_ms,
                'source': 'binance',
                'processed_at': datetime.utcnow().isoformat() + 'Z'
            }

            return trade_data

        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Error processing trade message: {e}, message: {message}")
            return None
        except Exception:
            logger.exception("Unexpected error while processing trade message")
            return None

    async def _send_to_kafka(self, trade_data: Dict):
        """Send processed trade data to Kafka (non-blocking)."""
        if not self.producer:
            logger.error("Kafka producer is not initialized — cannot send message")
            return

        try:
            future = self.producer.send(
                topic=self.kafka_topic,
                key=trade_data['symbol'],
                value=trade_data
            )

            # Attach callbacks to handle success/error
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)

        except Exception as e:
            # In typical kafka-python usage send() does not raise for transient errors;
            # errors are delivered to the future. Still catch exceptions to be defensive.
            logger.exception(f"Unexpected exception when calling producer.send(): {e}")

    def _on_send_success(self, record_metadata):
        """Callback for successful Kafka send"""
        logger.debug(
            f"Message sent successfully to {record_metadata.topic}:"
            f"{record_metadata.partition}:{record_metadata.offset}"
        )

    def _on_send_error(self, exception):
        """Callback for failed Kafka send"""
        logger.error(f"Failed to send message to Kafka: {exception}")

    async def _connect_websocket(self):
        """Connect to Binance WebSocket and handle messages"""
        try:
            logger.info(f"Connecting to Binance WebSocket: {self.ws_url}")

            async with websockets.connect(self.ws_url, max_size=None) as websocket:
                self.websocket = websocket

                # Subscribe to trade streams
                subscription = self._create_subscription_message()
                await websocket.send(json.dumps(subscription))
                logger.info(f"Subscribed to symbols: {self.symbols}")

                # Listen for messages
                async for message in websocket:
                    if not self.running:
                        logger.info("Running flag cleared — breaking websocket read loop")
                        break

                    try:
                        data = json.loads(message)

                        # Explicitly skip subscription confirmation messages:
                        # Binance returns {"result": None, "id": 1} for subscription ack.
                        if data.get('result') is None and data.get('id') == 1:
                            continue

                        # Process trade data
                        trade_data = self._process_trade_message(data)
                        if trade_data:
                            await self._send_to_kafka(trade_data)
                            logger.debug(
                                f"Processed trade: {trade_data['symbol']} @ {trade_data['price']}"
                            )

                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode WebSocket message: {e}")
                    except Exception:
                        logger.exception("Error processing WebSocket message")

        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed: {e}")
        except Exception:
            logger.exception("WebSocket connection error")
        finally:
            # ensure websocket closed reference is cleared
            try:
                if self.websocket and not self.websocket.closed:
                    await self.websocket.close()
            except Exception:
                logger.exception("Error closing websocket in finally block")
            self.websocket = None

    async def run(self):
        """Main run loop with reconnection logic and exponential backoff"""
        self._initialize_kafka_producer()

        logger.info("Starting Binance WebSocket producer...")

        # Exponential backoff parameters
        backoff_seconds = 1.0
        max_backoff_seconds = 60.0

        while self.running:
            try:
                await self._connect_websocket()

                if not self.running:
                    break

                # If the connection loop exits normally (e.g., server closed), wait and retry
                jitter = random.uniform(0, 1)
                sleep_for = min(backoff_seconds + jitter, max_backoff_seconds)
                logger.info(f"Reconnecting in {sleep_for:.1f} seconds...")
                await asyncio.sleep(sleep_for)

                # increase backoff
                backoff_seconds = min(backoff_seconds * 2, max_backoff_seconds)

            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt, shutting down...")
                break
            except Exception:
                logger.exception("Unexpected error in run loop")
                # short sleep before retrying to avoid tight loop on unexpected exception
                await asyncio.sleep(5)

        # Cleanup
        if self.producer:
            try:
                # flush with timeout to avoid indefinitely blocking shutdown
                self.producer.flush(timeout=10)
            except Exception:
                logger.exception("Exception while flushing producer")
            try:
                self.producer.close()
            except Exception:
                logger.exception("Exception while closing producer")

        logger.info("Producer shutdown complete")


def main():
    """Main entry point; set up signal handlers here (main thread)."""
    import signal

    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    kafka_topic = os.getenv('KAFKA_TOPIC_RAW', 'raw_data')

    # Parse symbols from environment variable
    symbols_env = os.getenv('BINANCE_SYMBOLS', 'btcusdt,ethusdt,adausdt,bnbusdt,solusdt')
    symbols = [s.strip().lower() for s in symbols_env.split(',') if s.strip()]

    producer = BinanceWebSocketProducer(
        kafka_servers=kafka_servers,
        kafka_topic=kafka_topic,
        symbols=symbols
    )

    # Signal handler that uses the producer.stop() method
    def _handle_signal(signum, frame):
        logger.info(f"Received signal {signum}, invoking producer.stop()")
        producer.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        asyncio.run(producer.run())
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception:
        logger.exception("Producer failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
