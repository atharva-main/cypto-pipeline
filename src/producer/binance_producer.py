# src/producer/binance_producer.py
import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime
from typing import Dict, List, Optional

import websockets # type: ignore
from kafka import KafkaProducer # type: ignore
from kafka.errors import KafkaError # type: ignore

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/producer.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class BinanceWebSocketProducer:
    """
    Binance WebSocket producer that streams live trade data to Kafka
    """
    
    def __init__(self, 
                 kafka_servers: str = 'localhost:9092',
                 kafka_topic: str = 'raw_data',
                 symbols: List[str] = None):
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.symbols = symbols or ['btcusdt', 'ethusdt', 'adausdt', 'bnbusdt', 'solusdt']
        self.producer = None
        self.websocket = None
        self.running = True
        
        # Binance WebSocket URL
        self.ws_url = "wss://stream.binance.com:9443/ws/"
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down...")
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
            logger.error(f"Failed to initialize Kafka producer: {e}")
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
            if 'data' not in message:
                return None
            
            data = message['data']
            
            # Extract trade information
            trade_data = {
                'symbol': data['s'],
                'price': float(data['p']),
                'quantity': float(data['q']),
                'trade_time': datetime.fromtimestamp(data['T'] / 1000).isoformat(),
                'trade_id': data['t'],
                'is_buyer_maker': data['m'],
                'event_time': datetime.fromtimestamp(data['E'] / 1000).isoformat(),
                'raw_timestamp': data['T'],
                'source': 'binance',
                'processed_at': datetime.utcnow().isoformat()
            }
            
            return trade_data
            
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Error processing trade message: {e}, message: {message}")
            return None
    
    async def _send_to_kafka(self, trade_data: Dict):
        """Send processed trade data to Kafka"""
        try:
            future = self.producer.send(
                topic=self.kafka_topic,
                key=trade_data['symbol'],
                value=trade_data
            )
            
            # Non-blocking send with callback
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
            
        except KafkaError as e:
            logger.error(f"Failed to send message to Kafka: {e}")
    
    def _on_send_success(self, record_metadata):
        """Callback for successful Kafka send"""
        logger.debug(f"Message sent successfully to {record_metadata.topic}:"
                    f"{record_metadata.partition}:{record_metadata.offset}")
    
    def _on_send_error(self, exception):
        """Callback for failed Kafka send"""
        logger.error(f"Failed to send message to Kafka: {exception}")
    
    async def _connect_websocket(self):
        """Connect to Binance WebSocket and handle messages"""
        try:
            logger.info(f"Connecting to Binance WebSocket: {self.ws_url}")
            
            async with websockets.connect(self.ws_url) as websocket:
                self.websocket = websocket
                
                # Subscribe to trade streams
                subscription = self._create_subscription_message()
                await websocket.send(json.dumps(subscription))
                logger.info(f"Subscribed to symbols: {self.symbols}")
                
                # Listen for messages
                async for message in websocket:
                    if not self.running:
                        break
                    
                    try:
                        data = json.loads(message)
                        
                        # Skip subscription confirmation messages
                        if 'result' in data or 'id' in data:
                            continue
                        
                        # Process trade data
                        trade_data = self._process_trade_message(data)
                        if trade_data:
                            await self._send_to_kafka(trade_data)
                            logger.debug(f"Processed trade: {trade_data['symbol']} @ {trade_data['price']}")
                    
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode WebSocket message: {e}")
                    except Exception as e:
                        logger.error(f"Error processing WebSocket message: {e}")
        
        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket connection closed")
        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")
    
    async def run(self):
        """Main run loop with reconnection logic"""
        self._initialize_kafka_producer()
        
        logger.info("Starting Binance WebSocket producer...")
        
        while self.running:
            try:
                await self._connect_websocket()
                
                if self.running:
                    logger.info("Reconnecting in 5 seconds...")
                    await asyncio.sleep(5)
                    
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt, shutting down...")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                if self.running:
                    await asyncio.sleep(5)
        
        # Cleanup
        if self.producer:
            self.producer.flush()
            self.producer.close()
        
        logger.info("Producer shutdown complete")


def main():
    """Main entry point"""
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    kafka_topic = os.getenv('KAFKA_TOPIC_RAW', 'raw_data')
    
    # Parse symbols from environment variable
    symbols_env = os.getenv('BINANCE_SYMBOLS', 'btcusdt,ethusdt,adausdt,bnbusdt,solusdt')
    symbols = [s.strip().lower() for s in symbols_env.split(',')]
    
    producer = BinanceWebSocketProducer(
        kafka_servers=kafka_servers,
        kafka_topic=kafka_topic,
        symbols=symbols
    )
    
    try:
        asyncio.run(producer.run())
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Producer failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()