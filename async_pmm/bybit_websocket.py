"""
Bybit WebSocket Client - Asynchronous implementation using websockets
"""
import asyncio
import json
import logging
import time
import hmac
import hashlib
import websockets
import uvloop
import inspect
from typing import Dict, List, Any, Callable, Optional, Set
from urllib.parse import urlencode

# Set uvloop as the event loop policy
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

class BybitWebSocketClient:
    """
    Asynchronous WebSocket client for Bybit exchange
    """
    
    # WebSocket endpoints
    PUBLIC_WS_URL = "wss://stream.bybit.com/v5/public"  # Public data is identical to mainnet
    PRIVATE_WS_URL = "wss://stream.bybit.com/v5/private"  # Private endpoint
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None, 
                 testnet: bool = True, ping_interval: int = 20, category: str = "linear"):
        """
        Initialize the WebSocket client
        
        Args:
            api_key: API key for authenticated endpoints
            api_secret: API secret for authenticated endpoints
            testnet: Whether to use testnet
            ping_interval: Ping interval in seconds
        """
        # Authentication
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.category = category

        # WebSocket configuration
        self.ping_interval = ping_interval
        self.ping_timeout = 10
        self.reconnect_delay = 5
        self.max_reconnect_attempts = 10
        
        # WebSocket connections
        self.public_ws = None
        self.private_ws = None
        self.public_connected = False
        self.private_connected = False
        self.reconnect_count = 0
        
        # Subscription management
        self.public_subscriptions: Set[str] = set()
        self.private_subscriptions: Set[str] = set()
        
        # Data storage
        self.tickers: Dict[str, Dict[str, Any]] = {}
        self.klines: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        self.orderbooks: Dict[str, Dict[str, Any]] = {}
        self.positions: Dict[str, Dict[str, Any]] = {}
        self.orders: Dict[str, Dict[str, Any]] = {}
        self.executions: List[Dict[str, Any]] = []
        
        # Callbacks
        self.ticker_callbacks: Dict[str, List[Callable]] = {}
        self.kline_callbacks: Dict[str, Dict[str, List[Callable]]] = {}
        self.orderbook_callbacks: Dict[str, List[Callable]] = {}
        self.position_callbacks: List[Callable] = []
        self.order_callbacks: List[Callable] = []
        self.execution_callbacks: List[Callable] = []
        self.connection_callbacks: List[Callable] = []
        
        # Message queue for processing
        self.message_queue = asyncio.Queue()
        
        # Setup logging
        self.logger = logging.getLogger("bybit_websocket")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        # Prevent logs from being passed to the root logger
        self.logger.propagate = False
        
        # Adjust URLs for testnet
        if self.testnet:
            self.PUBLIC_WS_URL = "wss://stream.bybit.com/v5/public"
            self.PRIVATE_WS_URL = "wss://stream-demo.bybit.com/v5/private"

    def _generate_signature(self, expires: int) -> str:
        """
        Generate signature for WebSocket authentication
        
        Args:
            expires: Expiration timestamp in milliseconds
            
        Returns:
            HMAC signature
        """
        val = f"GET/realtime{expires}"
        return hmac.new(
            self.api_secret.encode(),
            val.encode(),
            hashlib.sha256
        ).hexdigest()
    
    async def connect(self):
        """Connect to public and private WebSocket endpoints"""
        # Start the message processor
        asyncio.create_task(self._process_messages())

        # Connect to private WebSocket if credentials provided
        if self.api_key and self.api_secret:
            await self._connect_private()

        # Connect to public WebSocket
        await self._connect_public()
    
    async def _connect_public(self):
        """Connect to public WebSocket endpoint"""
        try:
            self.logger.info("Connecting to Bybit public WebSocket...")
            self.public_ws = await websockets.connect(
                f'{self.PUBLIC_WS_URL}/{self.category}',
                ping_interval=self.ping_interval,
                ping_timeout=self.ping_timeout
            )
            self.public_connected = True
            self.reconnect_count = 0
            self.logger.info("Connected to Bybit public WebSocket")
            
            # Resubscribe to previous topics
            for topic in self.public_subscriptions:
                await self._send_public_subscription(topic)
            
            # Start message receiver
            asyncio.create_task(self._receive_public_messages())
            
            # Notify connection callbacks
            for callback in self.connection_callbacks:
                try:
                    if inspect.iscoroutinefunction(callback):
                        await callback(True)
                    else:
                        callback(True)
                except Exception as e:
                    self.logger.error(f"Error in connection callback: {e}")
            
        except Exception as e:
            self.public_connected = False
            self.logger.error(f"Error connecting to public WebSocket: {e}")
            await self._handle_reconnect("public")
    
    async def _connect_private(self):
        """Connect to private WebSocket endpoint"""
        if not self.api_key or not self.api_secret:
            self.logger.warning("Cannot connect to private WebSocket: API credentials not provided")
            return
            
        try:
            self.logger.info("Connecting to Bybit private WebSocket...")
            
            # Generate authentication parameters
            expires = int((time.time() + 10) * 1000)
            signature = self._generate_signature(expires)
            
            # Build connection URL with auth params
            auth_params = {
                "api_key": self.api_key,
                "expires": str(expires),
                "signature": signature
            }
            auth_query = urlencode(auth_params)
            private_url = f"{self.PRIVATE_WS_URL}?{auth_query}"
            
            self.private_ws = await websockets.connect(
                private_url,
                ping_interval=self.ping_interval,
                ping_timeout=self.ping_timeout
            )
            self.private_connected = True
            self.reconnect_count = 0
            self.logger.info("Connected to Bybit private WebSocket")
            
            # Resubscribe to previous topics
            for topic in self.private_subscriptions:
                await self._send_private_subscription(topic)
            
            # Start message receiver
            asyncio.create_task(self._receive_private_messages())
            
        except Exception as e:
            self.private_connected = False
            self.logger.error(f"Error connecting to private WebSocket: {e}")
            await self._handle_reconnect("private")
    
    async def _handle_reconnect(self, ws_type: str):
        """
        Handle reconnection logic
        
        Args:
            ws_type: WebSocket type ('public' or 'private')
        """
        if self.reconnect_count >= self.max_reconnect_attempts:
            self.logger.error(f"Max reconnection attempts reached for {ws_type} WebSocket")
            return
            
        self.reconnect_count += 1
        delay = self.reconnect_delay * (2 ** min(self.reconnect_count - 1, 5))  # Exponential backoff
        self.logger.info(f"Reconnecting to {ws_type} WebSocket in {delay} seconds (attempt {self.reconnect_count})")
        
        await asyncio.sleep(delay)
        
        if ws_type == "public":
            await self._connect_public()
        else:
            await self._connect_private()
    
    async def _receive_public_messages(self):
        """Receive and process messages from public WebSocket"""
        try:
            while self.public_connected and self.public_ws:
                try:
                    message = await self.public_ws.recv()
                    # Add to processing queue
                    await self.message_queue.put(("public", message))
                except websockets.exceptions.ConnectionClosed:
                    self.public_connected = False
                    self.logger.warning("Public WebSocket connection closed")
                    await self._handle_reconnect("public")
                    break
                except Exception as e:
                    self.logger.error(f"Error receiving public WebSocket message: {e}")
        except Exception as e:
            self.logger.error(f"Error in public message receiver: {e}")
            self.public_connected = False
            await self._handle_reconnect("public")
    
    async def _receive_private_messages(self):
        """Receive and process messages from private WebSocket"""
        try:
            while self.private_connected and self.private_ws:
                try:
                    message = await self.private_ws.recv()
                    # Add to processing queue
                    await self.message_queue.put(("private", message))
                except websockets.exceptions.ConnectionClosed:
                    self.private_connected = False
                    self.logger.warning("Private WebSocket connection closed")
                    await self._handle_reconnect("private")
                    break
                except Exception as e:
                    self.logger.error(f"Error receiving private WebSocket message: {e}")
        except Exception as e:
            self.logger.error(f"Error in private message receiver: {e}")
            self.private_connected = False
            await self._handle_reconnect("private")
    
    async def _process_messages(self):
        """Process messages from the queue"""
        while True:
            try:
                ws_type, message = await self.message_queue.get()
                
                # Parse JSON message
                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    self.logger.error(f"Invalid JSON message: {message}")
                    continue
                
                # Handle ping/pong for older API versions
                if "op" in data and data["op"] == "ping":
                    await self._handle_ping(ws_type, data)
                    continue
                
                # Process the message based on type
                if ws_type == "public":
                    await self._handle_public_message(data)
                else:
                    await self._handle_private_message(data)
                
            except Exception as e:
                self.logger.error(f"Error processing message: {e}")
            finally:
                self.message_queue.task_done()
    
    async def _handle_ping(self, ws_type: str, data: Dict[str, Any]):
        """
        Handle ping messages by responding with pong
        
        Args:
            ws_type: WebSocket type ('public' or 'private')
            data: Ping message data
        """
        try:
            pong_msg = json.dumps({"op": "pong", "args": [data.get("ts", int(time.time() * 1000))]})
            if ws_type == "public" and self.public_ws:
                await self.public_ws.send(pong_msg)
            elif ws_type == "private" and self.private_ws:
                await self.private_ws.send(pong_msg)
        except Exception as e:
            self.logger.error(f"Error sending pong: {e}")
    
    async def _handle_public_message(self, data: Dict[str, Any]):
        """
        Process public WebSocket messages
        
        Args:
            data: Message data
        """
        try:
            # Check for success/error response
            if "success" in data:
                if not data["success"]:
                    self.logger.error(f"WebSocket error: {data.get('ret_msg', 'Unknown error')}")
                return
                
            # Extract topic and data
            topic = data.get("topic", "")
            if not topic:
                return
                
            # Process based on topic type
            if topic.startswith("tickers."):
                await self._handle_ticker(topic, data)
            elif topic.startswith("kline."):
                await self._handle_kline(topic, data)
            elif topic.startswith("orderbook."):
                await self._handle_orderbook(topic, data)
            
        except Exception as e:
            self.logger.error(f"Error handling public message: {e}")
    
    async def _handle_private_message(self, data: Dict[str, Any]):
        """
        Process private WebSocket messages
        
        Args:
            data: Message data
        """
        try:
            # Check for success/error response
            if "success" in data:
                if not data["success"]:
                    self.logger.error(f"WebSocket error: {data.get('ret_msg', 'Unknown error')}")
                return
                
            # Extract topic and data
            topic = data.get("topic", "")
            if not topic:
                return
                
            # Process based on topic type
            if topic == "position":
                await self._handle_position(data)
            elif topic == "order":
                await self._handle_order(data)
            elif topic == "execution":
                await self._handle_execution(data)
            
        except Exception as e:
            self.logger.error(f"Error handling private message: {e}")
    
    async def _handle_ticker(self, topic: str, data: Dict[str, Any]):
        """
        Process ticker updates
        
        Args:
            topic: Message topic
            data: Message data
        """
        try:
            # Extract symbol from topic (format: "tickers.{symbol}")
            parts = topic.split(".")
            if len(parts) < 2:
                return
                
            symbol = parts[1]
            ticker_data = data.get("data", {})
            
            # Update ticker data
            self.tickers[symbol] = ticker_data
            
            # Call registered callbacks
            if symbol in self.ticker_callbacks:
                for callback in self.ticker_callbacks[symbol]:
                    try:
                        await callback(ticker_data)
                    except Exception as e:
                        self.logger.error(f"Error in ticker callback: {e}")
            
        except Exception as e:
            self.logger.error(f"Error handling ticker update: {e}")
    
    async def _handle_kline(self, topic: str, data: Dict[str, Any]):
        """
        Process kline/candle updates
        
        Args:
            topic: Message topic
            data: Message data
        """
        try:
            # Extract symbol and interval from topic (format: "kline.{interval}.{symbol}")
            parts = topic.split(".")
            if len(parts) < 3:
                return
                
            interval = parts[1]
            symbol = parts[2]
            kline_data = data.get("data", [])
            
            # Initialize if needed
            if symbol not in self.klines:
                self.klines[symbol] = {}
            if interval not in self.klines[symbol]:
                self.klines[symbol][interval] = []
                
            # Update kline data (append new candles)
            for candle in kline_data:
                # Check if candle already exists by timestamp
                timestamp = candle.get("timestamp", 0)
                existing_idx = next((i for i, k in enumerate(self.klines[symbol][interval]) 
                                   if k.get("timestamp") == timestamp), None)
                
                if existing_idx is not None:
                    # Update existing candle
                    self.klines[symbol][interval][existing_idx] = candle
                else:
                    # Add new candle
                    self.klines[symbol][interval].append(candle)
                    
            # Sort by timestamp and limit to 500 candles
            self.klines[symbol][interval] = sorted(
                self.klines[symbol][interval], 
                key=lambda x: x.get("timestamp", 0)
            )[-500:]
            
            # Call registered callbacks
            if symbol in self.kline_callbacks and interval in self.kline_callbacks[symbol]:
                for callback in self.kline_callbacks[symbol][interval]:
                    try:
                        await callback(self.klines[symbol][interval])
                    except Exception as e:
                        self.logger.error(f"Error in kline callback: {e}")
            
        except Exception as e:
            self.logger.error(f"Error handling kline update: {e}")
    
    async def _handle_orderbook(self, topic: str, data: Dict[str, Any]):
        """
        Process orderbook updates
        
        Args:
            topic: Message topic
            data: Message data
        """
        try:
            # Extract symbol from topic (format: "orderbook.{depth}.{symbol}")
            parts = topic.split(".")
            if len(parts) < 3:
                return
                
            symbol = parts[2]
            orderbook_data = data.get("data", {})
            
            # Update orderbook data
            self.orderbooks[symbol] = orderbook_data
            
            # Call registered callbacks
            if symbol in self.orderbook_callbacks:
                for callback in self.orderbook_callbacks[symbol]:
                    try:
                        await callback(orderbook_data)
                    except Exception as e:
                        self.logger.error(f"Error in orderbook callback: {e}")
            
        except Exception as e:
            self.logger.error(f"Error handling orderbook update: {e}")
    
    async def _handle_position(self, data: Dict[str, Any]):
        """
        Process position updates
        
        Args:
            data: Message data
        """
        try:
            position_data = data.get("data", [])
            
            # Update positions
            for position in position_data:
                symbol = position.get("symbol", "")
                if symbol:
                    self.positions[symbol] = position
            
            # Call registered callbacks
            for callback in self.position_callbacks:
                try:
                    await callback(position_data)
                except Exception as e:
                    self.logger.error(f"Error in position callback: {e}")
            
        except Exception as e:
            self.logger.error(f"Error handling position update: {e}")
    
    async def _handle_order(self, data: Dict[str, Any]):
        """
        Process order updates
        
        Args:
            data: Message data
        """
        try:
            order_data = data.get("data", [])
            
            # Update orders
            for order in order_data:
                order_id = order.get("orderId", "")
                if order_id:
                    self.orders[order_id] = order
            
            # Call registered callbacks
            for callback in self.order_callbacks:
                try:
                    await callback(order_data)
                except Exception as e:
                    self.logger.error(f"Error in order callback: {e}")
            
        except Exception as e:
            self.logger.error(f"Error handling order update: {e}")
    
    async def _handle_execution(self, data: Dict[str, Any]):
        """
        Process execution updates
        
        Args:
            data: Message data
        """
        try:
            execution_data = data.get("data", [])
            
            # Add to executions list
            for execution in execution_data:
                self.executions.append(execution)
            
            # Limit to last 100 executions
            self.executions = self.executions[-100:]
            
            # Call registered callbacks
            for callback in self.execution_callbacks:
                try:
                    await callback(execution_data)
                except Exception as e:
                    self.logger.error(f"Error in execution callback: {e}")
            
        except Exception as e:
            self.logger.error(f"Error handling execution update: {e}")
    
    async def _send_public_subscription(self, topic: str):
        """
        Send subscription request to public WebSocket
        
        Args:
            topic: Topic to subscribe to
        """
        if not self.public_connected or not self.public_ws:
            self.logger.warning(f"Cannot subscribe to {topic}: Public WebSocket not connected")
            return
            
        try:
            sub_message = json.dumps({
                "op": "subscribe",
                "args": [topic]
            })
            await self.public_ws.send(sub_message)
            self.logger.info(f"Subscribed to {topic}")
        except Exception as e:
            self.logger.error(f"Error subscribing to {topic}: {e}")
    
    async def _send_private_subscription(self, topic: str):
        """
        Send subscription request to private WebSocket
        
        Args:
            topic: Topic to subscribe to
        """
        if not self.private_connected or not self.private_ws:
            self.logger.warning(f"Cannot subscribe to {topic}: Private WebSocket not connected")
            return
            
        try:
            sub_message = json.dumps({
                "op": "subscribe",
                "args": [topic]
            })
            await self.private_ws.send(sub_message)
            self.logger.info(f"Subscribed to {topic}")
        except Exception as e:
            self.logger.error(f"Error subscribing to {topic}: {e}")
    
    async def subscribe_ticker(self, symbol: str):
        """
        Subscribe to ticker updates for a symbol
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
        """
        topic = f"tickers.{symbol}"
        self.public_subscriptions.add(topic)
        await self._send_public_subscription(topic)
    
    async def subscribe_kline(self, symbol: str, interval: str = "1"):
        """
        Subscribe to kline/candle updates for a symbol
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            interval: Kline interval (e.g., '1', '5', '15', '30', '60', '240', 'D', 'W', 'M')
        """
        topic = f"kline.{interval}.{symbol}"
        self.public_subscriptions.add(topic)
        await self._send_public_subscription(topic)
    
    async def subscribe_orderbook(self, symbol: str, depth: str = "50"):
        """
        Subscribe to orderbook updates for a symbol
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            depth: Orderbook depth (e.g., '1', '50', '200', '500')
        """
        topic = f"orderbook.{depth}.{symbol}"
        self.public_subscriptions.add(topic)
        await self._send_public_subscription(topic)
    
    async def subscribe_positions(self):
        """Subscribe to position updates"""
        if not self.api_key or not self.api_secret:
            self.logger.warning("Cannot subscribe to positions: API credentials not provided")
            return
            
        topic = "position"
        self.private_subscriptions.add(topic)
        await self._send_private_subscription(topic)
    
    async def subscribe_orders(self):
        """Subscribe to order updates"""
        if not self.api_key or not self.api_secret:
            self.logger.warning("Cannot subscribe to orders: API credentials not provided")
            return
            
        topic = "order"
        self.private_subscriptions.add(topic)
        await self._send_private_subscription(topic)
    
    async def subscribe_executions(self):
        """Subscribe to execution updates"""
        if not self.api_key or not self.api_secret:
            self.logger.warning("Cannot subscribe to executions: API credentials not provided")
            return
            
        topic = "execution"
        self.private_subscriptions.add(topic)
        await self._send_private_subscription(topic)
    
    def register_ticker_callback(self, symbol: str, callback: Callable[[Dict[str, Any]], Any]):
        """
        Register callback for ticker updates
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            callback: Async callback function
        """
        if symbol not in self.ticker_callbacks:
            self.ticker_callbacks[symbol] = []
        self.ticker_callbacks[symbol].append(callback)
    
    def register_kline_callback(self, symbol: str, interval: str, callback: Callable[[List[Dict[str, Any]]], Any]):
        """
        Register callback for kline updates
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            interval: Kline interval
            callback: Async callback function
        """
        if symbol not in self.kline_callbacks:
            self.kline_callbacks[symbol] = {}
        if interval not in self.kline_callbacks[symbol]:
            self.kline_callbacks[symbol][interval] = []
        self.kline_callbacks[symbol][interval].append(callback)
    
    def register_orderbook_callback(self, symbol: str, callback: Callable[[Dict[str, Any]], Any]):
        """
        Register callback for orderbook updates
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            callback: Async callback function
        """
        if symbol not in self.orderbook_callbacks:
            self.orderbook_callbacks[symbol] = []
        self.orderbook_callbacks[symbol].append(callback)
    
    def register_position_callback(self, callback: Callable[[Dict[str, Any]], Any]):
        """
        Register callback for position updates
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            callback: Async callback function
        """
        self.position_callbacks.append(callback)
    
    def register_order_callback(self, callback: Callable[[List[Dict[str, Any]]], Any]):
        """
        Register callback for order updates
        
        Args:
            callback: Async callback function
        """
        self.order_callbacks.append(callback)
    
    def register_execution_callback(self, callback: Callable[[List[Dict[str, Any]]], Any]):
        """
        Register callback for execution updates
        
        Args:
            callback: Async callback function
        """
        self.execution_callbacks.append(callback)
    
    def register_connection_callback(self, callback: Callable[[bool], Any]):
        """
        Register callback for connection status updates
        
        Args:
            callback: Callback function that takes a boolean indicating connection status
        """
        self.connection_callbacks.append(callback)
    
    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Get latest ticker data for a symbol
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            
        Returns:
            Latest ticker data
        """
        return self.tickers.get(symbol, {})
    
    def get_klines(self, symbol: str, interval: str = "1") -> List[Dict[str, Any]]:
        """
        Get kline/candle data for a symbol
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            interval: Kline interval
            
        Returns:
            List of kline data
        """
        if symbol not in self.klines or interval not in self.klines[symbol]:
            return []
        return self.klines[symbol][interval]
    
    def get_orderbook(self, symbol: str) -> Dict[str, Any]:
        """
        Get orderbook data for a symbol
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            
        Returns:
            Orderbook data
        """
        return self.orderbooks.get(symbol, {})
    
    def get_position(self, symbol: str) -> Dict[str, Any]:
        """
        Get position data for a symbol
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            
        Returns:
            Position data
        """
        return self.positions.get(symbol, {})
    
    def get_orders(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all active orders
        
        Returns:
            Dictionary of orders by order ID
        """
        return self.orders
    
    def get_executions(self) -> List[Dict[str, Any]]:
        """
        Get recent executions
        
        Returns:
            List of execution data
        """
        return self.executions
    
    async def close(self):
        """Close WebSocket connections"""
        try:
            if self.public_ws:
                await self.public_ws.close()
                self.public_connected = False
                
            if self.private_ws:
                await self.private_ws.close()
                self.private_connected = False
                
            self.logger.info("WebSocket connections closed")
            
        except Exception as e:
            self.logger.error(f"Error closing WebSocket connections: {e}")