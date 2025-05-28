"""
Bybit REST API Client - Asynchronous implementation using aiohttp
"""
import aiohttp
import asyncio
import hmac
import hashlib
import json
import logging
import time
import urllib.parse
import math
from pybit.unified_trading import HTTP
from typing import Dict, List, Any, Optional, Union, Tuple

class BybitRestClient:
    """
    Asynchronous REST API client for Bybit exchange
    """
    
    # API endpoints
    BASE_URL = "https://api.bybit.com"  # Demo API endpoint
    DEMO_URL = "https://api-demo.bybit.com"  # Demo API endpoint (same as BASE_URL)
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None, 
                 testnet: bool = True, timeout: int = 10):
        """
        Initialize the REST API client
        
        Args:
            api_key: API key for authenticated endpoints
            api_secret: API secret for authenticated endpoints
            testnet: Whether to use testnet
            timeout: Request timeout in seconds
        """
        # Authentication
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet

        # HTTP configuration
        self.timeout = timeout
        self.session = None
        self.base_url = self.DEMO_URL if testnet else self.BASE_URL
        
        # Rate limiting
        self.request_weight = 0
        self.last_request_time = 0
        self.rate_limit_reset = 0
        self.rate_limit_remaining = 0
        
        # Setup logging
        self.logger = logging.getLogger("bybit_rest")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        # Prevent logs from being passed to the root logger
        self.logger.propagate = False

    async def _init_session(self):
        """Initialize aiohttp session if not already created"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                headers={"Content-Type": "application/json"}
            )
    
    async def close(self):
        """Close the aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None
    
    def _generate_signature(
        self,
        payload: Union[str, Dict[str, Any]],
        timestamp: int,
        recv_window: int
    ) -> str:
        """
        Generate HMAC-SHA256 signature for Bybit V5 Unified API.
        """
        if isinstance(payload, dict):
            # POST JSON body
            payload_str = json.dumps(payload, separators=(",", ":"))
        elif isinstance(payload, str):
            payload_str = payload
        else:
            raise ValueError("Unsupported payload type")

        origin_string = f"{timestamp}{self.api_key}{recv_window}{payload_str}"

        return hmac.new(
            self.api_secret.encode(),
            origin_string.encode(),
            hashlib.sha256
        ).hexdigest()
    
    async def _handle_response(self, response: aiohttp.ClientResponse) -> Dict[str, Any]:
        """
        Handle API response and extract data
        
        Args:
            response: aiohttp response object
            
        Returns:
            Response data
            
        Raises:
            Exception: If response indicates an error
        """
        # Update rate limit info from headers
        if "X-Bapi-Limit-Status" in response.headers:
            self.rate_limit_remaining = int(response.headers.get("X-Bapi-Limit-Status", "0"))
        if "X-Bapi-Limit-Reset-Timestamp" in response.headers:
            self.rate_limit_reset = int(response.headers.get("X-Bapi-Limit-Reset-Timestamp", "0"))
        
        # Parse response
        text = await response.text()
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON response: {text}")
            raise Exception(f"Invalid JSON response: {text}")
        
        # Check for errors
        if data.get("retCode") != 0:
            error_msg = f"API error: {data.get('retCode')} - {data.get('retMsg')}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
        
        return data.get("result", {})
    
    async def _request(self, method: str, endpoint: str, params: Optional[Dict[str, Any]] = None, 
                      data: Optional[Dict[str, Any]] = None, auth: bool = False) -> Dict[str, Any]:
        """
        Send request to Bybit API
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            params: URL parameters
            data: Request body for POST requests
            auth: Whether authentication is required
            
        Returns:
            Response data
        """
        await self._init_session()
        
        url = f"{self.base_url}{endpoint}"
        headers = {}
        
        # Prepare parameters
        params = params or {}
        
        # Add authentication if required
        if auth:
            if not self.api_key or not self.api_secret:
                raise Exception("API key and secret required for authenticated endpoints")

            timestamp = int(time.time() * 1000)
            recv_window = 1000

            headers["X-BAPI-API-KEY"] = self.api_key
            headers["X-BAPI-TIMESTAMP"] = str(timestamp)
            headers["X-BAPI-RECV-WINDOW"] = str(recv_window)

            if method in ("GET", "DELETE"):
                # For GET/DELETE, sign the URL-encoded query string
                payload = urllib.parse.urlencode(params or {}, doseq=True)
            else:
                # For POST, sign the JSON-serialized body
                payload = json.dumps(params or {})
                headers["Content-Type"] = "application/json"

            signature = self._generate_signature(payload, timestamp, recv_window)
            headers["X-BAPI-SIGN"] = signature

        # Apply rate limiting if needed
        if self.rate_limit_remaining == 0 and time.time() < self.rate_limit_reset / 1000:
            wait_time = (self.rate_limit_reset / 1000) - time.time() + 0.5
            self.logger.warning(f"Rate limit reached, waiting for {wait_time:.2f} seconds")
            await asyncio.sleep(wait_time)

        # Send request
        try:
            self.last_request_time = time.time()
            
            if method == "GET":
                async with self.session.get(url, params=params, headers=headers) as response:
                    return await self._handle_response(response)
            elif method == "POST":
                async with self.session.post(url, json=params, headers=headers) as response:
                    return await self._handle_response(response)

            else:
                raise Exception(f"Unsupported HTTP method: {method}")
                
        except aiohttp.ClientError as e:
            self.logger.error(f"HTTP error: {e} {e.__traceback__.tb_lineno}")
            raise Exception(f"HTTP error: {e} {e.__traceback__.tb_lineno}")
        except asyncio.TimeoutError:
            self.logger.error("Request timed out")
            raise Exception("Request timed out")
        except Exception as e:
            self.logger.error(f"Request error: {e} {e.__traceback__.tb_lineno}")
            raise Exception(f"Request error: {e} {e.__traceback__.tb_lineno}")
    
    # Market Data Endpoints

    async def get_scale(self, category, symbol='USDT'):
        """
        Get ticker scale
        
        Args:
            category: Product category (spot, linear, inverse)
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            
        Returns:
            Ticker data
        """
        params = {"category": category,
                  "symbol": symbol}
        response = await self._request("GET", "/v5/market/instruments-info", params=params)
        data = response['list'][0]
        price_scale = data.get("priceScale", None)
        qty_scale = -int(math.log10(float(data['lotSizeFilter']['qtyStep'])))
        return {'price_scale': price_scale, 'qty_scale': qty_scale}

    async def get_server_time(self) -> int:
        """
        Get server time
        
        Returns:
            Server time in milliseconds
        """
        response = await self._request("GET", "/v5/market/time")
        return int(int(response.get("timeNano", 0)) / 1000000)
    
    async def get_symbols(self, category: str = "spot") -> List[Dict[str, Any]]:
        """
        Get available trading symbols
        
        Args:
            category: Product category (spot, linear, inverse)
            
        Returns:
            List of symbols
        """
        params = {"category": category}
        response = await self._request("GET", "/v5/market/instruments-info", params=params)
        return response.get("list", [])
    
    async def get_ticker(self, category: str, symbol: str) -> Dict[str, Any]:
        """
        Get ticker information for a symbol
        
        Args:
            category: Product category (spot, linear, inverse)
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            
        Returns:
            Ticker data
        """
        params = {
            "category": category,
            "symbol": symbol
        }
        response = await self._request("GET", "/v5/market/tickers", params=params)
        tickers = response.get("list", [])
        return tickers[0] if tickers else {}
    
    async def get_klines(self, category: str, symbol: str, interval: str, 
                        start: Optional[int] = None, end: Optional[int] = None, 
                        limit: int = 200) -> List[Dict[str, Any]]:
        """
        Get kline/candle data for a symbol
        
        Args:
            category: Product category (spot, linear, inverse)
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            interval: Kline interval (1, 3, 5, 15, 30, 60, 120, 240, 360, 720, D, W, M)
            start: Start timestamp in milliseconds
            end: End timestamp in milliseconds
            limit: Number of candles to return (max 1000)
            
        Returns:
            List of kline data
        """
        params = {
            "category": category,
            "symbol": symbol,
            "interval": interval,
            "limit": str(limit)
        }
        
        if start:
            params["start"] = str(start)
        if end:
            params["end"] = str(end)
            
        response = await self._request("GET", "/v5/market/kline", params=params)
        return response.get("list", [])
    
    async def get_orderbook(self, category: str, symbol: str, limit: int = 50) -> Dict[str, Any]:
        """
        Get orderbook for a symbol
        
        Args:
            category: Product category (spot, linear, inverse)
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            limit: Depth of the orderbook (1-200)
            
        Returns:
            Orderbook data
        """
        params = {
            "category": category,
            "symbol": symbol,
            "limit": str(limit)
        }
        return await self._request("GET", "/v5/market/orderbook", params=params)
    
    # Account Endpoints
    
    async def get_wallet_balance(self, account_type: str = "UNIFIED", coin: Optional[str] = None) -> Dict[str, Any]:
        """
        Get wallet balance
        
        Args:
            account_type: Account type (UNIFIED, CONTRACT, SPOT)
            coin: Filter by coin
            
        Returns:
            Wallet balance data
        """
        params = {"accountType": account_type}
        if coin:
            params["coin"] = coin
            
        return await self._request("GET", "/v5/account/wallet-balance", params=params, auth=True)
    
    async def get_positions(self, category: str, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get positions
        
        Args:
            category: Product category (linear, inverse)
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            
        Returns:
            List of positions
        """
        params = {"category": category}
        if symbol:
            params["symbol"] = symbol
            
        response = await self._request("GET", "/v5/position/list", params=params, auth=True)
        return response.get("list", [])
    
    # Order Endpoints
    
    async def place_order(self, category: str, symbol: str, side: str, order_type: str, 
                         qty: str, price: Optional[str] = None, time_in_force: str = "GTC",
                         reduce_only: bool = False, close_on_trigger: bool = False,
                         order_link_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Place a new order
        
        Args:
            category: Product category (spot, linear, inverse)
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            side: Order side (Buy, Sell)
            order_type: Order type (Limit, Market)
            qty: Order quantity
            price: Order price (required for Limit orders)
            time_in_force: Time in force (GTC, IOC, FOK, PostOnly)
            reduce_only: Whether the order is reduce-only
            close_on_trigger: Whether to close position on trigger
            order_link_id: Custom order ID
            
        Returns:
            Order data
        """
        params = {
            "category": category,
            "symbol": symbol,
            "side": side,
            "orderType": order_type,
            "qty": str(qty),
            "timeInForce": time_in_force
        }
        
        if price and order_type.lower() == "limit":
            params["price"] = str(price)
            
        if reduce_only:
            params["reduceOnly"] = "true"
            
        if close_on_trigger:
            params["closeOnTrigger"] = "true"
            
        if order_link_id:
            params["orderLinkId"] = order_link_id
            
        return await self._request("POST", "/v5/order/create", params=params, auth=True)
    
    async def cancel_order(self, category: str, symbol: str, 
                          order_id: Optional[str] = None, 
                          order_link_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Cancel an order
        
        Args:
            category: Product category (spot, linear, inverse)
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            order_id: Order ID
            order_link_id: Custom order ID
            
        Returns:
            Cancellation result
        """
        params = {
            "category": category,
            "symbol": symbol
        }
        
        if order_id:
            params["orderId"] = order_id
        elif order_link_id:
            params["orderLinkId"] = order_link_id
        else:
            raise Exception("Either order_id or order_link_id must be provided")
            
        return await self._request("POST", "/v5/order/cancel", params=params, auth=True)
    
    async def cancel_all_orders(self, category: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        """
        Cancel all orders
        
        Args:
            category: Product category (spot, linear, inverse)
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            
        Returns:
            Cancellation result
        """
        params = {"category": category}
        if symbol:
            params["symbol"] = symbol
            
        return await self._request("POST", "/v5/order/cancel-all", params=params, auth=True)
    
    async def get_order(self, category: str, symbol: Optional[str] = None,
                       order_id: Optional[str] = None, 
                       order_link_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get order details
        
        Args:
            category: Product category (spot, linear, inverse)
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            order_id: Order ID
            order_link_id: Custom order ID
            
        Returns:
            Order details
        """
        params = {"category": category}
        
        if symbol:
            params["symbol"] = symbol
            
        if order_id:
            params["orderId"] = order_id
        elif order_link_id:
            params["orderLinkId"] = order_link_id
            
        response = await self._request("GET", "/v5/order/realtime", params=params, auth=True)
        orders = response.get("list", [])
        return orders[0] if orders else {}
    
    async def get_open_orders(self, category: str, symbol: Optional[str] = None, 
                             limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get open orders
        
        Args:
            category: Product category (spot, linear, inverse)
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            limit: Number of orders to return (max 50)
            
        Returns:
            List of open orders
        """
        params = {
            "category": category,
            "limit": str(limit)
        }
        
        if symbol:
            params["symbol"] = symbol
            
        response = await self._request("GET", "/v5/order/realtime", params=params, auth=True)
        return response.get("list", [])
    
    async def get_order_history(self, category: str, symbol: Optional[str] = None,
                               limit: int = 50, cursor: Optional[str] = None) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Get order history
        
        Args:
            category: Product category (spot, linear, inverse)
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            limit: Number of orders to return (max 50)
            cursor: Cursor for pagination
            
        Returns:
            Tuple of (list of orders, next cursor)
        """
        params = {
            "category": category,
            "limit": str(limit)
        }
        
        if symbol:
            params["symbol"] = symbol
            
        if cursor:
            params["cursor"] = cursor
            
        response = await self._request("GET", "/v5/order/history", params=params, auth=True)
        return response.get("list", []), response.get("nextPageCursor")
    
    async def get_executions(self, category: str, symbol: Optional[str] = None,
                            limit: int = 50, cursor: Optional[str] = None) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Get trade executions
        
        Args:
            category: Product category (spot, linear, inverse)
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            limit: Number of executions to return (max 100)
            cursor: Cursor for pagination
            
        Returns:
            Tuple of (list of executions, next cursor)
        """
        params = {
            "category": category,
            "limit": str(limit)
        }
        
        if symbol:
            params["symbol"] = symbol
            
        if cursor:
            params["cursor"] = cursor
            
        response = await self._request("GET", "/v5/execution/list", params=params, auth=True)
        return response.get("list", []), response.get("nextPageCursor")
    
    # Demo Account Endpoints
    
    async def request_demo_funds(self, coins: Optional[List[Dict[str, str]]] = None) -> Dict[str, Any]:
        """
        Request demo funds
        
        Args:
            coins: List of coin dictionaries with 'coin' and 'amountStr' keys
                  e.g., [{'coin': 'USDT', 'amountStr': '10000'}, {'coin': 'BTC', 'amountStr': '1'}]
            
        Returns:
            Request result
        """
        params = {
            "adjustType": 0  # 0: add demo funds
        }
        
        if coins:
            params["utaDemoApplyMoney"] = coins
        else:
            # Default request for common trading coins
            params["utaDemoApplyMoney"] = [
                {"coin": "USDT", "amountStr": "10000"},
                {"coin": "BTC", "amountStr": "1"},
                {"coin": "ETH", "amountStr": "10"}
            ]
            
        return await self._request("POST", "/v5/account/demo-apply-money", params=params, auth=True)