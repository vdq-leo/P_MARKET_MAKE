"""
PMM Strategy Module - Core strategy implementation for market making
"""
import asyncio
import logging
import time
import numpy as np
import json
import math
from typing import Dict, List, Optional, Tuple, Any, Callable
from datetime import datetime

class PMMStrategy:
    """
    Price Market Making Strategy - Core logic for market making
    Handles volatility, skew, and spread calculations
    """
    
    def __init__(self ,symbol: str, quote: str = "USDT", strategy_params: Optional[Dict[str, Any]] = None, category: str = "linear"):
        """
        Initialize the PMM strategy
        
        Args:
            symbol: Trading pair base symbol (e.g., 'BTC')
            quote: Quote currency (e.g., 'USDT')
            strategy_params: Optional strategy parameters to override defaults
        """
        self.symbol = symbol
        self.quote = quote
        self.symbol_pair = f"{symbol}{quote}"
        self.category = category

        # Initialize clients
        self.rest_client = None
        self.ws_client = None   
        
        # Setup logging
        self._setup_logger()
        
        # Initialize parameters
        self._init_strategy_params(strategy_params)
        self._init_inventory_params()
        self._init_risk_management_params() # Not use yet
        
        # Market data
        self.ticker_data = {}
        self.kline_data = {}
        self.order_book = {}
        self.position_data = {}
        
        # Scale data
        self.price_scale = None
        self.qty_scale = None
        self.tick_size = None

        # Strategy state
        self.volatility = 0.0
        self.fair_price = 0.0
        self.old_fair_price = 0.0
        self.skew = 0.0
        self.inventory_ratio = 0.5  # Neutral position (0.0 = all quote, 1.0 = all base)
        self.position = 0.0
        self.trade_size = 0.0
        self.spread = None
        
        # Order tracking
        self.active_orders = {}
        self.last_order_time = 0
        
        self.logger.info(f"Initialized PMM strategy for {self.symbol_pair}")
    
    async def set_scale(self):
        scale = await self.rest_client.get_scale(self.category, self.symbol_pair)
        self.price_scale = int(scale['price_scale'])
        self.qty_scale = int(scale['qty_scale'])
        self.tick_size = math.log10(1 / (10 ** self.price_scale))
        self.logger.info(f"Set scale for {self.symbol_pair}: {self.price_scale}, {self.qty_scale}")

    def set_clients(self, rets_client: Any, ws_client: Any):
        """Set the exchange API client"""
        self.rest_client = rets_client
        self.ws_client = ws_client
        self.logger.info(f"Set clients for {self.symbol_pair}")

    def _setup_logger(self):
        """Setup strategy logger"""
        self.logger = logging.getLogger(f"pmm_strategy_{self.symbol}")
        self.logger.setLevel(logging.INFO)
        
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        # Prevent logs from being passed to the root logger
        self.logger.propagate = False
    
    def _init_strategy_params(self, strategy_params: Optional[Dict[str, Any]] = None):
        """Initialize strategy parameters with defaults or provided values"""
        # Default strategy parameters
        self.params = {
            # Order parameters
            "use_imbalance_price": False,  # Use imbalance price
            "order_count": 3,           # Number of orders on each side
            "base_spread": 0.0020,
            "min_spread": 0.0020,       # Minimum spread (0.2%)
            "max_spread": 0.0050,       # Maximum spread (0.5%)
            "vol_scale": 1.0,           # Volatility impact on spread
            "skew_factor": 0.2,         # Inventory skew impact
            "refresh_time": 60,         # Order refresh time in seconds
            
            # Volatility parameters
            "use_dynamic_vol": False,
            "vol_window": 10,           # Window for volatility calculation
            
            # Order distribution
            "size_multiplier": 1,     # Size multiplier for orders further from mid
        }
        
        # Override defaults with provided parameters
        if strategy_params:
            for key, value in strategy_params.items():
                if key in self.params:
                    self.params[key] = value
                    # self.logger.info(f"Overriding parameter {key}: {value}")
                else:
                    self.logger.warning(f"Unknown parameter: {key}")
    
    def _init_inventory_params(self):
        """Initialize inventory management parameters"""
        self.inventory = {
            "target_ratio": 0.5,        # Target inventory ratio (0.5 = 50% base, 50% quote)
            "base_balance": 0.0,        # Current base asset balance
            "quote_balance": 0.0,       # Current quote asset balance
            "total_value": 0.0,         # Total portfolio value in quote currency
            "base_trade_size": 50.0,
            "min_trade_size": 10.0,     # Minimum trade size in quote currency
            "max_trade_size": 100.0    # Maximum trade size in quote currency
        }
    
    def _init_risk_management_params(self):
        """Initialize risk management parameters"""
        self.risk = {
            "max_position": 1000.0,     # Maximum position size in quote currency
            "max_drawdown": 0.05,       # Maximum allowed drawdown (5%)
            "stop_loss": 0.10,          # Stop loss threshold (10%)
        }
    
    async def update_market_data(self, ticker: Dict[str, Any] = None, klines: List[Dict[str, Any]] = None, 
                                order_book: Dict[str, Any] = None, position: Dict[str, Any] = None):
        """
        Update market data from exchange feeds
        
        Args:
            ticker: Latest ticker data
            klines: Latest kline/candle data
            order_book: Latest order book data
        """
        if ticker:
            self.ticker_data = ticker
            
        if klines:
            self.kline_data = klines
            
        if order_book:
            self.order_book = order_book

        if position:
            self.position_data = position
            
        # Update derived metrics
        await self.update_market_metrics()
    
    async def update_market_metrics(self):
        """Update market metrics based on latest data"""
        await asyncio.gather(
            self.update_volatility(),
            self.update_inventory_skew(),
            self.update_trade_size(),
            self.update_spread(),
        )
    
    async def update_volatility(self) -> float:
        """
        Calculate market volatility from kline data
        
        Returns:
            Current volatility estimate
        """
        if not self.kline_data:
            return self.volatility
            
        try:
            # Extract close prices from klines
            closes = [float(k['close']) for k in self.kline_data 
                     if 'close' in k and k['close'] is not None]
            
            if len(closes) < self.params["vol_window"]:
                return self.volatility
                
            # Calculate returns
            returns = np.diff(np.log(closes))
            
            # Calculate volatility (standard deviation of returns)
            vol = np.std(returns[-self.params["vol_window"]:]) * np.sqrt(self.params["vol_window"])  # Annualized
            
            # Apply EMA smoothing
            self.volatility = vol
            
            return self.volatility
            
        except Exception as e:
            self.logger.error(f"Error calculating volatility: {e}")
            return self.volatility

    async def update_inventory_skew(self) -> float:
        """
        Update inventory ratio based on current balances
        
        Returns:
            Current inventory ratio (0.0 = all quote, 1.0 = all base)
        """
        if self.category == "linear":
            self.skew = self.volatility * self.position * self.params["skew_factor"]
            return
        
        try:
            base_value = self.inventory["base_balance"] * self.fair_price
            total_value = base_value + self.inventory["quote_balance"]
            
            if total_value > 0:
                self.inventory_ratio = base_value / total_value
                self.inventory["total_value"] = total_value
            else:
                self.inventory_ratio = 0.5  # Default to neutral if no balance
                
            # Calculate skew based on deviation from target ratio
            self.skew = (self.inventory_ratio - self.inventory["target_ratio"]) * self.params["skew_factor"]
            
        except Exception as e:
            self.logger.error(f"Error updating inventory ratio: {e}")
    
    async def update_trade_size(self) -> float:
        """
        Calculate appropriate trade size based on volatility and balance
        
        Returns:
            Calculated trade size in quote currency
        """
        try:
            # Ensure within min/max bounds
            self.trade_size = max(
                self.inventory["min_trade_size"],
                min(self.inventory['base_trade_size'], self.inventory["max_trade_size"])
            )
            
            return self.trade_size
            
        except Exception as e:
            self.logger.error(f"Error updating trade size: {e}")
            return self.trade_size
    
    async def update_spread(self) -> float:
        """
        Calculate bid-ask spread based on volatility and market conditions.
        Only update if the new spread differs from the current one.

        Returns:
            Calculated spread as a decimal (e.g., 0.001 = 0.1%)
        """
        self.spread = float(self.params['base_spread']) if self.spread is None else self.spread

        if not self.params['use_dynamic_vol']:
            return self.spread
        
        try:
            # Calculate spread based on volatility
            vol_spread = self.volatility * self.params["vol_scale"]
            
            # Clamp within min/max bounds
            new_spread = max(
                self.params["min_spread"],
                min(vol_spread, self.params["max_spread"])
            )

            # Update only if there's a change
            if not hasattr(self, "spread") or abs(new_spread - self.spread) > 1e-9:
                self.spread = new_spread
                self.logger.info(f"Updated spread: {self.spread}")

            return self.spread

        except Exception as e:
            self.logger.error(f"Error updating spread: {e}")
            return getattr(self, "spread", 0.0)
    
    async def compute_fair_price(self) -> float:
        """
        Calculate fair price from market data

        Returns:
            Calculated fair price
        """
        try:
            data = self.ticker_data
            required_keys = {'bid1Price', 'ask1Price', 'bid1Size', 'ask1Size'}
            if not data or not required_keys <= data.keys():
                data = await self.rest_client.get_ticker(self.category, self.symbol_pair)

            bid_price = float(data.get('bid1Price', 0))
            ask_price = float(data.get('ask1Price', 0))
            bid_size = float(data.get('bid1Size', 0))
            ask_size = float(data.get('ask1Size', 0))

            if bid_price > 0 and ask_price > 0:
                if self.params['use_imbalance_price'] and bid_size > 0 and ask_size > 0:
                    self.fair_price = (bid_price * bid_size + ask_price * ask_size) / (bid_size + ask_size)
                else:
                    self.fair_price = (bid_price + ask_price) / 2
            else:
                self.logger.warning("Cannot calculate fair price: Missing bid/ask data")
                return None
            
            return self.fair_price

        except Exception as e:
            self.logger.error(f"Error computing fair price: {e}")
            return None

    
    async def generate_orders(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Generate orders for both sides of the book
        
        Returns:
            Dictionary with 'buy' and 'sell' lists of order specifications
        """
        try:
            # Ensure we have a valid fair price
            fair_price = await self.compute_fair_price()

            if fair_price is None or fair_price <= 0:
                self.logger.warning("Cannot generate orders: Invalid fair price")
                return {"buy": [], "sell": []}
            
            # Generate orders for both sides
            buy_orders = await self.generate_orders_for_side("buy", fair_price)
            sell_orders = await self.generate_orders_for_side("sell", fair_price)
            
            if not buy_orders or not sell_orders:
                self.logger.warning("No orders generated.")
                return {"buy": [], "sell": []}
            
            return {
                "buy": buy_orders,
                "sell": sell_orders
            }
            
        except Exception as e:
            self.logger.error(f"Error generating orders: {e}")
            return {"buy": [], "sell": []}

    async def generate_orders_for_side(self, side: str, fair_price: float) -> List[Dict[str, Any]]:
        """
        Generate orders for one side of the book
        
        Args:
            side: Order side ('buy' or 'sell')
            fair_price: Current fair price
            
        Returns:
            List of order specifications
        """
        orders = []
        try:
            is_buy = side == "buy"
            spread_with_skew = self.spread + self.skew if is_buy else self.spread - self.skew
            spread_with_skew = max(self.params["min_spread"], spread_with_skew)

            base_price = fair_price * (1 - spread_with_skew) if is_buy else fair_price * (1 + spread_with_skew)
            price_step = max(self.spread, self.params["min_spread"])
            size_multiplier = self.params["size_multiplier"]
            max_trade = self.inventory["max_trade_size"]
            min_trade = self.inventory["min_trade_size"]

            tick_size = self.tick_size
            price_scale = self.price_scale
            qty_scale = self.qty_scale
            trade_size = self.trade_size

            for i in range(self.params["order_count"]):
                factor = 1.0 + i * price_step

                if is_buy:
                    raw_price = base_price / factor
                    price = min(raw_price, raw_price - tick_size)
                else:
                    raw_price = base_price * factor
                    price = max(raw_price, raw_price + tick_size)

                price = round(price, price_scale)

                size_quote = trade_size * (size_multiplier ** i)
                size_base = size_quote / price
                size_base = max(min(size_base, max_trade / price), min_trade / price)
                size_base = round(size_base, qty_scale)

                orders.append({
                    "side": side,
                    "price": price,
                    "quantity": size_base,
                    "type": "limit"
                })

            return orders

        except Exception as e:
            self.logger.error(f"Error generating {side} orders: {e}")
            return []

    
    async def update_balances(self, base_balance: float, quote_balance: float):
        """
        Update current balances
        
        Args:
            base_balance: Current base asset balance
            quote_balance: Current quote asset balance
        """
        self.inventory["base_balance"] = base_balance
        self.inventory["quote_balance"] = quote_balance
        await self.update_inventory_ratio()
    
    async def process_trade(self, trade: Dict[str, Any]):
        """
        Process a completed trade and update strategy state
        
        Args:
            trade: Trade details
        """
        try:
            side = trade.get("side", "")
            price = float(trade.get("price", 0))
            quantity = float(trade.get("quantity", 0))
            
            # self.logger.info(f"Processing trade: {side} {quantity} @ {price}")
            
            # Update balances based on trade
            if side.lower() == "buy":
                self.inventory["base_balance"] += quantity
                self.inventory["quote_balance"] -= quantity * price
            else:
                self.inventory["base_balance"] -= quantity
                self.inventory["quote_balance"] += quantity * price
            
            # Update inventory ratio
            await self.update_inventory_skew()
            
        except Exception as e:
            self.logger.error(f"Error processing trade: {e}")
    
    async def should_refresh_orders(self, current_time: float) -> bool:
        """
        Determine if orders should be refreshed
        
        Args:
            current_time: Current timestamp
            
        Returns:
            True if orders should be refreshed
        """
        # Refresh if too much time has passed
        time_since_last_order = current_time - self.last_order_time
        if time_since_last_order > self.params["refresh_time"]:
            # self.logger.info("Refreshing orders due to inactivity...")
            return True
            
        # Refresh if fair price has moved significantly
        if self.old_fair_price:
            fair_price = await self.compute_fair_price()
            price_change = abs(fair_price - self.old_fair_price) / self.old_fair_price
            if price_change >= self.params['min_spread'] / 2:
                # self.logger.info("Refreshing orders due to price change...")
                return True
            
        return False