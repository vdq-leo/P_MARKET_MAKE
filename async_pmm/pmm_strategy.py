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
        self._init_risk_management_params()
        
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
        self.spread = None
        self.skew = 0.0

        # Order states
        self.is_buy = True
        self.is_sell = True
        
        # Inventory management
        self.inventory_ratio = 0.5  # Neutral position (0.0 = all quote, 1.0 = all base)
        self.trade_value = 0.0
        self.trade_base_size = 0.0

        # For perp
        self.position_normalize = 0.0
        self.cum_rlzd_pnl = 0.0
        self.peak = 0.0

        # Order tracking
        self.active_orders = {}
        self.last_order_time = 0
        
        self.logger.info(f"Initialized PMM strategy for {self.symbol_pair}")
    
    @staticmethod
    def clamp(value, min_value, max_value):
        return max(min(value, max_value), min_value)

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

    
    def _init_inventory_params(self, strategy_params: Optional[Dict[str, Any]] = None):
        """Initialize inventory management parameters"""
        self.inventory = {
            "target_ratio": 0.5,        # Target inventory ratio (0.5 = 50% base, 50% quote)
            "base_balance": 0.0,        # Current base asset balance
            "quote_balance": 0.0,       # Current quote asset balance
            "total_value": 0.0,         # Total portfolio value in quote currency
            "base_trade_value": 50.0,
            "min_trade_value": 10.0,     # Minimum trade size in quote currency
            "max_trade_value": 100.0    # Maximum trade size in quote currency
        }
        # Override defaults with provided parameters
        if strategy_params:
            for key, value in strategy_params.items():
                if key in self.params:
                    self.params[key] = value
                    # self.logger.info(f"Overriding parameter {key}: {value}")

    def _init_risk_management_params(self, strategy_params: Optional[Dict[str, Any]] = None):
        """Initialize risk management parameters"""
        self.risk = {
            "max_position_value": 500.0,  # Maximum position value in quote currency
            "take_profit_value": 50, # Take profit value
            "stop_loss_value": 50, # Stop loss value
            "max_drawdown": 200 # Maximum drawdown in quote currency
        }
        # Override defaults with provided parameters
        if strategy_params:
            for key, value in strategy_params.items():
                if key in self.params:
                    self.params[key] = value
                    # self.logger.info(f"Overriding parameter {key}: {value}")

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
            position = self.position_data[0]

            side = 1 if position["side"].lower() == "buy" else -1 
            size = float(position["size"])
            position_normalize = size * side / self.trade_base_size

            self.position_normalize = position_normalize
            self.skew = -self.params["skew_factor"] * position_normalize * self.volatility**2 * 0.5

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
            
            self.trade_value = self.clamp(self.trade_value, self.inventory["min_trade_value"], self.inventory["max_trade_value"])
            
            if self.fair_price == 0:
                fair_price = await self.compute_fair_price()
                self.trade_base_size = self.trade_value / fair_price
            else:
                self.trade_base_size = self.trade_value / self.fair_price
            
        except Exception as e:
            self.logger.error(f"Error updating trade size: {e}")
    
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
            vol_spread = self.params['base_spread'] + self.volatility * self.params["vol_scale"] * 0.5
            
            # Update only if there's a change
            if not hasattr(self, "spread") or abs(vol_spread - self.spread) > 1e-9:
                self.spread = vol_spread
                # self.logger.info(f"Updated spread: {self.spread}")

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
                    fair_price = (bid_price * bid_size + ask_price * ask_size) / (bid_size + ask_size)
                else:
                    fair_price = (bid_price + ask_price) / 2
            else:
                self.logger.warning("Cannot calculate fair price: Missing bid/ask data")
                return None
            
            self.fair_price = fair_price + self.skew

            return fair_price

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
            
            buy_task = self.generate_orders_for_side("buy", fair_price) if self.is_buy else None
            sell_task = self.generate_orders_for_side("sell", fair_price) if self.is_sell else None

            buy_orders, sell_orders = await asyncio.gather(
                buy_task if buy_task else asyncio.sleep(0),
                sell_task if sell_task else asyncio.sleep(0)
            )
            
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
            spread = self.spread
            base_price = fair_price * (1 - spread) if is_buy else fair_price * (1 + spread)
            price_step = max(self.spread, self.params["min_spread"])
            size_multiplier = self.params["size_multiplier"]
            max_trade = self.inventory["max_trade_value"]
            min_trade = self.inventory["min_trade_value"]

            tick_size = self.tick_size
            price_scale = self.price_scale
            qty_scale = self.qty_scale

            
            for i in range(self.params["order_count"]):
                factor = 1.0 + i * price_step

                if is_buy:
                    raw_price = base_price / factor
                    price = min(raw_price, raw_price - tick_size)
                else:
                    raw_price = base_price * factor
                    price = max(raw_price, raw_price + tick_size)

                price = round(price, price_scale)

                size_base =  self.trade_base_size * (size_multiplier ** i)
                size_base = self.clamp(size_base, min_trade / price, max_trade / price)
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
    
    async def handle_position_management(self):
        """
        Determine if the bot should handle (i.e., exit) the current position (only for linear contracts).

        Returns:
            bool: True if take-profit or stop-loss condition is met; otherwise False.
        """
        if self.category != "linear":
            return False

        position = self.position_data[0]
        if not position:
            return False

        side = position["side"].lower()
        curr_position = float(position.get('positionValue', 0))
        unrlzd_pnl = float(position.get('unrealisedPnl', 0))

        # Adjust trading direction based on position size
        if abs(curr_position) > self.params["max_position_size"]:
            if side == 'buy':
                self.is_buy = False
            else:
                self.is_sell = False
        else:
            self.is_buy = True
            self.is_sell = True

        # Check take profit / stop loss conditions
        if unrlzd_pnl > self.risk['take_profit_value']:
            self.logger.info(f"Take profit triggered: {unrlzd_pnl}")
            self.cum_rlzd_pnl += unrlzd_pnl
            return True
        elif unrlzd_pnl < self.risk['stop_loss_value']:
            self.logger.info(f"Stop loss triggered: {unrlzd_pnl}")
            self.cum_rlzd_pnl += unrlzd_pnl
            return True

        return False

    async def handle_risk_management(self):
        """
        Check if cumulative realized PnL has breached max drawdown threshold (only for linear contracts).

        Returns:
            bool: True if risk limit breached and strategy should stop; otherwise False.
        """
        if self.category != "linear":
            return False

        # Update peak if new high
        self.peak = max(self.peak, self.cum_rlzd_pnl)

        # Calculate drawdown
        drawdown = self.cum_rlzd_pnl - self.peak

        # Check drawdown breach
        if drawdown <= -self.risk['max_drawdown_value']:
            self.logger.error(f"Max drawdown exceeded: {drawdown} — Stopping strategy.")
            return True

        return False


            

