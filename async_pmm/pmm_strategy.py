"""
PMM Strategy Module - Core strategy implementation for market making
"""
import asyncio
import time
import numpy as np
import math
import pandas as pd
from .numba_compute import *
from typing import Dict, List, Optional, Tuple, Any, Callable
from logger import setup_logger_global

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
        self.scan_time = int(time.time() * 1000)
        
        # Initialize clients
        self._market_metrics_initialized = False
        self.rest_client = None
        self.ws_client = None   
        
        # Setup logging
        self._setup_logger()
        
        # Initialize parameters
        self._init_strategy_params(strategy_params)
        self._init_inventory_params(strategy_params)
        self._init_risk_management_params(strategy_params)
        
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
        self.spread = self.params["base_spread"]
        self.volatility = 0.0
        self.atr = 0.0
        self.fair_price = 0.0
        self.old_fair_price = 0.0
        self.skew = 0.0

        self.bid = None
        self.ask = None

        # Order states
        self.is_buy = False
        self.is_sell = False
        self.tight_regime = True

        # Inventory management
        self.inventory_ratio = 0.5  # Neutral position (0.0 = all quote, 1.0 = all base)
        self.trade_value = 0.0
        self.trade_base_size = 0.0

        # For perp
        self.position_normalize = 0.0
        self.cum_rlzd_pnl = 0.0
        self.peak = 0.0
        self.unrlzd_pnl = 0.0
        self.nominal_value = 0.0

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
        self.logger = setup_logger_global('PMM_STRATEGY', f"pmm_strategy_{self.symbol_pair}")
    
    def _init_strategy_params(self, strategy_params: Optional[Dict[str, Any]] = None):
        """Initialize strategy parameters with defaults or provided values"""
        # Default strategy parameters
        self.params = {
            # Order parameters
            "use_imbalance_price": True,  # Use imbalance price
            "order_count": 1,           # Number of orders on each side
            "base_spread": 0.0020,
            "min_spread": 0.0010,       # Minimum spread (0.2%)
            "max_spread": 0.0050,       # Maximum spread (0.5%)
            "vol_scale": 1.0,           # Volatility impact on spread
            "skew_factor": 0.5,         # Inventory skew impact
            "refresh_time": 60,         # Order refresh time in seconds
            
            # Volatility parameters
            "use_dynamic_vol": True,    # Use dynamic volatility
            "vol_window": 10,           # Window for volatility calculation
            "vol_threshold": 0.0005,          # Tolerance for regime detection    

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
                if key in self.inventory:
                    self.inventory[key] = value
                    # self.logger.info(f"Overriding parameter {key}: {value}")

    def _init_risk_management_params(self, strategy_params: Optional[Dict[str, Any]] = None):
        """Initialize risk management parameters"""
        self.risk = {
            "max_position_value": 500.0,  # Maximum position value in quote currency
            "take_profit_value": 50, # Take profit value
            "stop_loss_value": 50, # Stop loss value
            "max_drawdown_value": 200 # Maximum drawdown in quote currency
        }
        # Override defaults with provided parameters
        if strategy_params:
            for key, value in strategy_params.items():
                if key in self.risk:
                    self.risk[key] = value
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

        if not self._market_metrics_initialized:
            await self.update_market_metrics()

            self._market_metrics_initialized = True

    async def _cron_update_metrics(self, interval: float = 1.0):
        """
        Run market metrics update periodically like a cron job.

        Args:
            interval: Seconds between each update
        """
        while True:
            try:
                await self.update_market_metrics()
            except Exception as e:
                self.logger.error(f"Cron job error in update_market_metrics: {e}")
            await asyncio.sleep(interval)
    
    async def update_market_metrics(self):
        """Update market metrics based on latest data"""
        await asyncio.gather(
            self.update_regime(),
            self.update_volatility(),
            self.update_inventory_skew(),
            self.update_trade_size(),
            self.update_spread(),
        )
    
    async def update_volatility(self) -> float:
        """
        Calculate market volatility from kline data using JIT-accelerated function
        """
        if not self.kline_data:
            return self.volatility

        try:
            closes = np.array([
                float(k['close']) for k in self.kline_data
                if 'close' in k and k['close'] is not None
            ], dtype=np.float64)

            if len(closes) < self.params["vol_window"] + 1:
                return self.volatility

            log_closes = np.log(closes)
            vol = compute_volatility(log_closes, self.params["vol_window"])

            self.volatility = vol
            return self.volatility

        except Exception as e:
            self.logger.error(f"Error calculating volatility: {e}")
            return self.volatility

    async def update_regime(self):
        """
        Determine market regime (trend vs mean-reversion) based on a normalized,
        smoothed Volatility/ATR ratio over a rolling window using Numba-accelerated logic.
        """
        volatility = self.volatility
        volatility_threshold = self.params["vol_threshold"]
        if not volatility:
            return self.tight_regime

        try:
            if volatility >= volatility_threshold:
                self.tight_regime = False
            else:
                self.tight_regime = True

            return self.tight_regime
        
        except Exception as e:
            self.logger.exception(f"Error calculating regime: {e}")
            return self.tight_regime
        
    async def update_inventory_skew(self) -> float:
        """
        Update inventory ratio and compute inventory-based skew

        Returns:
            Updated skew value (positive = favor buying, negative = favor selling)
        """
        try:
            skew_factor = self.params["skew_factor"]

            if self.category == "linear":

                if not self.position_data:
                    return self.skew
                
                position = self.position_data[0]

                side = 1 if position["side"].lower() == "buy" else -1

                if position["size"] is None:
                    return self.skew
                
                size = float(position["size"])
                base_unit = self.trade_base_size
                volatility = self.volatility or 0.00001  # avoid zero

                position_normalized = int(size / base_unit) * side

                # Skew logic: more inventory → less aggressive on that side
                skew = - position_normalized * volatility * skew_factor
                self.position_normalize = position_normalized
                
            else:
                # Spot-based inventory skew
                fair_price = self.fair_price or 1.0
                base_value = self.inventory.get("base_balance", 0.0) * fair_price
                quote_value = self.inventory.get("quote_balance", 0.0)
                total_value = base_value + quote_value

                if total_value > 0:
                    inventory_ratio = base_value / total_value
                else:
                    inventory_ratio = 0.5  # neutral

                target_ratio = self.inventory.get("target_ratio", 0.5)
                skew = (inventory_ratio - target_ratio) * skew_factor

                self.inventory_ratio = inventory_ratio
                self.inventory["total_value"] = total_value
                self.skew = skew

            return self.skew

        except Exception as e:
            self.logger.error(f"Error updating inventory skew: {e}")
            return 0.0

    async def update_trade_size(self) -> float:
        """
        Calculate appropriate trade size based on volatility and balance
        
        Returns:
            Calculated trade size in quote currency
        """
        try:
            # Ensure within min/max bounds
            
            self.trade_value = self.inventory['base_trade_value']      
                  
            if self.fair_price == 0:
                fair_price = await self.compute_fair_price()
                self.trade_base_size = self.trade_value / fair_price
            else:
                self.trade_base_size = self.trade_value / self.fair_price
            
        except Exception as e:
            self.logger.error(f"Error updating trade size: {e}")
    
    async def update_spread(self) -> float:
        """
        Dynamically update the bid-ask spread based on market volatility and strategy parameters.

        Returns:
            Updated spread value (e.g., 0.001 for 0.1%)
        """
        try:
            base_spread = float(self.params.get("base_spread"))
            vol_scale = float(self.params.get("vol_scale"))
            min_spread = float(self.params.get("min_spread"))
            max_spread = float(self.params.get("max_spread"))
            use_dynamic = self.params.get("use_dynamic_vol")
            tight_regime = self.tight_regime

            # Default to base spread if no volatility adjustment is needed
            if not use_dynamic or tight_regime:
                self.spread = base_spread
                return self.spread
            
            # Compute dynamic spread
            vol = self.volatility or 0.0
            dynamic_spread = base_spread + vol * vol_scale
            dynamic_spread = clamp(dynamic_spread, min_spread, max_spread)

            # Update only if significantly different
            if abs(dynamic_spread - self.spread) > 1e-6:
                self.spread = dynamic_spread
                # self.logger.info(f"Updated spread to: {self.spread:.6f}")

            return self.spread

        except Exception as e:
            self.logger.error(f"Error updating spread: {e}")
            return self.spread if hasattr(self, "spread") else 0.001
            
    async def compute_fair_price(self) -> float:
        """
        Compute the fair price using top-of-book data and optional inventory skew adjustment.

        Returns:
            Fair price (float) or None if data is invalid
        """
        try:
            data = self.ticker_data or {}
            required_keys = {'bid1Price', 'ask1Price', 'bid1Size', 'ask1Size'}

            if not required_keys <= data.keys():
                # Fetch fresh data if incomplete
                data = await self.rest_client.get_ticker(self.category, self.symbol_pair)

            bid_price = float(data.get('bid1Price', 0))
            ask_price = float(data.get('ask1Price', 0))
            bid_size = float(data.get('bid1Size', 0))
            ask_size = float(data.get('ask1Size', 0))

            if bid_price <= 0 or ask_price <= 0:
                self.logger.warning("Invalid bid/ask price for fair price computation.")
                return None

            use_imbalance = self.params.get("use_imbalance_price", False)

            if use_imbalance and bid_size > 0 and ask_size > 0:
                fair_price_ = (bid_price * bid_size + ask_price * ask_size) / (bid_size + ask_size)
            else:
                fair_price_ = (bid_price + ask_price) / 2.0

            # Add inventory skew if available and valid
            skew = self.skew if isinstance(self.skew, (float, int)) else 0.0

            fair_price = fair_price_ * (1 + skew)
            self.fair_price = fair_price

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
            is_buy = self.is_buy
            is_sell = self.is_sell
            fair_price = await self.compute_fair_price()

            if fair_price is None or fair_price <= 0:
                self.logger.warning("Cannot generate orders: Invalid fair price")
                return {"buy": [], "sell": []}
            
            tasks = []

            # Append in order, preserving position for unpacking
            tasks.append(self.generate_orders_for_side("buy", fair_price) if is_buy else asyncio.sleep(0))
            tasks.append(self.generate_orders_for_side("sell", fair_price) if is_sell else asyncio.sleep(0))

            buy_orders, sell_orders = await asyncio.gather(*tasks)
            
            if not isinstance(buy_orders, list):
                buy_orders = []
            if not isinstance(sell_orders, list):
                sell_orders = []

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
            adj_price = fair_price * (1 - spread) if is_buy else fair_price * (1 + spread)
            price_step = max(spread, self.params["min_spread"])

            size_multiplier = self.params["size_multiplier"]
            max_trade = self.inventory["max_trade_value"]
            min_trade = self.inventory["min_trade_value"]

            tick_size = self.tick_size
            price_scale = self.price_scale
            qty_scale = self.qty_scale
            trade_base_size = self.trade_base_size
            
            for i in range(self.params["order_count"]):
                factor = 1.0 + i * price_step

                if is_buy:
                    raw_price = adj_price / factor
                    price = min(raw_price, raw_price - tick_size / 2)
                else:
                    raw_price = adj_price * factor
                    price = max(raw_price, raw_price + tick_size / 2)

                price = round(price, price_scale)

                if i == 0:
                    if is_buy:
                        self.bid = price
                    else:
                        self.ask = price

                size_base =  trade_base_size * (size_multiplier ** i)
                size_base = clamp(size_base, min_trade / price, max_trade / price)
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
        # 1. Refresh if too much time has passed
        time_since_last_order = current_time - self.last_order_time
        if time_since_last_order > self.params["refresh_time"]:
            return True

        # 2. Refresh if fair price has moved significantly
        if self.old_fair_price:
            fair_price = await self.compute_fair_price()
            price_change = abs(fair_price - self.old_fair_price) / self.old_fair_price

            # Dynamic threshold based on spread
            price_change_threshold = self.params['min_spread']
            if price_change >= price_change_threshold:
                return True
                
        return False
    
    async def handle_position_management(self):
        """
        Determine if the bot should manage (i.e., exit) the current position for linear contracts.

        Returns:
            bool: True if take-profit or stop-loss condition is met; otherwise False.
        """
        if self.category != "linear":
            return False

        # Get first position (assumes single-position system)
        position = self.position_data[0]
        if not position:
            return False

        try:
            side = position.get("side", "").lower()
            curr_position = float(position.get("positionValue", 0) or 0)
            unrlzd_pnl = float(position.get("unrealisedPnl", 0) or 0)
            side_ = 1 if side == "buy" else -1

            self.unrlzd_pnl = unrlzd_pnl
            self.nominal_value = curr_position * side_

        except (ValueError, TypeError):
            self.logger.warning("Position data malformed.")
            return False

        # Take profit or stop loss check
        if unrlzd_pnl >= float(self.risk["take_profit_value"]):
            self.logger.info(f"Take profit triggered: {unrlzd_pnl}")
            return True

        elif unrlzd_pnl <= -float(self.risk["stop_loss_value"]):
            self.logger.info(f"Stop loss triggered: {unrlzd_pnl}")
            return True

        # Adjust direction based on position size
        over_limit = abs(curr_position) >= float(self.risk["max_position_value"])
        if over_limit:
            if side == "buy":
                self.is_buy = False
                self.is_sell = True
                
            elif side == "sell":
                self.is_sell = False
                self.is_buy = True

        else:
            # Reset both directions to allowed
            self.is_buy = True
            self.is_sell = True

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

    async def _pnl_cron_job(self, interval: float = 10.0):
        """Periodically fetch and update realized PnL."""
        while True:
            try:
                await self.calculate_pnl()
            except Exception as e:
                import traceback
                tb = traceback.extract_tb(e.__traceback__)
                self.logger.error(f"[CRON_PNL] Exception: {e} at line {tb[-1].lineno}")
            await asyncio.sleep(interval)

    async def calculate_pnl(self):
        """Fetch trade history and update cumulative PnL."""
        all_trades = []
        cursor = None

        try:
            while True:
                params = {"limit": 100, "startTime": self.scan_time}
                if cursor:
                    params["cursor"] = cursor

                response = await self.rest_client.get_pnl(self.category, self.symbol_pair, **params)
                trades = response.get("list", [])
                if not trades:
                    break

                all_trades.extend(trades)
                cursor = response.get("nextPageCursor")
                if not cursor:
                    break
            
            if all_trades:
                df = pd.DataFrame(all_trades)
                df["closedPnl"] = pd.to_numeric(df["closedPnl"], errors="coerce").fillna(0)
                total_pnl = df["closedPnl"].sum()
                self.cum_rlzd_pnl += total_pnl
                self.scan_time = int(df["updatedTime"].max())
                self.logger.info(f"[PNL] Updated: {total_pnl} | Cumulative: {self.cum_rlzd_pnl}" )

            else:
                self.logger.info("[PNL] No trades found.")

        except Exception as e:
            import traceback
            tb = traceback.extract_tb(e.__traceback__)
            self.logger.error(f"[PNL] Error: {e} at line {tb[-1].lineno}")


            

