"""
PMM Application - Main application for running the PMM strategy
"""
import asyncio
import signal
import time
import uvloop
import sys
from typing import Dict, Any, Optional, List

from logger import setup_logger_global
from .pmm_strategy import PMMStrategy
from .bybit_rest import BybitRestClient
from .bybit_websocket import BybitWebSocketClient
from .order_manager import OrderManager

# Set uvloop as the event loop policy
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

class PMMApplication:
    """
    Price Market Making Application
    Coordinates strategy, order management, and exchange communication
    """
    
    def __init__(self, symbol: str, quote: str = "USDT", category: str = "linear", 
                 testnet: bool = True, strategy_params: Optional[Dict[str, Any]] = None):
        """
        Initialize the PMM application
        
        Args:
            symbol: Base symbol (e.g., 'BTC')
            quote: Quote currency (e.g., 'USDT')
            category: Product category (spot, linear, inverse)
            testnet: Whether to use testnet (Note: For Bybit demo, set to False as we use demo API directly)
            strategy_params: Optional strategy parameters
        """
        self.symbol = symbol
        self.quote = quote
        self.symbol_pair = f"{symbol}{quote}"
        self.category = category
        self.testnet = testnet
        
        # Setup logging
        self.logger = setup_logger_global('PMM_APP',f"pmm_app_{self.symbol_pair}")
        
        # Initialize components
        self.strategy = PMMStrategy(symbol, quote, strategy_params)
        self.rest_client = None
        self.ws_client = None
        self.order_manager = None
        
        # Application state
        self.running = False
        self.stop_event = asyncio.Event()
        self.last_order_time = 0
        self.last_refresh_check = 0
        self.refresh_interval = 1.0  # Check for refreshes every second
        
        self.logger.info(f"Initialized PMM application for {self.symbol_pair}")
    
    async def start(self, api_key: str, api_secret: str):
        """
        Start the PMM application
        
        Args:
            api_key: API key
            api_secret: API secret
        """
        try:
            self.logger.info(f"Starting PMM application for {self.symbol_pair}")
            
            # Initialize clients
            self.rest_client = BybitRestClient(api_key, api_secret, self.testnet)
            self.ws_client = BybitWebSocketClient(api_key, api_secret, self.testnet)

            if self.rest_client is None or self.ws_client is None:
                raise Exception("Failed to initialize clients")
            
            # Initialize strategy
            self.strategy.set_clients(self.rest_client, self.ws_client)
            await self.strategy.set_scale()

            # Initialize order manager
            self.order_manager = OrderManager(self.rest_client, self.ws_client, self.symbol_pair, self.category)
            
            # Connect to WebSocket
            await self.ws_client.connect()
            
            # Subscribe to market data
            await self.ws_client.subscribe_ticker(self.symbol_pair)
            await self.ws_client.subscribe_kline(self.symbol_pair, "1")
            # await self.ws_client.subscribe_orderbook(self.symbol_pair, "50")
            # await self.ws_client.subscribe_positions(self.symbol_pair, "50")
            
            # Register callbacks
            self.ws_client.register_ticker_callback(self.symbol_pair, self._handle_ticker_update)
            self.ws_client.register_kline_callback(self.symbol_pair, "1", self._handle_kline_update)
            # self.ws_client.register_orderbook_callback(self.symbol_pair, self._handle_orderbook_update) # Not support yet
            # self.ws_client.register_position_callback(self.symbol_pair, "1", self._handle_position_update) # Not need yet
                        
            # Start order manager
            await self.order_manager.start()
            
            # Fetch initial market data
            await self._fetch_initial_data()
            
            # Start main loop
            self.running = True
            asyncio.create_task(self.strategy._pnl_cron_job(interval=10.0))
            self.position_task = asyncio.create_task(self._poll_position_info(interval=1.0), name="position_task")
            self.strategy_task = asyncio.create_task(self._run_strategy_loop(), name="strategy_task")
            
        except Exception as e:
            self.logger.error(f"Error starting PMM application: {e} {e.__traceback__.tb_lineno}")
            await self.stop()
        
    async def stop(self):
        """Stop the PMM application."""
        try:
            self.logger.info("Stopping PMM application...")
            self.running = False
            self.stop_event.set()  # Signal loops to stop

            # Attempt to close any open position
            try:
                await self.close_position()
            except Exception as e:
                self.logger.error(f"Error closing position: {e}")

            # Cancel background tasks
            background_tasks = [
                getattr(self, 'websocket_task', None),
                getattr(self, 'order_refresh_task', None),
                getattr(self, 'risk_task', None),
                getattr(self, 'position_task', None),
                getattr(self, 'strategy_task', None),
            ]

            for task in background_tasks:
                if task:
                    task.cancel()

            # Await task cancellation
            for task in background_tasks:
                if task:
                    try:
                        await task
                    except asyncio.CancelledError:
                        task_name = getattr(task, 'get_name', lambda: 'Unnamed Task')()
                        self.logger.info(f"Task {task_name} cancelled")

            # Clean up order manager
            if self.order_manager:
                try:
                    await self.order_manager.cancel_all_orders()
                    await self.order_manager.stop()
                except Exception as e:
                    self.logger.error(f"Error stopping order manager: {e}")

            # Close WebSocket client
            if self.ws_client:
                try:
                    await self.ws_client.close()
                except Exception as e:
                    self.logger.error(f"Error closing WebSocket client: {e}")

            # Close REST client
            if self.rest_client:
                try:
                    await self.rest_client.close()
                except Exception as e:
                    self.logger.error(f"Error closing REST client: {e}")

            self.logger.info("PMM application stopped successfully.")

        except Exception as e:
            self.logger.error(f"Unexpected error while stopping PMM application: {e}")

    async def _poll_position_info(self, interval: float = 1.0):
        """
        Polls position info periodically (default: every 1 second)
        """
        while True:
            try:
                position_info = await self.rest_client.get_positions(self.category, self.symbol_pair)
                if position_info:
                    await self._handle_position_update(position_info)
            except Exception as e:
                self.logger.error(f"Error in position polling loop: {e}")
            await asyncio.sleep(interval)

    async def _fetch_initial_data(self):
        """Fetch initial market data"""
        try:
            # Fetch ticker
            ticker = await self.rest_client.get_ticker(self.category, self.symbol_pair)
            await self._handle_ticker_update(ticker)
            
            # Fetch klines
            klines = await self.rest_client.get_klines(
                self.category, 
                self.symbol_pair, 
                "1", 
                limit=100
            )
            await self._handle_kline_update(klines)
            
            self.logger.info("Initial market data fetched")
            
        except Exception as e:
            self.logger.error(f"Error fetching initial data: {e} {e.__traceback__.tb_lineno}")
        
    async def _handle_ticker_update(self, ticker_data: Dict[str, Any]):
        """
        Handle ticker updates
        
        Args:
            ticker_data: Ticker data
        """
        await self.strategy.update_market_data(ticker=ticker_data)
    
    async def _handle_kline_update(self, kline_data: List[Dict[str, Any]]):
        """
        Handle kline updates
        
        Args:
            kline_data: Kline data
        """
        await self.strategy.update_market_data(klines=kline_data)
    
    async def _handle_orderbook_update(self, orderbook_data: Dict[str, Any]):
        """
        Handle orderbook updates
        
        Args:
            orderbook_data: Orderbook data
        """
        await self.strategy.update_market_data(order_book=orderbook_data)

    async def _handle_position_update(self, position: Dict[str, Any]):
        """
        Handle orderbook updates
        
        Args:
            orderbook_data: Orderbook data
        """
        await self.strategy.update_market_data(position=position)
    
    async def _run_strategy_loop(self):
        """Main strategy loop"""
        self.logger.info("Starting strategy")

        asyncio.create_task(self._log_params(interval = 0.2))
        asyncio.create_task(self._handle_position_risk(interval = 1))

        while self.running and not self.stop_event.is_set():
            try:
                # Check if we should refresh orders
                current_time = time.time()
                if current_time - self.last_refresh_check >= self.refresh_interval:
                    self.last_refresh_check = current_time
  
                    # Check if orders need refreshing
                    if await self.strategy.should_refresh_orders(current_time):
                        await self._refresh_orders()
                    
                # Small sleep to prevent CPU spinning
                await asyncio.sleep(0.05)
                
            except Exception as e:
                self.logger.error(f"Error in strategy loop: {e} {e.__traceback__.tb_lineno}")
                await asyncio.sleep(5)
    
    async def _refresh_orders(self):
        """Refresh orders based on current market conditions"""
        try:
            # Cancel existing orders
            # Fire-and-forget cancel without awaiting
            asyncio.create_task(self.order_manager.cancel_all_orders())
            
            # Generate new orders
            orders = await self.strategy.generate_orders()
            
            # Create task lists
            buy_tasks = [
                self.order_manager.place_order(
                    side="Buy",
                    order_type="Limit",
                    qty=order["quantity"],
                    price=order["price"],
                    time_in_force="GTC"
                ) for order in orders.get("buy", [])
            ]

            sell_tasks = [
                self.order_manager.place_order(
                    side="Sell",
                    order_type="Limit",
                    qty=order["quantity"],
                    price=order["price"],
                    time_in_force="GTC"
                ) for order in orders.get("sell", [])
            ]

            # Run all place_order tasks concurrently
            await asyncio.gather(*(buy_tasks + sell_tasks))
            
            # Update last order time
            self.last_order_time = time.time()
            self.strategy.last_order_time = self.last_order_time
            
            # Update old fair price
            self.strategy.old_fair_price = self.strategy.fair_price
            
            # self.logger.info(f"Refreshed orders: {len(orders['buy'])} buys, {len(orders['sell'])} sells")
            
        except Exception as e:
            self.logger.error(f"Error refreshing orders: {e} {e.__traceback__.tb_lineno}")

    async def _log_params(self, interval: float = 0.2):
        """Ultra-fast terminal update for strategy parameters."""
        # ANSI color codes for styling
        GREEN = "\033[92m"
        YELLOW = "\033[93m"
        CYAN = "\033[96m"
        RED = "\033[91m"
        RESET = "\033[0m"
        BOLD = "\033[1m"

        def fmt_num(val, fmt, default_color=RESET):
            if val is None:
                return f"{RED}N/A{RESET}"
            color = RED if val < 0 else default_color
            return f"{color}{fmt.format(val)}{RESET}"


        await asyncio.sleep(1)  # initial delay

        while True:
            try:
                spread = float(self.strategy.spread) if self.strategy.spread is not None else None
                skew = float(self.strategy.skew) if self.strategy.skew is not None else None
                vol = float(self.strategy.volatility) if self.strategy.volatility is not None else None
                position_raw = self.strategy.position_normalize
                position = position_raw if position_raw is not None else None
                cum_rlzd_pnl = float(self.strategy.cum_rlzd_pnl) if self.strategy.cum_rlzd_pnl is not None else None
                unrlzd_pnl = float(self.strategy.unrlzd_pnl) if self.strategy.unrlzd_pnl is not None else None
                fair_price = float(self.strategy.fair_price) if self.strategy.fair_price is not None else None
                bid_price = float(self.strategy.bid) if self.strategy.bid is not None else None
                ask_price = float(self.strategy.ask) if self.strategy.ask is not None else None
                nominal_value = float(self.strategy.nominal_value) if self.strategy.nominal_value is not None else None

                output = "\033[2J\033[H"  # Clear screen + move cursor to top-left
                output += f"{BOLD}=== Strategy State ==={RESET}\n\n"
                
                output += f"Ask Price      : {fmt_num(ask_price, '{:.5f}', RED)}\n"
                output += f"Fair Price     : {fmt_num(fair_price, '{:.5f}', CYAN)}\n"
                output += f"Bid Price      : {fmt_num(bid_price, '{:.5f}', GREEN)}\n\n"

                output += f"Volatility     : {fmt_num((vol * 100) if vol is not None else None, '{:.4f} %', YELLOW)}\n"
                output += f"Spread         : {fmt_num((spread * 100) if spread is not None else None, '{:.4f} %', YELLOW)}\n"
                output += f"Skew           : {fmt_num((skew * 100) if skew is not None else None, '{:.4f} %', YELLOW)}\n\n"

                output += f"Position       : {fmt_num(position, '{:d}', CYAN)}\n"
                output += f"Nominal Value  : {fmt_num(nominal_value, '{:.2f}', CYAN)}\n"
                output += f"Unrealized PNL : {fmt_num(unrlzd_pnl, '{:.4f}', GREEN)}\n"
                output += f"Realized PNL   : {fmt_num(cum_rlzd_pnl, '{:.4f}', GREEN)}\n"

                sys.stdout.write(output)
                sys.stdout.flush()

            except Exception as e:
                sys.stdout.write(f"\nError: {e}\n")
                sys.stdout.flush()

            await asyncio.sleep(interval)

    async def _handle_position_risk(self, interval: float = 5.0):
        """Continuously monitor position and risk."""
        self.logger.info('Warm-up: 5 seconds')
        await asyncio.sleep(5)

        while True:
            try:
                if await self.strategy.handle_position_management():
                    await self.close_position()
                    self.logger.info("Position management condition met. Closed position.")
                    return

                if await self.strategy.handle_risk_management():
                    await self.stop()
                    self.logger.info("Risk limit breached. Strategy stopped.")
                    return

            except Exception as e:
                self.logger.error(f"Error handling position risk: {e}")

            await asyncio.sleep(interval)

    async def close_position(self):
        """
        Closes the current open position (only for linear contracts).
        """
        if self.category != "linear":
            return 

        try:
            for attempt in range(4):
                positions = await self.rest_client.get_positions(self.category, self.symbol_pair)
                if not positions:
                    # No positions left, done
                    return

                position = positions[0]
                side = position["side"].lower()
                curr_size = float(position.get('size', 0))

                if curr_size == 0:
                    # Position already closed
                    return

                opposite_side = "Sell" if side == "buy" else "Buy"
                await self.order_manager.place_order(
                    side=opposite_side,
                    order_type="Market",
                    qty=curr_size,
                    time_in_force="GTC",
                    reduce_only=True
                )
                # self.logger.info(f"Attempt {attempt+1}: Placed market order to close {side} position of size {curr_size}")

                # Wait a short period to allow the order to fill and position to update
                await asyncio.sleep(1)

            self.logger.warning("close_position: Reached max retry attempts but position might still be open.")

        except Exception as e:
            self.logger.error(f"Error closing position: {e}")


async def run_pmm(api_key: str, api_secret: str, symbol: str = "HYPE", quote: str = "USDT",
                 category: str = "linear", testnet: bool = True, 
                 strategy_params: Optional[Dict[str, Any]] = None):
    """
    Run the PMM application
    
    Args:
        api_key: API key
        api_secret: API secret
        symbol: Base symbol (e.g., 'BTC')
        quote: Quote currency (e.g., 'USDT')
        category: Product category (spot, linear, inverse)
        testnet: Whether to use testnet
        strategy_params: Optional strategy parameters
    """
    
    # Create application
    app = PMMApplication(symbol, quote, category, testnet, strategy_params)
    
    # Setup signal handlers
    loop = asyncio.get_running_loop()
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(app.stop()))
    
    try:
        # Start application
        await app.start(api_key, api_secret)
        
        # Wait until stopped
        while app.running:
            await asyncio.sleep(1)
            
    except Exception as e:
        print(f"Error running PMM application: {e} {e.__traceback__.tb_lineno}")
        
    finally:
        # Ensure application is stopped
        await app.stop()

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run PMM strategy")
    parser.add_argument("--api-key", required=True, help="API key")
    parser.add_argument("--api-secret", required=True, help="API secret")
    parser.add_argument("--symbol", default="BTC", help="Base symbol (e.g., 'BTC')")
    parser.add_argument("--quote", default="USDT", help="Quote currency (e.g., 'USDT')")
    parser.add_argument("--category", default="linear", help="Product category (spot, linear, inverse)")
    parser.add_argument("--testnet", action="store_true", help="Use testnet (not needed for Bybit demo)")
    
    args = parser.parse_args()
    
    # Run with uvloop
    uvloop.install()
    asyncio.run(run_pmm(
        api_key=args.api_key,
        api_secret=args.api_secret,
        symbol=args.symbol,
        quote=args.quote,
        category=args.category,
        testnet=args.testnet
    ))


if __name__ == "__main__":
    main()