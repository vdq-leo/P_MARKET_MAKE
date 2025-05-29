"""
Async Order Manager - Handles order placement, tracking, and execution
"""
import asyncio
import logging
import time
import uuid
from typing import Dict, List, Any, Optional, Callable, Set, Tuple
from datetime import datetime

from .bybit_rest import BybitRestClient
from .bybit_websocket import BybitWebSocketClient

class OrderManager:
    """
    Asynchronous Order Manager for handling order placement, tracking, and execution
    """
    
    def __init__(self, rest_client: BybitRestClient, ws_client: BybitWebSocketClient, 
                 symbol: str, category: str = "spot"):
        """
        Initialize the Order Manager
        
        Args:
            rest_client: Bybit REST API client
            ws_client: Bybit WebSocket client
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            category: Product category (spot, linear, inverse)
        """
        self.rest_client = rest_client
        self.ws_client = ws_client
        self.symbol = symbol
        self.category = category
        
        # Order tracking
        self.active_orders: Dict[str, Dict[str, Any]] = {}
        self.order_queue = asyncio.Queue()
        self.cancel_queue = asyncio.Queue()
        self.order_updates_queue = asyncio.Queue()
        
        # Order processing flags
        self.processing = False
        self.stop_flag = False
        
        # Order callbacks
        self.order_update_callbacks: List[Callable[[Dict[str, Any]], None]] = []
        self.trade_callbacks: List[Callable[[Dict[str, Any]], None]] = []
        
        # Setup logging
        self.logger = logging.getLogger(f"order_manager_{symbol}")
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        # Prevent logs from being passed to the root logger
        self.logger.propagate = False
        
    async def start(self):
        """Start the order manager"""
        self.processing = True
        self.stop_flag = False
        
        # Register WebSocket callbacks
        self.ws_client.register_order_callback(self._handle_order_update)
        self.ws_client.register_execution_callback(self._handle_execution_update)
        
        # Subscribe to order and execution updates
        await self.ws_client.subscribe_orders()
        await self.ws_client.subscribe_executions()
        
        # Start order processing tasks
        asyncio.create_task(self._process_order_queue())
        asyncio.create_task(self._process_cancel_queue())
        # asyncio.create_task(self._process_order_updates_queue())
        
        # Fetch initial open orders
        # await self._fetch_open_orders()
        
        self.logger.info(f"Order manager started for {self.symbol}")
    
    async def stop(self):
        """Stop the order manager"""
        self.stop_flag = True
        self.processing = False
        self.logger.info("Order manager stopping...")
        
        # Wait for queues to be processed
        await self.order_queue.join()
        await self.cancel_queue.join()
        await self.order_updates_queue.join()
        
        self.logger.info("Order manager stopped")
    
    async def _fetch_open_orders(self):
        """Fetch open orders from the exchange"""
        try:
            orders = await self.rest_client.get_open_orders(self.category, self.symbol)
            
            for order in orders:
                order_id = order.get("orderId")
                if order_id:
                    self.active_orders[order_id] = order
                    
            self.logger.info(f"Fetched {len(orders)} open orders")
            
        except Exception as e:
            self.logger.error(f"Error fetching open orders: {e}")
    
    async def place_order(self, side: str, order_type: str, qty: float, 
                         price: Optional[float] = None, time_in_force: str = "GTC",
                         reduce_only: bool = False, close_on_trigger: bool = False) -> Optional[str]:
        """
        Place an order
        
        Args:
            side: Order side (Buy, Sell)
            order_type: Order type (Limit, Market)
            qty: Order quantity
            price: Order price (required for Limit orders)
            time_in_force: Time in force (GTC, IOC, FOK, PostOnly)
            reduce_only: Whether the order is reduce-only
            close_on_trigger: Whether to close position on trigger
            
        Returns:
            Order ID if successful, None otherwise
        """
        # Generate a unique order link ID
        order_link_id = f"pmm_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
        
        # Format quantity and price
        qty_str = str(qty)
        price_str = str(price) if price is not None else None
        
        # Create order request
        order_request = {
            "category": self.category,
            "symbol": self.symbol,
            "side": side,
            "order_type": order_type,
            "qty": qty_str,
            "price": price_str,
            "time_in_force": time_in_force,
            "reduce_only": reduce_only,
            "close_on_trigger": close_on_trigger,
            "order_link_id": order_link_id
        }
        
        # Add to order queue
        await self.order_queue.put(order_request)
        
        self.logger.info(f"Queued {side} {order_type} order for {qty} @ {price}")
        
        return order_link_id
    
    async def cancel_order(self, order_id: Optional[str] = None, order_link_id: Optional[str] = None):
        """
        Cancel an order
        
        Args:
            order_id: Order ID
            order_link_id: Custom order ID
        """
        if not order_id and not order_link_id:
            self.logger.error("Either order_id or order_link_id must be provided")
            return
            
        # Create cancel request
        cancel_request = {
            "category": self.category,
            "symbol": self.symbol,
            "order_id": order_id,
            "order_link_id": order_link_id
        }
        
        # Add to cancel queue
        await self.cancel_queue.put(cancel_request)
        
        self.logger.info(f"Queued cancel for order: {order_id or order_link_id}")
    
    async def cancel_all_orders(self):
        """Cancel all active orders"""
        try:
            self.logger.info("Cancelling all orders...")
            result = await self.rest_client.cancel_all_orders(self.category, self.symbol)
            
            # Clear active orders
            self.active_orders.clear()
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error cancelling all orders: {e}")
    
    async def _process_order_queue(self):
        """Process orders from the queue"""
        while not self.stop_flag:
            try:
                if self.order_queue.empty():
                    await asyncio.sleep(0.01)
                    continue
                    
                # Get order from queue
                order_request = await self.order_queue.get()
                
                try:
                    # Extract parameters
                    category = order_request.pop("category")
                    symbol = order_request.pop("symbol")
                    side = order_request.pop("side")
                    order_type = order_request.pop("order_type")
                    qty = order_request.pop("qty")
                    price = order_request.pop("price")
                    time_in_force = order_request.pop("time_in_force")
                    reduce_only = order_request.pop("reduce_only")
                    close_on_trigger = order_request.pop("close_on_trigger")
                    order_link_id = order_request.pop("order_link_id")
                    
                    # Place order
                    result = await self.rest_client.place_order(
                        category=category,
                        symbol=symbol,
                        side=side,
                        order_type=order_type,
                        qty=qty,
                        price=price,
                        time_in_force=time_in_force,
                        reduce_only=reduce_only,
                        close_on_trigger=close_on_trigger,
                        order_link_id=order_link_id
                    )
                    
                    # Track order
                    order_id = result.get("orderId")
                    # if order_id:
                    #     self.active_orders[order_id] = {
                    #         "orderId": order_id,
                    #         "orderLinkId": order_link_id,
                    #         "symbol": symbol,
                    #         "side": side,
                    #         "orderType": order_type,
                    #         "price": price,
                    #         "qty": qty,
                    #         "timeInForce": time_in_force,
                    #         "orderStatus": "New",
                    #         "createdTime": int(time.time() * 1000),
                    #         "updatedTime": int(time.time() * 1000)
                    #     }
                        
                    self.logger.info(f"Placed order: {order_id} ({side} {qty} @ {price})")
                    
                except Exception as e:
                    self.logger.error(f"Error placing order: {e}")
                    
                finally:
                    # Mark task as done
                    self.order_queue.task_done()
                    
            except Exception as e:
                self.logger.error(f"Error in order queue processing: {e}")
                await asyncio.sleep(1)
    
    async def _process_cancel_queue(self):
        """Process cancellations from the queue"""
        while not self.stop_flag:
            try:
                if self.cancel_queue.empty():
                    await asyncio.sleep(0.01)
                    continue
                    
                # Get cancel request from queue
                cancel_request = await self.cancel_queue.get()
                
                try:
                    # Extract parameters
                    category = cancel_request.pop("category")
                    symbol = cancel_request.pop("symbol")
                    order_id = cancel_request.pop("order_id", None)
                    order_link_id = cancel_request.pop("order_link_id", None)
                    
                    # Cancel order
                    result = await self.rest_client.cancel_order(
                        category=category,
                        symbol=symbol,
                        order_id=order_id,
                        order_link_id=order_link_id
                    )
                    
                    # Remove from active orders if successful
                    if order_id and order_id in self.active_orders:
                        del self.active_orders[order_id]
                        
                    self.logger.info(f"Cancelled order: {order_id or order_link_id}")
                    
                except Exception as e:
                    self.logger.error(f"Error cancelling order: {e}")
                    
                finally:
                    # Mark task as done
                    self.cancel_queue.task_done()
                    
            except Exception as e:
                self.logger.error(f"Error in cancel queue processing: {e}")
                await asyncio.sleep(1)
    
    async def _process_order_updates_queue(self):
        """Process order updates from the queue"""
        while not self.stop_flag:
            try:
                if self.order_updates_queue.empty():
                    await asyncio.sleep(0.01)
                    continue
                    
                # Get update from queue
                update = await self.order_updates_queue.get()
                
                try:
                    # Process update
                    order_id = update.get("orderId")
                    order_status = update.get("orderStatus")
                    
                    # Update active orders
                    if order_id:
                        if order_status in ["Filled", "Cancelled", "Rejected"]:
                            # Remove completed orders
                            if order_id in self.active_orders:
                                del self.active_orders[order_id]
                        else:
                            # Update active order
                            self.active_orders[order_id] = update
                    
                    # Notify callbacks
                    for callback in self.order_update_callbacks:
                        try:
                            callback(update)
                        except Exception as e:
                            self.logger.error(f"Error in order update callback: {e}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing order update: {e}")
                    
                finally:
                    # Mark task as done
                    self.order_updates_queue.task_done()
                    
            except Exception as e:
                self.logger.error(f"Error in order updates queue processing: {e}")
                await asyncio.sleep(1)
    
    async def _handle_order_update(self, updates: List[Dict[str, Any]]):
        """
        Handle order updates from WebSocket
        
        Args:
            updates: List of order updates
        """
        for update in updates:
            # Only process updates for our symbol
            if update.get("symbol") == self.symbol:
                await self.order_updates_queue.put(update)
    
    async def _handle_execution_update(self, updates: List[Dict[str, Any]]):
        """
        Handle execution updates from WebSocket
        
        Args:
            updates: List of execution updates
        """
        for update in updates:
            # Only process updates for our symbol
            if update.get("symbol") == self.symbol:
                # Notify trade callbacks
                for callback in self.trade_callbacks:
                    try:
                        callback(update)
                    except Exception as e:
                        self.logger.error(f"Error in trade callback: {e}")
    
    def register_order_update_callback(self, callback: Callable[[Dict[str, Any]], None]):
        """
        Register callback for order updates
        
        Args:
            callback: Callback function
        """
        self.order_update_callbacks.append(callback)
    
    def register_trade_callback(self, callback: Callable[[Dict[str, Any]], None]):
        """
        Register callback for trade executions
        
        Args:
            callback: Callback function
        """
        self.trade_callbacks.append(callback)
    
    def get_active_orders(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all active orders
        
        Returns:
            Dictionary of active orders by order ID
        """
        return self.active_orders.copy()
    
    def get_order_count(self) -> int:
        """
        Get number of active orders
        
        Returns:
            Number of active orders
        """
        return len(self.active_orders)
    
    def get_order_sides(self) -> Tuple[int, int]:
        """
        Get count of buy and sell orders
        
        Returns:
            Tuple of (buy_count, sell_count)
        """
        buy_count = sum(1 for order in self.active_orders.values() if order.get("side") == "Buy")
        sell_count = sum(1 for order in self.active_orders.values() if order.get("side") == "Sell")
        return buy_count, sell_count