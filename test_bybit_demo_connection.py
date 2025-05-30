#!/usr/bin/env python3
"""
Test Bybit Demo API Connection
"""
import asyncio
import logging
import os
import sys
import uvloop
import csv
from dotenv import load_dotenv
from typing import Dict, Any

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the Bybit clients
from async_pmm.bybit_rest import BybitRestClient
from async_pmm.bybit_websocket import BybitWebSocketClient

# Load environment variables from .env file
load_dotenv()

# Set uvloop as the event loop policy
uvloop.install()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger("bybit_demo_test")

async def test_rest_api(api_key, api_secret):
    """Test REST API connection"""
    logger.info("Testing REST API connection...")
    client = BybitRestClient(api_key, api_secret)
    
    try:
        # # Test server time
        # server_time = await client.get_server_time()
        # logger.info(f"Server time: {server_time}")
        
        # # Test get symbols
        # symbols = await client.get_symbols("linear")
        # logger.info(f"Got {len(symbols)} linear symbols")
        
        # # Test get ticker
        # ticker = await client.get_ticker("linear", "HYPEUSDT")
        # logger.info(f"ticker: {ticker}")
        
        # Test get positions
        positions = await client.get_positions("linear", "HYPEUSDT")
        logger.info(f"Positions: {positions}")

        # # Test wallet balance
        # try:
        #     balance = await client.get_wallet_balance()
        #     logger.info(f"Wallet balance: {balance}")
        # except Exception as e:
        #     logger.error(f"Error getting wallet balance: {e} {e.__traceback__.tb_lineno}")
        
        # # Test request demo funds
        # try:
        #     result = await client.request_demo_funds()
        #     logger.info(f"Demo funds request result: {result}")
        # except Exception as e:
        #     logger.error(f"Error requesting demo funds: {e} {e.__traceback__.tb_lineno}")
        
        # logger.info("REST API tests completed")

        # Test place order
        # try:
        #     result = await client.place_order(
        #         category="linear",
        #         symbol="HYPEUSDT",
        #         side="Sell",
        #         order_type="Limit",
        #         qty=1,
        #         price=100,
        #         time_in_force="GTC"
        #     )
        #     logger.info(f"Order placed: {result}")
        # except Exception as e:
        #     logger.error(f"Error placing order: {e} {e.__traceback__.tb_lineno}")

    except Exception as e:
        logger.error(f"Error testing REST API: {e} {e.__traceback__.tb_lineno}")
    finally:
        await client.close()

async def on_ticker(data):
    """Callback for ticker updates"""
    logger.info(f"Ticker update: {data}")

async def on_position(data):
    """Callback for position updates"""
    logger.info(f"Position update: {data}")

async def on_connection(connected):
    """Callback for connection status"""
    logger.info(f"WebSocket {'connected' if connected else 'disconnected'}")

async def test_websocket(api_key, api_secret):
    """Test WebSocket connection"""
    logger.info("Testing WebSocket connection...")
    client = BybitWebSocketClient(api_key, api_secret, testnet=True)
    
    try:
        # Connect to WebSocket
        await client.connect()

        # Register callbacks
        client.register_ticker_callback("HYPEUSDT", on_ticker)
        client.register_connection_callback(on_connection)
        
        # Subscribe to ticker
        # await client.subscribe_ticker("HYPEUSDT")
        
        # Register callbacks
        client.register_position_callback(on_position)
        
        # Subscribe to position
        await client.subscribe_positions()
        




        # Wait for some updates
        logger.info("Waiting for WebSocket updates (5 seconds)...")
        await asyncio.sleep(5)

        logger.info("WebSocket tests completed")
        
    except Exception as e:
        logger.error(f"Error testing WebSocket: {e} {e.__traceback__.tb_lineno}")
    finally:
        await client.close()

async def main():
    """Main entry point"""
    # Get API credentials from environment variables
    api_key = os.getenv("BYBIT_API_KEY")
    api_secret = os.getenv("BYBIT_API_SECRET")
    
    if not api_key or not api_secret:
        logger.error("API credentials not found. Please set BYBIT_API_KEY and BYBIT_API_SECRET environment variables.")
        return
    
    # Test REST API
    await test_rest_api(api_key, api_secret)
    
    # Test WebSocket
    # await test_websocket(api_key, api_secret)


async def fetch_all_trade_history_to_csv(category: str, symbol: str, filename: str, limit: int = 100):
    api_key = os.getenv("BYBIT_API_KEY")
    api_secret = os.getenv("BYBIT_API_SECRET")

    client = BybitRestClient(api_key, api_secret)

    all_trades = []
    cursor = None

    try:
        while True:
            trade_hist_params: Dict[str, Any] = {"limit": limit}
            if cursor:
                trade_hist_params["cursor"] = cursor

            response = await client.get_trade_history(category, symbol, **trade_hist_params)

            if not response or "list" not in response:
                logger.warning("No data in response or unexpected format.")
                break

            trades = response["list"]
            all_trades.extend(trades)
            logger.info(f"Fetched {len(trades)} trades, total so far: {len(all_trades)}")

            cursor = response.get("nextPageCursor")
            if not cursor:
                break

        # Save to CSV
        if all_trades:
            keys = all_trades[0].keys()
            with open(filename, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(all_trades)

            logger.info(f"Saved {len(all_trades)} trades to {filename}")
        else:
            logger.info("No trades found to save.")

    except Exception as e:
        import traceback
        tb = traceback.extract_tb(e.__traceback__)
        logger.error(f"Error getting trade history: {e} at line {tb[-1].lineno}")

async def fetch_all_pnl_to_csv(category: str, symbol: str, filename: str, limit: int = 100):
    api_key = os.getenv("BYBIT_API_KEY")
    api_secret = os.getenv("BYBIT_API_SECRET")
    client = BybitRestClient(api_key, api_secret)

    all_trades = []
    cursor = None

    try:
        while True:
            trade_hist_params: Dict[str, Any] = {"limit": limit}
            if cursor:
                trade_hist_params["cursor"] = cursor

            response = await client.get_pnl(category, symbol, **trade_hist_params)

            if not response or "list" not in response:
                logger.warning("No data in response or unexpected format.")
                break

            trades = response["list"]
            all_trades.extend(trades)
            logger.info(f"Fetched {len(trades)} trades, total so far: {len(all_trades)}")

            cursor = response.get("nextPageCursor")
            if not cursor:
                break

        # Save to CSV
        if all_trades:
            keys = all_trades[0].keys()
            with open(filename, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(all_trades)

            logger.info(f"Saved {len(all_trades)} trades to {filename}")
        else:
            logger.info("No trades found to save.")

    except Exception as e:
        import traceback
        tb = traceback.extract_tb(e.__traceback__)
        logger.error(f"Error getting trade history: {e} at line {tb[-1].lineno}")


if __name__ == "__main__":
    asyncio.run(fetch_all_pnl_to_csv("linear", "HYPEUSDT", "pnl_history.csv", 100))
    # asyncio.run(fetch_all_trade_history_to_csv("linear", "HYPEUSDT", "trade_history.csv", 100))
    # asyncio.run(main())