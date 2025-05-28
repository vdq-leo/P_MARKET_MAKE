#!/usr/bin/env python3
"""
Test Bybit Demo API Connection
"""
import asyncio
import logging
import os
import sys
import uvloop
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the Bybit clients
from QUANT.async_pmm.bybit_rest import BybitRestClient
from QUANT.async_pmm.bybit_websocket import BybitWebSocketClient

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
        # Test server time
        server_time = await client.get_server_time()
        logger.info(f"Server time: {server_time}")
        
        # Test get symbols
        symbols = await client.get_symbols("linear")
        logger.info(f"Got {len(symbols)} linear symbols")
        
        # Test get ticker
        ticker = await client.get_ticker("linear", "HYPEUSDT")
        logger.info(f"ticker: {ticker}")
        
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

    #     # Test place order
    #     try:
    #         result = await client.place_order(
    #             category="linear",
    #             symbol="HYPEUSDT",
    #             side="Sell",
    #             order_type="Limit",
    #             qty=1,
    #             price=100,
    #             time_in_force="GTC"
    #         )
    #         logger.info(f"Order placed: {result}")
    #     except Exception as e:
    #         logger.error(f"Error placing order: {e} {e.__traceback__.tb_lineno}")
        
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
    client = BybitWebSocketClient(api_key, api_secret)
    
    try:
        # Register callbacks
        client.register_ticker_callback("HYPEUSDT", on_ticker)
        client.register_connection_callback(on_connection)
        
        # Connect to WebSocket
        await client.connect()
        
        # Subscribe to ticker
        await client.subscribe_ticker("HYPEUSDT")
        
        # Wait for some updates
        logger.info("Waiting for WebSocket updates (10 seconds)...")
        await asyncio.sleep(10)
        
        # # Register callbacks
        # client.register_position_callback(on_position)
        # client.register_connection_callback(on_connection)
        
        # # Connect to WebSocket
        # await client.connect()
        
        # # Subscribe to ticker
        # await client.subscribe_positions()
        
        # # Wait for some updates
        # logger.info("Waiting for WebSocket updates (10 seconds)...")
        # await asyncio.sleep(10)

        # logger.info("WebSocket tests completed")
        
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
    # await test_rest_api(api_key, api_secret)
    
    # Test WebSocket
    await test_websocket(api_key, api_secret)

if __name__ == "__main__":
    asyncio.run(main())