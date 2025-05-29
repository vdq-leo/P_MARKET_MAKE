#!/usr/bin/env python3
"""
Run Async PMM Strategy - Example runner script
"""
import asyncio
import logging
import os
import sys
import uvloop
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the PMM package
from async_pmm import run_pmm

# Load environment variables from .env file
load_dotenv()

# Set uvloop as the event loop policy
uvloop.install()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Strategy parameters
STRATEGY_PARAMS = {
    # Order parameters
    "use_imbalance_price": True,   # Use imbalance price for order placement
    "order_count": 1,           # Number of orders on each side
    "base_spread": 0.0003,
    "min_spread": 0.0001,       # Minimum spread 
    "max_spread": 0.0005,       # Maximum spread 
    "vol_scale": 1,           # Volatility impact on spread
    "skew_factor": 1,           # Inventory skew impact
    "refresh_time": 1,          # Order refresh time in seconds
    
    # Volatility parameters
    "use_dynamic_vol": True,    # Use dynamic volatility
    "vol_window": 5,           # Window for volatility calculation
    
    # Order distribution
    "size_multiplier": 2,     # Size multiplier for orders further from mid

    # Trade parameters
    "base_trade_size": 50.0,
    "min_trade_size": 10.0,     # Minimum trade size in quote currency
    "max_trade_size": 100.0,    # Maximum trade size in quote currency
}

async def request_demo_funds(api_key, api_secret):
    """Request demo funds before starting the strategy"""
    from async_pmm.bybit_rest import BybitRestClient
    
    print("Requesting demo funds...")
    client = BybitRestClient(api_key, api_secret)
    
    try:
        # Request funds for trading
        result = await client.request_demo_funds([
            {"coin": "USDT", "amountStr": "50000"},
            {"coin": "BTC", "amountStr": "1"},
            {"coin": "ETH", "amountStr": "10"}
        ])
        print(f"Demo funds request result: {result}")
        
        # Wait a moment for funds to be credited
        await asyncio.sleep(2)
        
        # Get wallet balance to confirm
        balance = await client.get_wallet_balance()
        print(f"Wallet balance: {balance}")
        
    except Exception as e:
        print(f"Error requesting demo funds: {e}")
    finally:
        await client.close()

async def main():
    """Main entry point"""
    # Get API credentials from environment variables
    api_key = os.getenv("BYBIT_API_KEY")
    api_secret = os.getenv("BYBIT_API_SECRET")
    
    if not api_key or not api_secret:
        print("Error: API credentials not found. Please set BYBIT_API_KEY and BYBIT_API_SECRET environment variables.")
        return
    
    # Request demo funds first
    # await request_demo_funds(api_key, api_secret)
    
    # Run the PMM strategy
    await run_pmm(
        api_key=api_key,
        api_secret=api_secret,
        symbol="HYPE",
        quote="USDT",
        category="linear",
        testnet=True,  # Not using testnet, using demo API directly
        strategy_params=STRATEGY_PARAMS
    )

if __name__ == "__main__":
    asyncio.run(main())