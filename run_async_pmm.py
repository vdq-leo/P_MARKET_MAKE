#!/usr/bin/env python3
"""
Run Async PMM Strategy - Example runner script
"""
import asyncio
import os
import sys
import uvloop
import json
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the PMM package
from async_pmm import run_pmm

# Load environment variables from .env file
load_dotenv()

# Set uvloop as the event loop policy
uvloop.install()

# Strategy parameters
STRATEGY_PARAMS = {
    # Order parameters
    "use_imbalance_price": True,   # Use imbalance price for order placement
    "order_count": 1,           # Number of orders on each side
    "base_spread": 0.0004,
    "min_spread": 0.0001,       # Minimum spread 
    "max_spread": 0.0020,       # Maximum spread 
    "vol_scale": 0.5,           # Volatility impact on spread
    "skew_factor": 0.1,           # Inventory skew impact
    "refresh_time": 0.2,          # Order refresh time in seconds
    
    # Volatility parameters
    "use_dynamic_vol": True,    # Use dynamic volatility
    "vol_window": 10,           # Window for volatility calculation
    "vol_threshold": 0.0005,    # Tolerance for regime detection

    # Order distribution
    "size_multiplier": 2,     # Size multiplier for orders further from mid

    # Trade parameters
    "base_trade_value": 100.0,
    "min_trade_value": 10.0,     # Minimum trade size in quote currency
    "max_trade_value": 200.0,    # Maximum trade size in quote currency

    # Risk management parameters
    "max_position_value": 1000.0,  # Maximum position value in quote currency
    "take_profit_value": 10, # Take profit value
    "stop_loss_value": 10, # Stop loss value
    "max_drawdown_value": 200 # Maximum drawdown in quote currency
}

async def request_demo_funds(api_key, api_secret):
    """Request demo funds before starting the strategy"""
    from async_pmm.bybit_rest import BybitRestClient
    
    print("Requesting demo funds...")
    client = BybitRestClient(api_key, api_secret)
    
    try:
        # Request funds for trading
        result = await client.request_demo_funds([
            {"coin": "USDT", "amountStr": "1000"}
        ], 
        adjust_type=0
        )
        # print(f"Demo funds request result: {result}")
        
        # Wait a moment for funds to be credited
        await asyncio.sleep(2)
        
        # # Get wallet balance to confirm
        # balance = await client.get_wallet_balance()
        # print(f"Wallet balance: {balance}")
        
    except Exception as e:
        print(f"Error requesting demo funds: {e}")
    finally:
        await client.close()

async def clear_demo_funds(api_key, api_secret):
    """Request demo funds before starting the strategy"""
    from async_pmm.bybit_rest import BybitRestClient
    
    print("Clearing demo funds...")
    client = BybitRestClient(api_key, api_secret)
    
    try:
        # Get wallet balance to confirm
        balance = await client.get_wallet_balance()
        bal_list = balance["list"][0]['coin']

        re = []
        for bal in bal_list:
            token = bal["coin"]
            balance = bal["walletBalance"]
            if balance == 0 or token not in ["BTC" ,"ETH", "USDT", "USDC"]:
                continue
            _ = {
                "coin": str(token),
                "amountStr": str(balance)
            }
            re.append(_)

        # # Clear funds for trading
        result = await client.request_demo_funds(re, 
        adjust_type=1
        )
        # print(f"Demo funds clear result: {result}")
        
        # Wait a moment for funds to be credited
        await asyncio.sleep(2)
        
    except Exception as e:
        print(f"Error clear demo funds: {e}")
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
    
    # Clear demo funds first
    await clear_demo_funds(api_key, api_secret)

    # Request demo funds first
    await request_demo_funds(api_key, api_secret)
    
    # Run the PMM strategy
    await run_pmm(
        api_key=api_key,
        api_secret=api_secret,
        symbol="HYPE",
        quote="USDT",
        category="linear",
        testnet=True, 
        strategy_params=STRATEGY_PARAMS
    )

if __name__ == "__main__":
    asyncio.run(main())