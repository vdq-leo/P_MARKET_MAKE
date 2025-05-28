# Async PMM Framework

A high-performance, asynchronous Price Market Making (PMM) framework for cryptocurrency trading.

## Features

- **High Performance**: Built with `uvloop` and `asyncio` for maximum performance
- **Real-time Data**: WebSocket integration for real-time market data
- **Efficient Order Management**: Asynchronous order placement and tracking
- **Modular Design**: Separate components for strategy, order management, and exchange communication
- **Customizable Strategy**: Configurable parameters for volatility, spread, and inventory management

## Architecture

```
[ uvloop + asyncio ]
        ↓
[ websockets / aiohttp ]
        ↓
[ asyncio.Queue ]
        ↓
[ PMM Strategy Module (vol, skew, spread) ]
        ↓
[ Async Order Manager (REST or WS) ]
```

## Components

- **PMM Strategy**: Core market making logic (volatility, skew, spread calculations)
- **WebSocket Client**: Real-time market data and order updates
- **REST Client**: API communication for order placement and account management
- **Order Manager**: Asynchronous order placement, tracking, and execution
- **PMM Application**: Main application that coordinates all components

## Installation

```bash
pip install -r requirements.txt
```

## Usage

1. Create a `.env` file with your API credentials:

```
BYBIT_API_KEY=your_demo_api_key
BYBIT_API_SECRET=your_demo_api_secret
```

2. Run the PMM strategy:

```bash
python run_async_pmm.py
```

### Bybit Demo API

This framework is configured to work with the Bybit Demo API, which allows you to test trading strategies with demo funds. The Demo API uses the same market data as the mainnet but with a separate account and balance.

For detailed instructions on how to get API keys for the Bybit Demo environment, see [BYBIT_DEMO_INSTRUCTIONS.md](BYBIT_DEMO_INSTRUCTIONS.md).

Key features of the Bybit Demo integration:
- Automatic demo funds request on startup
- Proper WebSocket connections for real-time data
- Full order management capabilities
- Realistic trading experience with demo funds

## Configuration

You can customize the strategy parameters in `run_async_pmm.py`:

```python
STRATEGY_PARAMS = {
    # Order parameters
    "use_imbalance_price": True,   # Use imbalance price for order placement
    "order_count": 1,           # Number of orders on each side
    "base_spread": 0.0002,
    "min_spread": 0.0002,       # Minimum spread 
    "max_spread": 0.0005,       # Maximum spread 
    "vol_scale": 1,           # Volatility impact on spread
    "skew_factor": 0,           # Inventory skew impact
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
```

## Performance Optimizations

- **uvloop**: Faster event loop implementation for asyncio
- **WebSockets**: Real-time data with minimal latency
- **Queue-based Processing**: Non-blocking order processing
- **Concurrent Order Placement**: Place multiple orders simultaneously
- **Efficient Data Structures**: Optimized data storage and retrieval

## License

LEO