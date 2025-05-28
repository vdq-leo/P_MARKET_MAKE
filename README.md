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
    "order_count": 3,           # Number of orders on each side
    "min_spread": 0.0010,       # Minimum spread (0.1%)
    "max_spread": 0.0050,       # Maximum spread (0.5%)
    "vol_scale": 1.0,           # Volatility impact on spread
    "skew_factor": 0.2,         # Inventory skew impact
    "refresh_time": 60,         # Order refresh time in seconds
    "price_precision": 2,       # Price decimal precision
    "size_precision": 6,        # Size decimal precision
    
    # Volatility parameters
    "vol_window": 20,           # Window for volatility calculation
    "vol_ema_period": 10,       # EMA period for volatility smoothing
    
    # Order distribution
    "price_spacing": 0.0015,    # Spacing between orders (0.15%)
    "size_multiplier": 1.5,     # Size multiplier for orders further from mid
}
```

## Performance Optimizations

- **uvloop**: Faster event loop implementation for asyncio
- **WebSockets**: Real-time data with minimal latency
- **Queue-based Processing**: Non-blocking order processing
- **Concurrent Order Placement**: Place multiple orders simultaneously
- **Efficient Data Structures**: Optimized data storage and retrieval

## License

MIT