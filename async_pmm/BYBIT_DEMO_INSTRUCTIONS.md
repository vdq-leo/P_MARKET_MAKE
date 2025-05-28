# Bybit Demo API Instructions

This document provides instructions on how to use the Bybit Demo API with the Async PMM framework.

## What is Bybit Demo?

Bybit Demo is a separate environment that allows you to test trading strategies with demo funds. It's different from the testnet in that it uses the same market data as the mainnet but with a separate account and balance.

## Getting API Keys for Bybit Demo

1. Log in to your [Bybit mainnet account](https://www.bybit.com/)
2. Switch to "Demo Trading" mode (usually in the top right corner)
3. Go to your account settings and navigate to the API Management section
4. Create a new API key with appropriate permissions (at minimum: "Read" and "Trade")
5. Save your API key and secret securely

## Using the Async PMM Framework with Bybit Demo

1. Set your API credentials as environment variables:
   ```
   export BYBIT_API_KEY="your_demo_api_key"
   export BYBIT_API_SECRET="your_demo_api_secret"
   ```

2. Run the PMM strategy:
   ```
   python QUANT/run_async_pmm.py
   ```

3. The script will:
   - Request demo funds automatically
   - Connect to the Bybit Demo API
   - Start the PMM strategy

## Demo API Limitations

According to the Bybit documentation:

- Orders generated in demo trading are kept for 7 days
- There are default rate limits that cannot be upgraded
- Only certain API endpoints are available (but all the ones needed for this PMM strategy are supported)
- WebSocket Trade is not supported (but order and execution updates are)

## Requesting Demo Funds

The framework automatically requests demo funds when you start it. The maximum amounts you can request at once are:
- BTC: 15
- ETH: 200
- USDT: 100,000
- USDC: 100,000

You can only request funds once per minute.

## Troubleshooting

If you encounter connection issues:

1. Verify your API keys are for the Demo environment, not mainnet or testnet
2. Check that you have sufficient permissions on your API key
3. Ensure you're not hitting rate limits (especially for fund requests)
4. Check the Bybit status page for any ongoing maintenance

For more information, refer to the [official Bybit Demo API documentation](https://bybit-exchange.github.io/docs/v5/demo).