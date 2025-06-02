from numba import njit
import numpy as np

@njit
def compute_atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, window: int) -> float:
    n = highs.size
    if n < window + 1:
        return 0.0

    sum_tr = 0.0
    for i in range(n - window, n):
        high = highs[i]
        low = lows[i]
        prev_close = closes[i - 1]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        sum_tr += tr

    return sum_tr / window

@njit
def compute_volatility(log_closes: np.ndarray, window: int) -> float:
    n = log_closes.size
    if n < window + 1:
        return 0.0

    # Compute returns from log prices (log-return = log(P_t / P_{t-1}))
    returns = log_closes[n - window - 1 : n]  # last (window+1) values
    sum_sq = 0.0
    sum_ret = 0.0

    for i in range(1, window + 1):
        r = returns[i] - returns[i - 1]
        sum_ret += r
        sum_sq += r * r

    mean_ret = sum_ret / window
    variance = (sum_sq - window * mean_ret * mean_ret) / window  # Bessel's correction not used
    vol = np.sqrt(variance) * np.sqrt(window)

    return vol

@njit
def compute_ratio_series(highs, lows, closes, window, regime_window):
    ratio_series = np.empty(regime_window)
    for i in range(regime_window):
        sub_closes = closes[i:i + window + 1]
        sub_highs = highs[i:i + window + 1]
        sub_lows = lows[i:i + window + 1]

        log_returns = np.log(sub_closes[1:] / sub_closes[:-1])
        vol = np.std(log_returns)

        tr = np.maximum(sub_highs[1:] - sub_lows[1:],
                        np.maximum(np.abs(sub_highs[1:] - sub_closes[:-1]),
                                   np.abs(sub_lows[1:] - sub_closes[:-1])))
        atr = np.mean(tr)

        ratio_series[i] = vol / atr if atr > 0 else 0.0

    return ratio_series

@njit
def ema(series, span):
    alpha = 2.0 / (span + 1)
    result = np.empty_like(series)
    result[0] = series[0]
    for i in range(1, series.size):
        result[i] = alpha * series[i] + (1 - alpha) * result[i - 1]
    return result

@njit
def measure_trading_intensity(order_arrival_depth, out):
    max_tick = 0
    for depth in order_arrival_depth:
        if not np.isfinite(depth):
            continue

        # Sets the tick index to 0 for the nearest possible best price
        # as the order arrival depth in ticks is measured from the mid-price
        tick = round(depth / .5) - 1

        # In a fast-moving market, buy trades can occur below the mid-price (and vice versa for sell trades)
        # since the mid-price is measured in a previous time-step;
        # however, to simplify the problem, we will exclude those cases.
        if tick < 0 or tick >= len(out):
            continue

        # All of our possible quotes within the order arrival depth,
        # excluding those at the same price, are considered executed.
        out[:tick] += 1

        max_tick = max(max_tick, tick)
    return out[:max_tick]

@njit
def compute_coeff(xi, gamma, delta, A, k):
    k = clamp(k, 0.1, 2.5)       # Constrain decay
    A = max(A, 1e-6)                # Prevent division by zero or tiny A
    inv_k = 1 / k
    raw = 1 + xi * delta * inv_k

    c1 = np.log(raw) / (xi * delta)

    # Exponent term stabilizes c2
    exponent = k / (xi * delta) + 1
    term = raw ** exponent
    c2 = np.sqrt(gamma / (2 * A * delta * k) * term)

    return c1, c2

@njit
def linear_regression(x, y):
    sx = np.sum(x)
    sy = np.sum(y)
    sx2 = np.sum(x ** 2)
    sxy = np.sum(x * y)
    w = len(x)
    slope = (w * sxy - sx * sy) / (w * sx2 - sx**2)
    intercept = (sy - slope * sx) / w
    return slope, intercept

@njit
def clamp(val, min_val, max_val):
    return max(min(val, max_val), min_val)

@njit
def estimate_order_intensity(volatility, coef_A = 0.05, coef_k = 0.5):
    if np.isfinite(volatility) and volatility > 0:
        A = clamp(coef_A * (0.1 / volatility), 0.1, 1)
        k = clamp(coef_k /volatility, 0.1, 2.5)
    else: 
        A = None
        k = None
    return A, k
