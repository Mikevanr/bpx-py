"""
Backpack Exchange Points Farming Bot

Autonomous scalping bot that maximizes trading volume (points) by detecting
price wicks from Binance and executing maker-only trades on Backpack Exchange.

Features:
- Trades 6 perpetual futures with appropriate leverage
- Detects 0.3% wicks within 1 second from Binance stream
- Maker-only entries with 0.1% TP and 0.2% SL
- Auto-closes on SL hit or 30-second profit timeout
- Tracks volume and estimated points per trade

Usage:
    from bpx.points_farmer import PointsFarmer

    bot = PointsFarmer(public_key, secret_key)
    await bot.run()
"""

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Callable, Any
from enum import Enum

import aiohttp

from bpx.async_.account import Account
from bpx.async_.public import Public


# =============================================================================
# Configuration
# =============================================================================

# Trading pairs and their leverage
LEVERAGE: Dict[str, int] = {
    "BTC_USDC_PERP": 50,
    "ETH_USDC_PERP": 50,
    "SOL_USDC_PERP": 50,
    "ZEC_USDC_PERP": 10,
    "2Z_USDT_PERP": 10,
    "MON_USD_PERP": 10,
}

# Map Backpack symbols to Binance stream names
BINANCE_TICKERS: Dict[str, str] = {
    "BTC_USDC_PERP": "btcusdt",
    "ETH_USDC_PERP": "ethusdt",
    "SOL_USDC_PERP": "solusdt",
    "ZEC_USDC_PERP": "zecusdt",
    "2Z_USDT_PERP": "2zusdt",
    "MON_USD_PERP": "monusdt",
}

# Reverse mapping: Binance ticker -> Backpack symbol
BINANCE_TO_BACKPACK: Dict[str, str] = {v: k for k, v in BINANCE_TICKERS.items()}

# Trading parameters
WICK_THRESHOLD = 0.003  # 0.3% price move
WICK_WINDOW_SECONDS = 1.0  # Time window for wick detection
LEVERAGE_USAGE = 0.30  # Use 30% of max leverage
NUM_SYMBOLS = 6  # Number of trading pairs

# Exit parameters
TP_PERCENT = 0.001  # 0.1% take profit
SL_PERCENT = 0.002  # 0.2% stop loss
PROFIT_TIMEOUT_SECONDS = 30  # Close profitable position after 30s

# Safety parameters
COOLDOWN_SECONDS = 3  # Cooldown per symbol after trade
MAX_LOSS_PER_SYMBOL = -15.0  # Pause symbol if cumulative loss exceeds this
STALE_ORDER_TIMEOUT = 10  # Cancel unfilled orders after 10 seconds

# Binance WebSocket
BINANCE_WS_URL = "wss://fstream.binance.com/ws"


# =============================================================================
# Data Classes
# =============================================================================

class Side(Enum):
    LONG = "Bid"
    SHORT = "Ask"


@dataclass
class PricePoint:
    timestamp: float
    price: float


@dataclass
class Position:
    symbol: str
    side: Side
    entry_price: float
    quantity: float
    entry_time: float
    order_id: Optional[str] = None
    tp_order_id: Optional[str] = None
    tp_price: Optional[float] = None
    sl_price: Optional[float] = None
    notional: float = 0.0


@dataclass
class SymbolState:
    prices: deque = field(default_factory=lambda: deque(maxlen=1000))
    last_trade_time: float = 0.0
    cumulative_pnl: float = 0.0
    paused: bool = False
    position: Optional[Position] = None
    pending_entry_order_id: Optional[str] = None
    pending_entry_time: Optional[float] = None


@dataclass
class Stats:
    total_volume: float = 0.0
    total_points: float = 0.0
    total_pnl: float = 0.0
    total_trades: int = 0


# =============================================================================
# Points Farmer Bot
# =============================================================================

class PointsFarmer:
    """
    Autonomous points-farming scalper for Backpack Exchange.
    """

    def __init__(
        self,
        public_key: str,
        secret_key: str,
        debug: bool = False,
    ):
        self.public_key = public_key
        self.secret_key = secret_key
        self.debug = debug

        # Clients
        self.account = Account(public_key, secret_key, debug=debug)
        self.public = Public()

        # State per symbol
        self.states: Dict[str, SymbolState] = {
            symbol: SymbolState() for symbol in LEVERAGE.keys()
        }

        # Global stats
        self.stats = Stats()

        # Control
        self._running = False
        self._binance_ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._session: Optional[aiohttp.ClientSession] = None

        # Market data cache
        self._orderbooks: Dict[str, Dict] = {}
        self._last_prices: Dict[str, float] = {}

    async def run(self) -> None:
        """Main entry point - runs the bot forever."""
        self._running = True
        print("=" * 60)
        print("BACKPACK POINTS FARMER")
        print("=" * 60)
        print(f"Trading pairs: {list(LEVERAGE.keys())}")
        print(f"Wick threshold: {WICK_THRESHOLD * 100}%")
        print(f"TP: {TP_PERCENT * 100}% | SL: {SL_PERCENT * 100}%")
        print("=" * 60)

        try:
            # Get initial balance
            balances = await self.account.get_balances()
            usdc_balance = self._get_usdc_balance(balances)
            print(f"Starting USDC balance: ${usdc_balance:.2f}")
            print("=" * 60)

            # Run main loops concurrently
            await asyncio.gather(
                self._binance_stream_loop(),
                self._position_monitor_loop(),
                self._order_cleanup_loop(),
                self._stats_printer_loop(),
            )
        except KeyboardInterrupt:
            print("\nShutting down gracefully...")
        except Exception as e:
            print(f"Fatal error: {e}")
            raise
        finally:
            await self._cleanup()

    async def _cleanup(self) -> None:
        """Clean up resources."""
        self._running = False
        if self._binance_ws:
            await self._binance_ws.close()
        if self._session:
            await self._session.close()

    # =========================================================================
    # Binance Signal Detection
    # =========================================================================

    async def _binance_stream_loop(self) -> None:
        """Connect to Binance and process trade stream."""
        streams = [f"{ticker}@aggTrade" for ticker in BINANCE_TICKERS.values()]
        stream_url = f"{BINANCE_WS_URL}/{'/'.join(streams)}"

        while self._running:
            try:
                self._session = aiohttp.ClientSession()
                async with self._session.ws_connect(stream_url) as ws:
                    self._binance_ws = ws
                    print("Connected to Binance stream")

                    async for msg in ws:
                        if not self._running:
                            break

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._handle_binance_trade(msg.data)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print(f"Binance WS error: {ws.exception()}")
                            break

            except Exception as e:
                if self._running:
                    print(f"Binance connection error: {e}, reconnecting...")
                    await asyncio.sleep(5)
            finally:
                if self._session:
                    await self._session.close()
                    self._session = None

    async def _handle_binance_trade(self, data: str) -> None:
        """Process a Binance trade message."""
        try:
            msg = json.loads(data)

            # Handle combined stream format
            if "stream" in msg:
                stream = msg["stream"]
                ticker = stream.split("@")[0]
                trade_data = msg["data"]
            else:
                # Single stream format
                ticker = msg.get("s", "").lower()
                trade_data = msg

            # Get Backpack symbol
            symbol = BINANCE_TO_BACKPACK.get(ticker)
            if not symbol:
                return  # Unknown ticker, silently ignore

            price = float(trade_data["p"])
            timestamp = time.time()

            # Update price history
            state = self.states[symbol]
            state.prices.append(PricePoint(timestamp, price))
            self._last_prices[symbol] = price

            # Check for wick
            await self._check_wick(symbol, state)

        except (json.JSONDecodeError, KeyError, ValueError):
            pass  # Silently ignore malformed messages

    async def _check_wick(self, symbol: str, state: SymbolState) -> None:
        """Check if a wick signal has occurred."""
        if state.paused or state.position:
            return

        # Cooldown check
        if time.time() - state.last_trade_time < COOLDOWN_SECONDS:
            return

        # Check for pending entry order
        if state.pending_entry_order_id:
            return

        now = time.time()
        prices = state.prices

        # Get prices within the time window
        window_prices = [
            p for p in prices if now - p.timestamp <= WICK_WINDOW_SECONDS
        ]

        if len(window_prices) < 2:
            return

        # Calculate price change
        oldest_price = window_prices[0].price
        newest_price = window_prices[-1].price
        price_change = (newest_price - oldest_price) / oldest_price

        # Detect wick direction
        if abs(price_change) >= WICK_THRESHOLD:
            if price_change < 0:
                # Price dropped -> go Long
                await self._enter_position(symbol, Side.LONG)
            else:
                # Price spiked -> go Short
                await self._enter_position(symbol, Side.SHORT)

    # =========================================================================
    # Trade Execution
    # =========================================================================

    async def _enter_position(self, symbol: str, side: Side) -> None:
        """Place a maker-only limit entry order."""
        state = self.states[symbol]

        try:
            # Get orderbook for best price
            depth = await self.public.get_depth(symbol)

            if side == Side.LONG:
                # Buy at best bid
                if not depth.get("bids"):
                    return
                entry_price = float(depth["bids"][0][0])
            else:
                # Sell at best ask
                if not depth.get("asks"):
                    return
                entry_price = float(depth["asks"][0][0])

            # Calculate position size
            balances = await self.account.get_balances()
            usdc_balance = self._get_usdc_balance(balances)

            leverage = LEVERAGE[symbol]
            notional = usdc_balance * LEVERAGE_USAGE * leverage / NUM_SYMBOLS
            quantity = notional / entry_price

            # Round quantity appropriately
            quantity = self._round_quantity(symbol, quantity)

            if quantity <= 0:
                return

            # Place maker-only limit order
            order_side = "Bid" if side == Side.LONG else "Ask"

            result = await self.account.execute_order(
                symbol=symbol,
                side=order_side,
                order_type="Limit",
                quantity=str(quantity),
                price=str(entry_price),
                post_only=True,
                time_in_force="GTC",
            )

            if isinstance(result, dict) and result.get("id"):
                state.pending_entry_order_id = result["id"]
                state.pending_entry_time = time.time()
                state.last_trade_time = time.time()

                # Calculate TP and SL prices
                if side == Side.LONG:
                    tp_price = entry_price * (1 + TP_PERCENT)
                    sl_price = entry_price * (1 - SL_PERCENT)
                else:
                    tp_price = entry_price * (1 - TP_PERCENT)
                    sl_price = entry_price * (1 + SL_PERCENT)

                # Store pending position info
                state.position = Position(
                    symbol=symbol,
                    side=side,
                    entry_price=entry_price,
                    quantity=quantity,
                    entry_time=time.time(),
                    order_id=result["id"],
                    tp_price=tp_price,
                    sl_price=sl_price,
                    notional=notional,
                )

                side_str = "Long" if side == Side.LONG else "Short"
                print(f"ENTRY {symbol} {side_str} {quantity:.6f} @ {entry_price:.2f}")
                print(f"  [{symbol}] TP @ {tp_price:.2f}, SL @ {sl_price:.2f}")

        except Exception as e:
            if self.debug:
                print(f"Entry error {symbol}: {e}")

    async def _place_tp_order(self, symbol: str, position: Position) -> None:
        """Place take-profit limit order."""
        try:
            # Opposite side for closing
            close_side = "Ask" if position.side == Side.LONG else "Bid"

            result = await self.account.execute_order(
                symbol=symbol,
                side=close_side,
                order_type="Limit",
                quantity=str(position.quantity),
                price=str(position.tp_price),
                post_only=True,
                reduce_only=True,
                time_in_force="GTC",
            )

            if isinstance(result, dict) and result.get("id"):
                position.tp_order_id = result["id"]

        except Exception as e:
            if self.debug:
                print(f"TP order error {symbol}: {e}")

    async def _close_position_market(
        self, symbol: str, position: Position, reason: str
    ) -> None:
        """Close position with market order."""
        state = self.states[symbol]

        try:
            # Cancel TP order if exists
            if position.tp_order_id:
                try:
                    await self.account.cancel_order(
                        symbol=symbol, order_id=position.tp_order_id
                    )
                except Exception:
                    pass

            # Close with market order
            close_side = "Ask" if position.side == Side.LONG else "Bid"

            await self.account.execute_order(
                symbol=symbol,
                side=close_side,
                order_type="Market",
                quantity=str(position.quantity),
                reduce_only=True,
            )

            # Calculate P/L
            current_price = self._last_prices.get(symbol, position.entry_price)
            if position.side == Side.LONG:
                pnl = (current_price - position.entry_price) / position.entry_price
            else:
                pnl = (position.entry_price - current_price) / position.entry_price

            pnl_usdc = pnl * position.notional
            volume = position.notional * 2  # Entry + exit

            # Update stats
            state.cumulative_pnl += pnl_usdc
            self.stats.total_pnl += pnl_usdc
            self.stats.total_volume += volume
            self.stats.total_points += volume
            self.stats.total_trades += 1

            # Check if symbol should be paused
            if state.cumulative_pnl < MAX_LOSS_PER_SYMBOL:
                state.paused = True
                print(f"[{symbol}] PAUSED - cumulative loss: ${state.cumulative_pnl:.2f}")

            pnl_sign = "+" if pnl_usdc >= 0 else ""
            print(
                f"CLOSE {symbol} ({reason}) | "
                f"P/L={pnl_sign}{pnl_usdc:.2f} USDC | "
                f"Volume={volume:,.0f} | "
                f"Points~{volume:,.0f}"
            )

            # Clear position
            state.position = None
            state.pending_entry_order_id = None

        except Exception as e:
            if self.debug:
                print(f"Close error {symbol}: {e}")

    # =========================================================================
    # Position Monitoring
    # =========================================================================

    async def _position_monitor_loop(self) -> None:
        """Monitor positions for SL hits and profit timeouts."""
        while self._running:
            try:
                for symbol, state in self.states.items():
                    if not state.position:
                        continue

                    position = state.position
                    current_price = self._last_prices.get(symbol)

                    if not current_price:
                        continue

                    # Check if entry order is filled (position is active)
                    if state.pending_entry_order_id:
                        # Check order status
                        try:
                            order = await self.account.get_open_order(
                                symbol=symbol, order_id=state.pending_entry_order_id
                            )
                            if not order or order.get("status") == "Filled":
                                # Entry filled, place TP
                                state.pending_entry_order_id = None
                                position.entry_time = time.time()
                                await self._place_tp_order(symbol, position)
                        except Exception:
                            # Order might be filled or cancelled
                            state.pending_entry_order_id = None
                        continue

                    # Check SL
                    if position.side == Side.LONG:
                        if current_price <= position.sl_price:
                            await self._close_position_market(symbol, position, "SL")
                            continue
                        in_profit = current_price > position.entry_price
                    else:
                        if current_price >= position.sl_price:
                            await self._close_position_market(symbol, position, "SL")
                            continue
                        in_profit = current_price < position.entry_price

                    # Check profit timeout
                    if in_profit:
                        time_held = time.time() - position.entry_time
                        if time_held >= PROFIT_TIMEOUT_SECONDS:
                            await self._close_position_market(
                                symbol, position, "PROFIT_TIMEOUT"
                            )

            except Exception as e:
                if self.debug:
                    print(f"Monitor error: {e}")

            await asyncio.sleep(0.1)

    async def _order_cleanup_loop(self) -> None:
        """Cancel stale unfilled entry orders."""
        while self._running:
            try:
                for symbol, state in self.states.items():
                    if not state.pending_entry_order_id:
                        continue

                    if not state.pending_entry_time:
                        continue

                    # Check if order is stale
                    if time.time() - state.pending_entry_time > STALE_ORDER_TIMEOUT:
                        try:
                            await self.account.cancel_order(
                                symbol=symbol, order_id=state.pending_entry_order_id
                            )
                            if self.debug:
                                print(f"Cancelled stale order {symbol}")
                        except Exception:
                            pass

                        state.pending_entry_order_id = None
                        state.pending_entry_time = None
                        state.position = None

            except Exception as e:
                if self.debug:
                    print(f"Cleanup error: {e}")

            await asyncio.sleep(1)

    async def _stats_printer_loop(self) -> None:
        """Print periodic stats."""
        while self._running:
            await asyncio.sleep(60)  # Every minute

            print("-" * 60)
            print(
                f"STATS | Trades: {self.stats.total_trades} | "
                f"Volume: ${self.stats.total_volume:,.0f} | "
                f"Points: ~{self.stats.total_points:,.0f} | "
                f"P/L: ${self.stats.total_pnl:+.2f}"
            )
            print("-" * 60)

    # =========================================================================
    # Helpers
    # =========================================================================

    def _get_usdc_balance(self, balances: Any) -> float:
        """Extract USDC balance from balances response."""
        if isinstance(balances, dict):
            for asset, data in balances.items():
                if asset in ("USDC", "USDT"):
                    if isinstance(data, dict):
                        return float(data.get("available", 0))
                    return float(data)
        return 0.0

    def _round_quantity(self, symbol: str, quantity: float) -> float:
        """Round quantity based on symbol precision."""
        # BTC needs more decimals, others less
        if "BTC" in symbol:
            return round(quantity, 5)
        elif "ETH" in symbol:
            return round(quantity, 4)
        else:
            return round(quantity, 3)


# =============================================================================
# Entry Point
# =============================================================================

async def main():
    """Run the bot from command line."""
    import os

    public_key = os.environ.get("BPX_PUBLIC_KEY")
    secret_key = os.environ.get("BPX_SECRET_KEY")

    if not public_key or not secret_key:
        print("Error: Set BPX_PUBLIC_KEY and BPX_SECRET_KEY environment variables")
        return

    bot = PointsFarmer(public_key, secret_key, debug=False)
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
