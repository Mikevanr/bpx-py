"""
Backpack Exchange Momentum Scalper

High-frequency momentum scalping bot for BTC, ETH, SOL perpetuals.
Follows short-term momentum with tight risk management.

Strategy:
- Detect momentum moves from Binance (0.08% in 3 seconds)
- Enter WITH the momentum (not against it)
- Tight TP (0.06%) and SL (0.10%) for quick trades
- Use 50x leverage for maximum volume generation
- Immediate taker entries for guaranteed fills

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
from typing import Dict, Optional, List, Any
from enum import Enum

import aiohttp

from bpx.async_.account import Account
from bpx.async_.public import Public
from bpx.async_.private_websocket import PrivateWebsocket


# =============================================================================
# Configuration
# =============================================================================

# Trading pairs - HIGH LIQUIDITY ONLY for tight spreads
LEVERAGE: Dict[str, int] = {
    "BTC_USDC_PERP": 50,   # Bitcoin - most liquid
    "ETH_USDC_PERP": 50,   # Ethereum - very liquid
    "SOL_USDC_PERP": 50,   # Solana - good liquidity
}

# Binance price feeds for signal detection
BINANCE_TICKERS: Dict[str, str] = {
    "BTC_USDC_PERP": "btcusdt",
    "ETH_USDC_PERP": "ethusdt",
    "SOL_USDC_PERP": "solusdt",
}

# Reverse mapping
BINANCE_TO_BACKPACK: Dict[str, str] = {v: k for k, v in BINANCE_TICKERS.items()}

# =============================================================================
# MOMENTUM SCALPING PARAMETERS
# =============================================================================

# Signal Detection
MOMENTUM_THRESHOLD = 0.0008  # 0.08% move triggers entry
MOMENTUM_WINDOW = 3.0        # Seconds to measure momentum
MIN_VOLUME_RATIO = 1.5       # Volume must be 1.5x average to confirm signal

# Position Sizing
MAX_CONCURRENT_POSITIONS = 3  # One per symbol max
LEVERAGE_USAGE = 0.8          # Use 80% of max leverage (safety margin)
POSITION_SIZE_PCT = 0.30      # 30% of balance per position

# Take Profit & Stop Loss (TIGHT for scalping)
TP_PERCENT = 0.0006     # 0.06% take profit - quick scalps
SL_PERCENT = 0.0010     # 0.10% stop loss - tight risk control
TRAILING_STOP = True    # Enable trailing stop
TRAILING_ACTIVATION = 0.0003  # Activate trailing after 0.03% profit
TRAILING_DISTANCE = 0.0004    # Trail 0.04% behind price

# Risk Management
MAX_LOSS_USDC = 5.00           # Hard stop per position
MAX_DAILY_LOSS = 50.00         # Stop trading if daily loss exceeds this
COOLDOWN_SECONDS = 1           # Fast cooldown for more trades
MAX_POSITION_TIME = 60         # Force close after 60 seconds

# Order Execution
USE_TAKER_ORDERS = True        # Taker for immediate fills (more reliable)
MAX_SLIPPAGE = 0.0003          # 0.03% max slippage allowed

# Safety
MAX_PRICE_DEVIATION = 0.005    # 0.5% max deviation Binance vs Backpack
STALE_ORDER_TIMEOUT = 3        # Cancel unfilled limit orders after 3s

# Binance WebSocket
BINANCE_WS_URL = "wss://fstream.binance.com/stream"


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
    volume: float = 0.0  # Track volume for confirmation


@dataclass
class Position:
    symbol: str
    side: Side
    entry_price: float
    quantity: float
    entry_time: float
    order_id: Optional[str] = None
    tp_order_id: Optional[str] = None
    sl_order_id: Optional[str] = None
    tp_price: Optional[float] = None
    sl_price: Optional[float] = None
    notional: float = 0.0
    # Trailing stop tracking
    highest_price: float = 0.0  # For longs - track highest since entry
    lowest_price: float = 999999.0  # For shorts - track lowest since entry
    trailing_active: bool = False
    trailing_stop_price: float = 0.0


@dataclass
class SymbolState:
    prices: deque = field(default_factory=lambda: deque(maxlen=1000))
    volumes: deque = field(default_factory=lambda: deque(maxlen=100))  # Track recent volumes
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
    signals_detected: int = 0
    wins: int = 0
    losses: int = 0
    daily_pnl: float = 0.0  # Track daily P/L for risk management


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
        self._last_prices: Dict[str, float] = {}  # Binance prices
        self._backpack_prices: Dict[str, float] = {}  # Backpack prices

        # Track message count for debugging
        self._msg_count = 0

        # Private websocket for real-time order/position updates
        self._private_ws: Optional[PrivateWebsocket] = None

        # Track filled orders for logging
        self._pending_fills: Dict[str, Dict] = {}  # order_id -> order info

    async def run(self) -> None:
        """Main entry point - runs the bot forever."""
        self._running = True
        print("=" * 60)
        print("MOMENTUM SCALPER - BTC/ETH/SOL")
        print("=" * 60)
        print(f"Trading pairs: {list(LEVERAGE.keys())}")
        print(f"Momentum threshold: {MOMENTUM_THRESHOLD * 100}% in {MOMENTUM_WINDOW}s")
        print(f"TP: {TP_PERCENT * 100}% | SL: {SL_PERCENT * 100}%")
        print(f"Trailing: {TRAILING_ACTIVATION * 100}% activate, {TRAILING_DISTANCE * 100}% trail")
        print("=" * 60)

        try:
            # Get initial balance
            balances = await self.account.get_balances()
            usdc_balance = self._get_usdc_balance(balances)
            print(f"Starting USDC balance: ${usdc_balance:.2f}")

            # Connect to Backpack private websocket for real-time updates
            await self._connect_private_websocket()
            print("Connected to Backpack private websocket")

            # Detect and adopt any existing positions
            await self._detect_existing_positions()
            print("=" * 60)

            # Run main loops concurrently
            await asyncio.gather(
                self._binance_stream_loop(),
                self._backpack_price_loop(),
                self._position_monitor_loop(),
                self._order_cleanup_loop(),
                self._stats_printer_loop(),
                self._private_ws_loop(),
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
        if self._private_ws:
            await self._private_ws.disconnect()

    # =========================================================================
    # Binance Signal Detection
    # =========================================================================

    async def _binance_stream_loop(self) -> None:
        """Connect to Binance and process trade stream."""
        # Skip Binance stream if no tickers configured (e.g., PAXG-only mode)
        if not BINANCE_TICKERS:
            print("No Binance tickers configured - using Backpack prices only")
            return

        # Build combined stream URL correctly
        streams = [f"{ticker}@aggTrade" for ticker in BINANCE_TICKERS.values()]
        streams_param = "/".join(streams)
        stream_url = f"{BINANCE_WS_URL}?streams={streams_param}"

        if self.debug:
            print(f"Connecting to: {stream_url}")

        while self._running:
            try:
                self._session = aiohttp.ClientSession()
                async with self._session.ws_connect(stream_url) as ws:
                    self._binance_ws = ws
                    print(f"Connected to Binance stream ({len(BINANCE_TICKERS)} feeds)")

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
                    print(f"Binance connection error: {e}, reconnecting in 5s...")
                    await asyncio.sleep(5)
            finally:
                if self._session:
                    await self._session.close()
                    self._session = None

    async def _handle_binance_trade(self, data: str) -> None:
        """Process a Binance trade message."""
        try:
            msg = json.loads(data)
            self._msg_count += 1

            # Handle combined stream format: {"stream":"btcusdt@aggTrade","data":{...}}
            if "stream" in msg:
                stream = msg["stream"]
                ticker = stream.split("@")[0]
                trade_data = msg["data"]
            else:
                # Single stream format (fallback)
                ticker = msg.get("s", "").lower()
                trade_data = msg

            # Get Backpack symbol
            symbol = BINANCE_TO_BACKPACK.get(ticker)
            if not symbol:
                return  # Unknown ticker, silently ignore

            price = float(trade_data["p"])
            volume = float(trade_data.get("q", 0))  # Trade quantity
            timestamp = time.time()

            # Update price and volume history
            state = self.states[symbol]
            state.prices.append(PricePoint(timestamp, price, volume))
            state.volumes.append(volume)
            self._last_prices[symbol] = price

            # Debug: log first few messages
            if self.debug and self._msg_count <= 5:
                print(f"[{symbol}] Price: {price}, Vol: {volume}")

            # Check for momentum signal
            await self._check_momentum(symbol, state)

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            if self.debug:
                print(f"Parse error: {e}")

    # =========================================================================
    # Backpack Price & Private Websocket
    # =========================================================================

    async def _backpack_price_loop(self) -> None:
        """Fetch Backpack prices periodically and detect wicks for non-Binance tokens."""
        while self._running:
            try:
                tickers = await self.public.get_tickers()
                timestamp = time.time()

                if isinstance(tickers, list):
                    for ticker in tickers:
                        symbol = ticker.get("symbol")
                        if symbol in LEVERAGE:
                            last_price = ticker.get("lastPrice")
                            if last_price:
                                price = float(last_price)
                                self._backpack_prices[symbol] = price

                                # For tokens without Binance feed, use Backpack for wick detection
                                if symbol not in BINANCE_TICKERS:
                                    state = self.states[symbol]
                                    state.prices.append(PricePoint(timestamp, price))
                                    self._last_prices[symbol] = price
                                    await self._check_wick(symbol, state)

                elif isinstance(tickers, dict):
                    for symbol, data in tickers.items():
                        if symbol in LEVERAGE:
                            last_price = data.get("lastPrice")
                            if last_price:
                                price = float(last_price)
                                self._backpack_prices[symbol] = price

                                # For tokens without Binance feed, use Backpack for wick detection
                                if symbol not in BINANCE_TICKERS:
                                    state = self.states[symbol]
                                    state.prices.append(PricePoint(timestamp, price))
                                    self._last_prices[symbol] = price
                                    await self._check_wick(symbol, state)
            except Exception as e:
                if self.debug:
                    print(f"Backpack price fetch error: {e}")
            await asyncio.sleep(1)  # Update every second

    async def _connect_private_websocket(self) -> None:
        """Connect to Backpack private websocket for real-time order/position updates."""
        # Use the secret_key that was passed to the constructor (already base64 encoded)
        self._private_ws = PrivateWebsocket(
            public_key=self.public_key,
            secret_key=self.secret_key,
            debug=self.debug,
        )
        await self._private_ws.connect()

        # Subscribe to order and position updates
        await self._private_ws.subscribe_order_updates(self._handle_order_update)
        await self._private_ws.subscribe_position_updates(self._handle_position_update)

    async def _private_ws_loop(self) -> None:
        """Run the private websocket listener."""
        if self._private_ws:
            try:
                await self._private_ws.listen()
            except Exception as e:
                if self._running:
                    print(f"Private websocket error: {e}, reconnecting...")
                    await asyncio.sleep(5)
                    await self._connect_private_websocket()
                    await self._private_ws_loop()

    async def _handle_order_update(self, message: Dict[str, Any]) -> None:
        """Handle real-time order updates from Backpack."""
        try:
            data = message.get("data", {})
            order_id = data.get("id")
            status = data.get("status")
            symbol = data.get("symbol")
            side = data.get("side")
            filled_qty = data.get("executedQuantity", "0")
            price = data.get("price", "0")

            if self.debug:
                print(f"[WS ORDER] {symbol} {status}: {side} {filled_qty} @ {price}")

            if status == "Filled":
                state = self.states.get(symbol)
                if state and state.pending_entry_order_id == order_id:
                    # Entry order filled
                    print(f"*** ENTRY FILLED {symbol} {side} {filled_qty} @ {price} ***")
                    state.pending_entry_order_id = None
                    if state.position:
                        state.position.entry_time = time.time()
                        state.position.entry_price = float(price) if price else state.position.entry_price
                        # Place TP and SL orders
                        await self._place_tp_order(symbol, state.position)
                        await self._place_sl_order(symbol, state.position)

                elif state and state.position and state.position.tp_order_id == order_id:
                    # TP order filled
                    await self._handle_tp_fill(symbol, state, float(price) if price else None)

                elif state and state.position and state.position.sl_order_id == order_id:
                    # SL order filled
                    await self._handle_sl_fill(symbol, state, float(price) if price else None)

            elif status == "Cancelled":
                state = self.states.get(symbol)
                if state:
                    if state.pending_entry_order_id == order_id:
                        state.pending_entry_order_id = None
                        state.pending_entry_time = None
                        state.position = None
                        if self.debug:
                            print(f"[{symbol}] Entry order cancelled")
                    elif state.position and state.position.tp_order_id == order_id:
                        state.position.tp_order_id = None
                        if self.debug:
                            print(f"[{symbol}] TP order cancelled")
                    elif state.position and state.position.sl_order_id == order_id:
                        state.position.sl_order_id = None
                        if self.debug:
                            print(f"[{symbol}] SL order cancelled")

        except Exception as e:
            if self.debug:
                print(f"Order update error: {e}")

    async def _handle_position_update(self, message: Dict[str, Any]) -> None:
        """Handle real-time position updates from Backpack."""
        try:
            data = message.get("data", {})
            symbol = data.get("symbol")
            position_size = float(data.get("netSize", 0))
            entry_price = float(data.get("entryPrice", 0))
            unrealized_pnl = float(data.get("unrealizedPnl", 0))

            if self.debug:
                print(f"[WS POS] {symbol}: size={position_size}, entry={entry_price}, uPnL={unrealized_pnl}")

            state = self.states.get(symbol)
            if not state:
                return

            # Position closed (size is now 0)
            if abs(position_size) < 0.00001 and state.position:
                # Position was closed (either by TP, SL, or manual)
                if self.debug:
                    print(f"[{symbol}] Position closed detected via websocket")

        except Exception as e:
            if self.debug:
                print(f"Position update error: {e}")

    async def _handle_tp_fill(self, symbol: str, state: SymbolState, fill_price: Optional[float]) -> None:
        """Handle take-profit fill."""
        if not state.position:
            return

        position = state.position
        close_price = fill_price or self._last_prices.get(symbol, position.entry_price)

        # Calculate P/L using actual prices and quantity
        if position.side == Side.LONG:
            pnl_usdc = (close_price - position.entry_price) * position.quantity
        else:
            pnl_usdc = (position.entry_price - close_price) * position.quantity

        volume = position.notional * 2  # Entry + exit

        # Update stats
        state.cumulative_pnl += pnl_usdc
        self.stats.total_pnl += pnl_usdc
        self.stats.total_volume += volume
        self.stats.total_points += volume
        self.stats.total_trades += 1

        pnl_sign = "+" if pnl_usdc >= 0 else ""
        print(
            f"CLOSE {symbol} (TP_FILL) | "
            f"Entry={position.entry_price:.2f} Close={close_price:.2f} | "
            f"P/L={pnl_sign}{pnl_usdc:.2f} USDC | "
            f"Volume={volume:,.0f}"
        )

        # Cancel SL order since position is closed
        if position.sl_order_id:
            try:
                await self.account.cancel_order(symbol=symbol, order_id=position.sl_order_id)
            except Exception:
                pass

        # Clear position
        state.position = None
        state.pending_entry_order_id = None

    async def _handle_sl_fill(self, symbol: str, state: SymbolState, fill_price: Optional[float]) -> None:
        """Handle stop-loss fill."""
        if not state.position:
            return

        position = state.position
        close_price = fill_price or self._last_prices.get(symbol, position.entry_price)

        # Calculate P/L using actual prices and quantity
        if position.side == Side.LONG:
            pnl_usdc = (close_price - position.entry_price) * position.quantity
        else:
            pnl_usdc = (position.entry_price - close_price) * position.quantity

        volume = position.notional * 2  # Entry + exit

        # Update stats
        state.cumulative_pnl += pnl_usdc
        self.stats.total_pnl += pnl_usdc
        self.stats.total_volume += volume
        self.stats.total_points += volume
        self.stats.total_trades += 1

        pnl_sign = "+" if pnl_usdc >= 0 else ""
        print(
            f"CLOSE {symbol} (SL_FILL) | "
            f"Entry={position.entry_price:.2f} Close={close_price:.2f} | "
            f"P/L={pnl_sign}{pnl_usdc:.2f} USDC | "
            f"Volume={volume:,.0f}"
        )

        # Cancel TP order since position is closed
        if position.tp_order_id:
            try:
                await self.account.cancel_order(symbol=symbol, order_id=position.tp_order_id)
            except Exception:
                pass

        # Clear position
        state.position = None
        state.pending_entry_order_id = None

    async def _check_momentum(self, symbol: str, state: SymbolState) -> None:
        """Check for momentum signal - FOLLOW the trend."""
        # Skip if paused or already in position
        if state.paused:
            return
        if state.position:
            return

        # Check daily loss limit
        if self.stats.daily_pnl <= -MAX_DAILY_LOSS:
            if not state.paused:
                print(f"[{symbol}] PAUSED: Daily loss limit reached (${self.stats.daily_pnl:.2f})")
                state.paused = True
            return

        # Cooldown check
        if time.time() - state.last_trade_time < COOLDOWN_SECONDS:
            return

        # Check for pending entry order
        if state.pending_entry_order_id:
            return

        now = time.time()
        prices = state.prices

        # Get prices within the momentum window
        window_prices = [
            p for p in prices if now - p.timestamp <= MOMENTUM_WINDOW
        ]

        if len(window_prices) < 5:  # Need enough data points
            return

        # Calculate momentum (price change over window)
        oldest_price = window_prices[0].price
        newest_price = window_prices[-1].price
        momentum = (newest_price - oldest_price) / oldest_price

        # Check if momentum exceeds threshold
        if abs(momentum) < MOMENTUM_THRESHOLD:
            return

        # Volume confirmation - check if recent volume is above average
        if len(state.volumes) >= 10:
            avg_volume = sum(state.volumes) / len(state.volumes)
            recent_volume = sum(p.volume for p in window_prices[-5:])
            volume_ratio = recent_volume / (avg_volume * 5) if avg_volume > 0 else 1.0

            if volume_ratio < MIN_VOLUME_RATIO:
                # Low volume move - skip (likely noise)
                return

        self.stats.signals_detected += 1
        direction = "UP" if momentum > 0 else "DOWN"
        print(f"[{symbol}] MOMENTUM {direction}: {momentum*100:.3f}% - FOLLOWING", flush=True)

        # MOMENTUM FOLLOWING: Trade WITH the trend
        if momentum > 0:
            # Price moving UP -> go LONG (ride the wave)
            await self._enter_position(symbol, Side.LONG)
        else:
            # Price moving DOWN -> go SHORT (ride the wave)
            await self._enter_position(symbol, Side.SHORT)

    # =========================================================================
    # Trade Execution
    # =========================================================================

    async def _enter_position(self, symbol: str, side: Side) -> None:
        """Enter position with market order for immediate fill."""
        state = self.states[symbol]
        side_str = "Long" if side == Side.LONG else "Short"
        print(f"[{symbol}] _enter_position called: {side_str}", flush=True)

        # Check max concurrent positions limit
        current_positions = sum(1 for s in self.states.values() if s.position is not None)
        if current_positions >= MAX_CONCURRENT_POSITIONS:
            print(f"[{symbol}] BLOCKED: Max {MAX_CONCURRENT_POSITIONS} positions reached")
            return

        # Set cooldown immediately
        state.last_trade_time = time.time()

        try:
            # Get reference price from Binance
            reference_price = self._last_prices.get(symbol)
            if not reference_price:
                print(f"[{symbol}] BLOCKED: No Binance price", flush=True)
                return

            # Get Backpack orderbook for execution price
            try:
                depth = await self.public.get_depth(symbol)
                bids = depth.get("bids", [])
                asks = depth.get("asks", [])

                if not bids or not asks:
                    print(f"[{symbol}] Empty orderbook")
                    return

                best_bid = float(bids[0][0])
                best_ask = float(asks[0][0])
                spread_pct = (best_ask - best_bid) / best_bid * 100

                # Check slippage vs Binance price
                if side == Side.LONG:
                    slippage = (best_ask - reference_price) / reference_price
                else:
                    slippage = (reference_price - best_bid) / reference_price

                if slippage > MAX_SLIPPAGE:
                    print(f"[{symbol}] Slippage too high: {slippage*100:.3f}%")
                    return

            except Exception as e:
                print(f"[{symbol}] Orderbook error: {e}")
                return

            # Calculate position size: POSITION_SIZE_PCT of balance * leverage
            balances = await self.account.get_balances()
            usdc_balance = self._get_usdc_balance(balances)

            leverage = LEVERAGE[symbol]
            effective_leverage = leverage * LEVERAGE_USAGE
            margin = usdc_balance * POSITION_SIZE_PCT
            notional = margin * effective_leverage
            entry_price = best_ask if side == Side.LONG else best_bid
            quantity = notional / entry_price

            # Round quantity
            quantity = self._round_quantity(symbol, quantity)

            if quantity <= 0:
                print(f"[{symbol}] Quantity too small")
                return

            order_side = "Bid" if side == Side.LONG else "Ask"

            print(f"[{symbol}] MARKET {side_str} {quantity} @ ~${entry_price:.2f} (${notional:.0f} notional, {effective_leverage:.0f}x)")

            # Use MARKET order for immediate fill
            result = await self.account.execute_order(
                symbol=symbol,
                side=order_side,
                order_type="Market",
                quantity=str(quantity),
            )

            if isinstance(result, dict) and result.get("id"):
                order_id = result["id"]
                order_status = result.get("status", "")
                executed_qty = float(result.get("executedQuantity", 0) or 0)

                if order_status == "Filled" or executed_qty > 0:
                    fill_price = float(result.get("avgPrice") or result.get("price") or entry_price)
                    actual_qty = executed_qty if executed_qty > 0 else quantity

                    # Calculate TP and SL prices
                    if side == Side.LONG:
                        tp_price = fill_price * (1 + TP_PERCENT)
                        sl_price = fill_price * (1 - SL_PERCENT)
                    else:
                        tp_price = fill_price * (1 - TP_PERCENT)
                        sl_price = fill_price * (1 + SL_PERCENT)

                    # Create position with trailing stop tracking
                    state.position = Position(
                        symbol=symbol,
                        side=side,
                        entry_price=fill_price,
                        quantity=actual_qty,
                        entry_time=time.time(),
                        order_id=order_id,
                        tp_price=tp_price,
                        sl_price=sl_price,
                        notional=notional,
                        highest_price=fill_price,
                        lowest_price=fill_price,
                    )

                    print(f"*** FILLED {symbol} {side_str} {actual_qty:.6f} @ ${fill_price:.2f} ***")
                    print(f"    TP: ${tp_price:.2f} | SL: ${sl_price:.2f}")

                    # Update stats
                    self.stats.total_volume += notional
                    self.stats.total_points += notional

                else:
                    print(f"[{symbol}] Order failed: {order_status}")
            else:
                error_msg = result.get('message', result) if isinstance(result, dict) else result
                print(f"[{symbol}] Order rejected: {error_msg}")

        except Exception as e:
            print(f"Entry error {symbol}: {e}")

    async def _place_tp_order(self, symbol: str, position: Position) -> None:
        """Place take-profit maker-only limit order for 50% fee discount."""
        try:
            # Opposite side for closing
            close_side = "Ask" if position.side == Side.LONG else "Bid"

            # Round TP price properly
            tp_price = self._round_price(symbol, position.tp_price)

            result = await self.account.execute_order(
                symbol=symbol,
                side=close_side,
                order_type="Limit",
                quantity=str(position.quantity),
                price=str(tp_price),
                reduce_only=True,
                time_in_force="GTC",
                post_only=True,  # Ensures maker-only execution for 50% fee discount
            )

            if isinstance(result, dict) and result.get("id"):
                order_status = result.get("status", "")
                position.tp_order_id = result["id"]

                if order_status == "Filled":
                    # TP filled immediately - great!
                    print(f"[{symbol}] TP order filled immediately at {tp_price}")
                elif self.debug:
                    print(f"[{symbol}] TP order placed: {result['id']} at {tp_price}")
            else:
                error_msg = result.get('message', result) if isinstance(result, dict) else result
                print(f"[{symbol}] TP order failed: {error_msg}")

        except Exception as e:
            print(f"TP order error {symbol}: {e}")

    async def _place_sl_order(self, symbol: str, position: Position) -> None:
        """Place stop-loss order on exchange for guaranteed execution."""
        try:
            # Opposite side for closing
            close_side = "Ask" if position.side == Side.LONG else "Bid"

            # Round SL price properly
            sl_price = self._round_price(symbol, position.sl_price)

            # Use trigger price AND trigger quantity for stop order (Backpack requires both)
            result = await self.account.execute_order(
                symbol=symbol,
                side=close_side,
                order_type="Market",
                quantity=str(position.quantity),
                trigger_price=str(sl_price),
                trigger_quantity=str(position.quantity),  # Required by Backpack API
                reduce_only=True,
            )

            if isinstance(result, dict) and result.get("id"):
                position.sl_order_id = result["id"]
                print(f"[{symbol}] SL order placed: trigger @ {sl_price}")
            else:
                error_msg = result.get('message', result) if isinstance(result, dict) else result
                print(f"[{symbol}] SL order failed: {error_msg} - using software SL as backup")

        except Exception as e:
            print(f"SL order error {symbol}: {e} - using software SL as backup")

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

            # Cancel SL order if exists
            if position.sl_order_id:
                try:
                    await self.account.cancel_order(
                        symbol=symbol, order_id=position.sl_order_id
                    )
                except Exception:
                    pass

            # Close with market order
            close_side = "Ask" if position.side == Side.LONG else "Bid"

            result = await self.account.execute_order(
                symbol=symbol,
                side=close_side,
                order_type="Market",
                quantity=str(position.quantity),
                reduce_only=True,
            )

            # Get actual fill price from the order result
            close_price = None
            if isinstance(result, dict):
                # Try to get the average fill price from the order
                close_price = result.get("price") or result.get("avgPrice")
                if close_price:
                    close_price = float(close_price)

            # Fall back to cached price if no fill price returned
            if not close_price:
                close_price = self._backpack_prices.get(symbol) or self._last_prices.get(symbol, position.entry_price)

            # Calculate P/L using actual prices
            if position.side == Side.LONG:
                pnl_usdc = (close_price - position.entry_price) * position.quantity
            else:
                pnl_usdc = (position.entry_price - close_price) * position.quantity

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
                f"Entry={position.entry_price:.2f} Close={close_price:.2f} | "
                f"P/L={pnl_sign}{pnl_usdc:.2f} USDC | "
                f"Volume={volume:,.0f}"
            )

            # Clear position
            state.position = None
            state.pending_entry_order_id = None

        except Exception as e:
            print(f"Close error {symbol}: {e}")

    async def _emergency_close(
        self, symbol: str, position: Position, reason: str
    ) -> None:
        """Emergency close position with market order - bypasses maker-only strategy."""
        state = self.states[symbol]

        print(f"[{symbol}] 🚨 EMERGENCY CLOSE initiated - {reason}")

        try:
            # Cancel ALL existing orders immediately
            if position.tp_order_id:
                try:
                    await self.account.cancel_order(
                        symbol=symbol, order_id=position.tp_order_id
                    )
                    print(f"[{symbol}] Cancelled TP order")
                except Exception:
                    pass

            if position.sl_order_id:
                try:
                    await self.account.cancel_order(
                        symbol=symbol, order_id=position.sl_order_id
                    )
                    print(f"[{symbol}] Cancelled SL order")
                except Exception:
                    pass

            # EMERGENCY: Use market order for guaranteed execution
            close_side = "Ask" if position.side == Side.LONG else "Bid"

            print(f"[{symbol}] 🚨 Sending MARKET order to close {position.quantity}")

            result = await self.account.execute_order(
                symbol=symbol,
                side=close_side,
                order_type="Market",
                quantity=str(position.quantity),
                reduce_only=True,
            )

            # Get actual fill price from the order result
            close_price = None
            if isinstance(result, dict):
                close_price = result.get("price") or result.get("avgPrice")
                if close_price:
                    close_price = float(close_price)

            # Fall back to cached price if no fill price returned
            if not close_price:
                close_price = self._backpack_prices.get(symbol) or self._last_prices.get(symbol, position.entry_price)

            # Calculate P/L using actual prices
            if position.side == Side.LONG:
                pnl_usdc = (close_price - position.entry_price) * position.quantity
            else:
                pnl_usdc = (position.entry_price - close_price) * position.quantity

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
                f"🚨 EMERGENCY CLOSE {symbol} ({reason}) | "
                f"Entry={position.entry_price:.2f} Close={close_price:.2f} | "
                f"P/L={pnl_sign}{pnl_usdc:.2f} USDC | "
                f"Volume={volume:,.0f}"
            )

            # Clear position
            state.position = None
            state.pending_entry_order_id = None

        except Exception as e:
            print(f"🚨 EMERGENCY CLOSE FAILED {symbol}: {e}")
            # Try one more time with a fresh market order
            try:
                close_side = "Ask" if position.side == Side.LONG else "Bid"
                await self.account.execute_order(
                    symbol=symbol,
                    side=close_side,
                    order_type="Market",
                    quantity=str(position.quantity),
                    reduce_only=True,
                )
                state.position = None
                print(f"[{symbol}] Emergency close retry succeeded")
            except Exception as e2:
                print(f"[{symbol}] CRITICAL: Emergency close retry also failed: {e2}")

    # =========================================================================
    # Position Monitoring
    # =========================================================================

    async def _position_monitor_loop(self) -> None:
        """Monitor positions with trailing stops."""
        print("[MONITOR] Position monitor started", flush=True)
        last_sync = 0

        while self._running:
            try:
                now = time.time()

                # Sync positions every 5 seconds
                if now - last_sync >= 5:
                    last_sync = now
                    await self._sync_positions()

                for symbol, state in self.states.items():
                    if not state.position:
                        continue

                    position = state.position
                    current_price = self._last_prices.get(symbol)
                    if not current_price:
                        continue

                    # Calculate profit/loss
                    if position.side == Side.LONG:
                        profit_pct = (current_price - position.entry_price) / position.entry_price
                        unrealized_pnl = (current_price - position.entry_price) * position.quantity
                        # Update highest price for trailing stop
                        if current_price > position.highest_price:
                            position.highest_price = current_price
                    else:
                        profit_pct = (position.entry_price - current_price) / position.entry_price
                        unrealized_pnl = (position.entry_price - current_price) * position.quantity
                        # Update lowest price for trailing stop
                        if current_price < position.lowest_price:
                            position.lowest_price = current_price

                    time_held = now - position.entry_time

                    # ========== TAKE PROFIT ==========
                    if profit_pct >= TP_PERCENT:
                        await self._close_position(symbol, position, "TP", unrealized_pnl)
                        continue

                    # ========== TRAILING STOP ==========
                    if TRAILING_STOP and profit_pct >= TRAILING_ACTIVATION:
                        if not position.trailing_active:
                            position.trailing_active = True
                            print(f"[{symbol}] Trailing stop ACTIVATED at {profit_pct*100:.3f}%")

                        # Calculate trailing stop price
                        if position.side == Side.LONG:
                            position.trailing_stop_price = position.highest_price * (1 - TRAILING_DISTANCE)
                            if current_price <= position.trailing_stop_price:
                                await self._close_position(symbol, position, "TRAIL", unrealized_pnl)
                                continue
                        else:
                            position.trailing_stop_price = position.lowest_price * (1 + TRAILING_DISTANCE)
                            if current_price >= position.trailing_stop_price:
                                await self._close_position(symbol, position, "TRAIL", unrealized_pnl)
                                continue

                    # ========== STOP LOSS ==========
                    if position.side == Side.LONG:
                        if current_price <= position.sl_price:
                            await self._close_position(symbol, position, "SL", unrealized_pnl)
                            continue
                    else:
                        if current_price >= position.sl_price:
                            await self._close_position(symbol, position, "SL", unrealized_pnl)
                            continue

                    # ========== MAX LOSS (USDC) ==========
                    if unrealized_pnl <= -MAX_LOSS_USDC:
                        await self._close_position(symbol, position, "MAX_LOSS", unrealized_pnl)
                        continue

                    # ========== MAX TIME ==========
                    if time_held >= MAX_POSITION_TIME:
                        await self._close_position(symbol, position, "TIMEOUT", unrealized_pnl)
                        continue

                    # Log status every 10 seconds
                    if int(time_held) % 10 == 0 and int(time_held) > 0:
                        side_str = "L" if position.side == Side.LONG else "S"
                        trail_str = f" TRAIL@{position.trailing_stop_price:.2f}" if position.trailing_active else ""
                        print(f"[{symbol}] {side_str} {profit_pct*100:+.3f}% ${unrealized_pnl:+.2f} | {time_held:.0f}s{trail_str}")

            except Exception as e:
                # Always log monitor errors - this is critical for SL execution
                print(f"Monitor error: {e}")

            await asyncio.sleep(0.1)

    async def _close_position(self, symbol: str, position: Position, reason: str, pnl: float) -> None:
        """Close position with market order and update stats."""
        state = self.states[symbol]

        try:
            close_side = "Ask" if position.side == Side.LONG else "Bid"

            result = await self.account.execute_order(
                symbol=symbol,
                side=close_side,
                order_type="Market",
                quantity=str(position.quantity),
                reduce_only=True,
            )

            # Get actual fill price
            close_price = position.entry_price
            if isinstance(result, dict):
                close_price = float(result.get("avgPrice") or result.get("price") or close_price)

            # Recalculate actual PnL
            if position.side == Side.LONG:
                actual_pnl = (close_price - position.entry_price) * position.quantity
            else:
                actual_pnl = (position.entry_price - close_price) * position.quantity

            volume = position.notional * 2  # Entry + exit

            # Update stats
            self.stats.total_pnl += actual_pnl
            self.stats.daily_pnl += actual_pnl
            self.stats.total_volume += position.notional  # Exit volume
            self.stats.total_points += position.notional
            self.stats.total_trades += 1

            if actual_pnl >= 0:
                self.stats.wins += 1
            else:
                self.stats.losses += 1

            state.cumulative_pnl += actual_pnl

            # Log the close
            side_str = "LONG" if position.side == Side.LONG else "SHORT"
            pnl_str = f"+${actual_pnl:.2f}" if actual_pnl >= 0 else f"-${abs(actual_pnl):.2f}"
            win_rate = self.stats.wins / self.stats.total_trades * 100 if self.stats.total_trades > 0 else 0

            print(f"CLOSE {symbol} [{reason}] {side_str} | Entry=${position.entry_price:.2f} Exit=${close_price:.2f} | {pnl_str} | W/L: {self.stats.wins}/{self.stats.losses} ({win_rate:.0f}%)")

            # Clear position
            state.position = None

        except Exception as e:
            print(f"Close error {symbol}: {e}")

    async def _sync_positions(self) -> None:
        """Sync local state with actual positions on Backpack."""
        try:
            print("[SYNC] Fetching positions from API...", flush=True)
            positions = await self.account.get_open_positions()
            print(f"[SYNC] Got response: type={type(positions)}", flush=True)

            if not isinstance(positions, list):
                print(f"[SYNC] ERROR: Positions not a list: {type(positions)} - {positions}", flush=True)
                return

            # Always log position count (even if empty)
            print(f"[SYNC] API returned {len(positions)} position(s)", flush=True)

            # Debug: show all positions from API
            if positions:
                for pos in positions:
                    sym = pos.get("symbol", "?")
                    qty = pos.get("netQuantity", 0)
                    in_leverage = "YES" if sym in LEVERAGE else "NO"
                    print(f"[SYNC]   -> {sym}: qty={qty}, in_config={in_leverage}")

            # Build map of actual positions
            actual_positions: Dict[str, Dict] = {}
            for pos in positions:
                symbol = pos.get("symbol")
                if symbol and symbol in LEVERAGE:
                    actual_positions[symbol] = pos
                    net_qty = float(pos.get("netQuantity", 0))
                    if abs(net_qty) > 0.00001:
                        # Log every sync to verify positions are being detected
                        state = self.states.get(symbol)
                        tracked = "TRACKED" if (state and state.position) else "NOT TRACKED"
                        print(f"[SYNC] {symbol}: qty={net_qty:.6f} - {tracked}")

            # Check each tracked symbol
            for symbol, state in self.states.items():
                actual = actual_positions.get(symbol)
                actual_size = float(actual.get("netQuantity", 0)) if actual else 0

                # Case 1: We have a tracked position
                if state.position:
                    # If there's a pending entry, check if it's now filled
                    if state.pending_entry_order_id:
                        if abs(actual_size) > 0.00001:
                            # Position exists on Backpack - entry was filled!
                            actual_entry = float(actual.get("entryPrice", 0))
                            print(f"*** ENTRY CONFIRMED {symbol}: qty={actual_size:.6f} @ {actual_entry:.2f} ***")
                            state.pending_entry_order_id = None
                            state.pending_entry_time = None
                            # Update position with actual fill data
                            state.position.entry_price = actual_entry
                            state.position.quantity = abs(actual_size)
                            state.position.entry_time = time.time()
                            # Place TP and SL orders
                            await self._place_tp_order(symbol, state.position)
                            await self._place_sl_order(symbol, state.position)
                    else:
                        # No pending entry - check if position is still open
                        if abs(actual_size) < 0.00001:
                            # Position was closed externally (likely TP filled)
                            position = state.position

                            # Estimate P/L using current market price
                            close_price = self._backpack_prices.get(symbol) or self._last_prices.get(symbol)
                            if close_price and position.entry_price:
                                if position.side == Side.LONG:
                                    pnl_usdc = (close_price - position.entry_price) * position.quantity
                                else:
                                    pnl_usdc = (position.entry_price - close_price) * position.quantity

                                volume = position.notional * 2

                                # Update stats
                                state.cumulative_pnl += pnl_usdc
                                self.stats.total_pnl += pnl_usdc
                                self.stats.total_volume += volume
                                self.stats.total_points += volume
                                self.stats.total_trades += 1

                                pnl_sign = "+" if pnl_usdc >= 0 else ""
                                print(
                                    f"CLOSE {symbol} (EXTERNAL) | "
                                    f"Entry={position.entry_price:.2f} Close~{close_price:.2f} | "
                                    f"P/L={pnl_sign}{pnl_usdc:.2f} USDC | "
                                    f"Volume={volume:,.0f}"
                                )
                            else:
                                print(f"[{symbol}] Position closed externally (no price data)")

                            state.position = None

                # Case 2: No tracked position but Backpack has one - adopt it!
                elif abs(actual_size) > 0.00001:
                    await self._adopt_position(symbol, actual)

        except Exception as e:
            print(f"Sync positions error: {e}", flush=True)
            import traceback
            traceback.print_exc()
            import sys
            sys.stdout.flush()

    async def _detect_existing_positions(self) -> None:
        """Detect and adopt any existing positions on startup."""
        try:
            positions = await self.account.get_open_positions()
            print(f"Checking for existing positions...")

            if not isinstance(positions, list):
                print(f"No positions found (response type: {type(positions)})")
                return

            found_count = 0
            for pos in positions:
                symbol = pos.get("symbol")
                if symbol and symbol in LEVERAGE:
                    # Use netQuantity (the actual field from API)
                    net_qty = float(pos.get("netQuantity", 0))
                    if abs(net_qty) > 0.00001:
                        found_count += 1
                        await self._adopt_position(symbol, pos)

            if found_count == 0:
                print("No existing positions to adopt")
            else:
                print(f"Adopted {found_count} existing position(s)")

        except Exception as e:
            print(f"Error detecting existing positions: {e}")
            import traceback
            traceback.print_exc()

    async def _adopt_position(self, symbol: str, pos_data: Dict) -> None:
        """Adopt an existing position from Backpack into our tracking state."""
        try:
            state = self.states.get(symbol)
            if not state:
                print(f"[{symbol}] Cannot adopt - symbol not in tracked states")
                return

            # Already tracking a position for this symbol
            if state.position:
                print(f"[{symbol}] Cannot adopt - already tracking a position")
                return

            # Use netQuantity (the actual field name from Backpack API)
            net_qty = float(pos_data.get("netQuantity", 0))
            entry_price = float(pos_data.get("entryPrice", 0))

            # Calculate notional from netExposureNotional or compute it
            notional_value = float(pos_data.get("netExposureNotional", 0))
            if not notional_value:
                notional_value = abs(net_qty * entry_price)

            if abs(net_qty) < 0.00001:
                return

            if entry_price <= 0:
                print(f"[{symbol}] Cannot adopt - invalid entry price: {entry_price}")
                return

            # Determine side based on net quantity (positive = long, negative = short)
            side = Side.LONG if net_qty > 0 else Side.SHORT
            quantity = abs(net_qty)

            # Calculate TP and SL prices based on entry
            if side == Side.LONG:
                tp_price = entry_price * (1 + TP_PERCENT)
                sl_price = entry_price * (1 - SL_PERCENT)
            else:
                tp_price = entry_price * (1 - TP_PERCENT)
                sl_price = entry_price * (1 + SL_PERCENT)

            # Round prices
            tp_price = self._round_price(symbol, tp_price)
            sl_price = self._round_price(symbol, sl_price)

            # Create position object
            state.position = Position(
                symbol=symbol,
                side=side,
                entry_price=entry_price,
                quantity=quantity,
                entry_time=time.time(),  # We don't know actual entry time
                tp_price=tp_price,
                sl_price=sl_price,
                notional=notional_value,
            )

            side_str = "Long" if side == Side.LONG else "Short"
            print(f"*** ADOPTED POSITION: {symbol} {side_str} {quantity:.6f} @ {entry_price:.2f} ***")
            print(f"    TP @ {tp_price:.2f}, SL @ {sl_price:.2f}, Notional: ${notional_value:.2f}")

            # Place TP and SL orders for the adopted position
            await self._place_tp_order(symbol, state.position)
            await self._place_sl_order(symbol, state.position)

        except Exception as e:
            print(f"Error adopting position {symbol}: {e}")
            import traceback
            traceback.print_exc()

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
                            print(f"[{symbol}] Cancelled stale entry order")
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
        last_debug_print = 0

        while self._running:
            await asyncio.sleep(5)  # Check every 5 seconds

            now = time.time()

            # Print price changes every 10 seconds
            if now - last_debug_print >= 10:
                last_debug_print = now
                self._print_price_changes()

    def _print_price_changes(self) -> None:
        """Print price changes for all symbols over different timeframes."""
        now = time.time()

        print("-" * 85)
        print(f"{'Symbol':<10} {'Binance':>11} {'Backpack':>11} {'Diff':>8} {'1s':>8} {'10s':>8} {'1m':>8}")
        print("-" * 85)

        for symbol in BINANCE_TICKERS.keys():
            state = self.states[symbol]
            binance_price = self._last_prices.get(symbol)
            backpack_price = self._backpack_prices.get(symbol)

            if not binance_price or len(state.prices) == 0:
                print(f"{symbol:<10} {'no data':>11}")
                continue

            # Calculate price difference between exchanges
            price_diff = ""
            if backpack_price and binance_price:
                diff_pct = (backpack_price - binance_price) / binance_price * 100
                price_diff = f"{diff_pct:+.3f}%"

            # Calculate changes for different timeframes
            change_1s = self._calc_price_change(state.prices, now, 1.0)
            change_10s = self._calc_price_change(state.prices, now, 10.0)
            change_1m = self._calc_price_change(state.prices, now, 60.0)

            # Format output
            def fmt_change(c):
                if c is None:
                    return "n/a"
                color_prefix = ""
                if abs(c) >= MOMENTUM_THRESHOLD:
                    color_prefix = "**"  # Highlight momentum signal
                return f"{color_prefix}{c*100:+.3f}%"

            short_symbol = symbol.replace("_USDC_PERP", "").replace("_USDT_PERP", "").replace("_USD_PERP", "")
            bp_str = f"${backpack_price:>.2f}" if backpack_price else "n/a"
            print(
                f"{short_symbol:<10} "
                f"${binance_price:>10.2f} "
                f"{bp_str:>11} "
                f"{price_diff:>8} "
                f"{fmt_change(change_1s):>8} "
                f"{fmt_change(change_10s):>8} "
                f"{fmt_change(change_1m):>8}"
            )

        # Print summary stats
        win_rate = self.stats.wins / self.stats.total_trades * 100 if self.stats.total_trades > 0 else 0
        print("-" * 85)
        print(
            f"Signals: {self.stats.signals_detected} | "
            f"Trades: {self.stats.total_trades} | "
            f"W/L: {self.stats.wins}/{self.stats.losses} ({win_rate:.0f}%) | "
            f"Volume: ${self.stats.total_volume:,.0f} | "
            f"P/L: ${self.stats.total_pnl:+.2f}"
        )
        print("-" * 85)

    def _calc_price_change(self, prices: deque, now: float, seconds: float) -> Optional[float]:
        """Calculate price change over the given time window."""
        if len(prices) < 2:
            return None

        # Find oldest price within the window
        oldest_in_window = None
        for p in prices:
            if now - p.timestamp <= seconds:
                oldest_in_window = p
                break

        if oldest_in_window is None:
            return None

        newest = prices[-1]
        if oldest_in_window.timestamp == newest.timestamp:
            return 0.0

        return (newest.price - oldest_in_window.price) / oldest_in_window.price

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
        """Round quantity based on symbol precision (from Backpack specs)."""
        # Quantity decimal places for each symbol (0 = integer only)
        QTY_DECIMALS = {
            # High precision (expensive assets)
            "BTC_USDC_PERP": 4,    # 0.0001 BTC
            "ETH_USDC_PERP": 3,    # 0.001 ETH
            "PAXG_USDC_PERP": 3,   # 0.001 PAXG (gold)
            "TAO_USDC_PERP": 3,    # 0.001 TAO
            # Medium precision
            "SOL_USDC_PERP": 2,    # 0.01 SOL
            "BNB_USDC_PERP": 2,    # 0.01 BNB
            "AAVE_USDC_PERP": 2,   # 0.01 AAVE
            "LTC_USDC_PERP": 2,    # 0.01 LTC
            "ZEC_USDC_PERP": 2,    # 0.01 ZEC
            "AVAX_USDC_PERP": 1,   # 0.1 AVAX
            "LINK_USDC_PERP": 1,   # 0.1 LINK
            "UNI_USDC_PERP": 1,    # 0.1 UNI
            "DOT_USDC_PERP": 1,    # 0.1 DOT
            "APT_USDC_PERP": 1,    # 0.1 APT
            "SUI_USDC_PERP": 1,    # 0.1 SUI
            "NEAR_USDC_PERP": 1,   # 0.1 NEAR
            "HYPE_USDC_PERP": 1,   # 0.1 HYPE
            "ONDO_USDC_PERP": 1,   # 0.1 ONDO
            "JUP_USDC_PERP": 1,    # 0.1 JUP
            "PENDLE_USDC_PERP": 1, # 0.1 PENDLE
            "ENA_USDC_PERP": 1,    # 0.1 ENA
            "TON_USDC_PERP": 1,    # 0.1 TON
            "SEI_USDC_PERP": 1,    # 0.1 SEI
            "JTO_USDC_PERP": 1,    # 0.1 JTO
            "MNT_USDC_PERP": 1,    # 0.1 MNT
            "LDO_USDC_PERP": 1,    # 0.1 LDO
            "ARB_USDC_PERP": 1,    # 0.1 ARB
            "OP_USDC_PERP": 1,     # 0.1 OP
            "ZRO_USDC_PERP": 1,    # 0.1 ZRO
            "TIA_USDC_PERP": 1,    # 0.1 TIA
            "PYTH_USDC_PERP": 1,   # 0.1 PYTH
            "IP_USDC_PERP": 1,     # 0.1 IP
            "KAITO_USDC_PERP": 1,  # 0.1 KAITO
            "WLD_USDC_PERP": 1,    # 0.1 WLD
            "TRUMP_USDC_PERP": 1,  # 0.1 TRUMP
            "BERA_USDC_PERP": 1,   # 0.1 BERA
            # Integer quantities (low-price tokens)
            "XRP_USDC_PERP": 0,    # 1 XRP
            "DOGE_USDC_PERP": 0,   # 1 DOGE
            "XLM_USDC_PERP": 0,    # 1 XLM
            "ADA_USDC_PERP": 0,    # 1 ADA
            "HBAR_USDC_PERP": 0,   # 1 HBAR
            "CRV_USDC_PERP": 0,    # 1 CRV
            "PENGU_USDC_PERP": 0,  # 1 PENGU
            "WIF_USDC_PERP": 0,    # 1 WIF
            "W_USDC_PERP": 0,      # 1 W
            "S_USDC_PERP": 0,      # 1 S
            "VIRTUAL_USDC_PERP": 0, # 1 VIRTUAL
            "AERO_USDC_PERP": 0,   # 1 AERO
            "FARTCOIN_USDC_PERP": 0, # 1 FARTCOIN
            "MON_USDC_PERP": 0,    # 1 MON
            "ASTER_USDC_PERP": 0,  # 1 ASTER
            "LINEA_USDC_PERP": 0,  # 1 LINEA
            "ZORA_USDC_PERP": 0,   # 1 ZORA
            "XPL_USDC_PERP": 0,    # 1 XPL
            "FLOCK_USDC_PERP": 0,  # 1 FLOCK
            "WLFI_USDC_PERP": 0,   # 1 WLFI
            "kBONK_USDC_PERP": 0,  # 1 kBONK (already in thousands)
            "kPEPE_USDC_PERP": 0,  # 1 kPEPE (already in thousands)
        }

        decimals = QTY_DECIMALS.get(symbol, 0)  # Default to integer for unknown symbols
        return round(quantity, decimals)

    def _round_price(self, symbol: str, price: float) -> float:
        """Round price based on symbol tick size."""
        if "BTC" in symbol:
            return round(price, 1)  # $0.1 tick
        elif "ETH" in symbol:
            return round(price, 2)  # $0.01 tick
        elif "SOL" in symbol:
            return round(price, 2)  # $0.01 tick
        elif "ZEC" in symbol:
            return round(price, 2)  # $0.01 tick
        else:
            return round(price, 4)  # Default


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

    bot = PointsFarmer(public_key, secret_key, debug=True)
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
