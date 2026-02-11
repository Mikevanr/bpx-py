"""
Backpack Exchange Volume Farmer - SKR

Volume farming bot for SKR_USD_PERP on Backpack.
Replicates Binance SKRUSDT price movements for directional trades.

Strategy:
- Monitor SKRUSDT on Binance for momentum
- When price moves, replicate direction on Backpack
- Small positions with strict $2 max loss per trade
- 5x leverage (max for SKR)

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
# Configuration - SKR ONLY
# =============================================================================

# Trading pair - BTC only
LEVERAGE: Dict[str, int] = {
    "BTC_USDC_PERP": 50,   # BTC max leverage is 50x
}

# Binance price feed
BINANCE_TICKERS: Dict[str, str] = {
    "BTC_USDC_PERP": "btcusdt",
}

# Reverse mapping
BINANCE_TO_BACKPACK: Dict[str, str] = {v: k for k, v in BINANCE_TICKERS.items()}

# =============================================================================
# SKR VOLUME FARMING PARAMETERS
# =============================================================================

# Position Sizing - use 75% of available collateral
MAX_CONCURRENT_POSITIONS = 1  # Only SKR
POSITION_SIZE_PCT = 0.75      # 75% of collateral as margin
# With 5x leverage: margin × 5 = notional

# Momentum Detection
MOMENTUM_THRESHOLD = 0.003    # 0.3% move triggers entry
MOMENTUM_WINDOW = 10.0        # 10 second window to detect momentum

# Take Profit & Stop Loss (on margin, accounting for leverage AND fees)
# Maker fee: 0.01% per side = 0.02% round trip (using postOnly to avoid speed bump)
MAKER_FEE_PCT = 0.0001        # 0.01% per side (maker)
TAKER_FEE_PCT = 0.00026       # 0.026% per side (taker - for exits)
# Entry = maker, Exit = taker (market order for reliable close)
ROUND_TRIP_FEE_PCT = MAKER_FEE_PCT + TAKER_FEE_PCT  # 0.036% for entry + exit

# margin % = notional % × leverage
TP_MARGIN_PCT = 0.02          # 2% profit on margin (AFTER fees)
SL_MARGIN_PCT = 0.04          # 4% loss on margin

# Convert to notional % for price comparison (use first symbol's leverage)
DEFAULT_LEVERAGE = list(LEVERAGE.values())[0]  # 50x for BTC

# TP must cover: fees + target profit
# TP on notional = (target margin % / leverage) + round trip fees
TP_PCT = (TP_MARGIN_PCT / DEFAULT_LEVERAGE) + ROUND_TRIP_FEE_PCT  # 0.04% + 0.052% = 0.092%
SL_PCT = SL_MARGIN_PCT / DEFAULT_LEVERAGE    # 0.08% on notional = 4% on margin

# Trailing Stop Configuration (on margin %)
TRAILING_ACTIVATION_MARGIN_PCT = 0.015   # Activate trailing after 1.5% margin profit (covers fees)
TRAILING_DISTANCE_MARGIN_PCT = 0.01      # Trail 1% behind peak (on margin)
# Convert to notional
TRAILING_ACTIVATION_PCT = TRAILING_ACTIVATION_MARGIN_PCT / DEFAULT_LEVERAGE  # ~0.03% notional
TRAILING_DISTANCE_PCT = TRAILING_DISTANCE_MARGIN_PCT / DEFAULT_LEVERAGE      # ~0.02% notional

# Grace period - don't trigger SL/TP for first N seconds (let position breathe)
GRACE_PERIOD_SECONDS = 15.0  # Wait 15 seconds before checking TP/SL

# Order Management
MAX_ORDER_AGE = 30.0          # Cancel unfilled orders after 30 seconds
MAX_POSITION_TIME = 300       # Hold up to 5 minutes

# Risk Management
MAX_DAILY_LOSS = 20.00        # Stop trading if daily loss exceeds $20
MAX_LOSS_PER_SYMBOL = -10.00  # Pause symbol if cumulative loss exceeds $10
COOLDOWN_SECONDS = 5.0        # 5 seconds between trades

# Safety
MAX_PRICE_DEVIATION = 0.01    # 1% max deviation (SKR may have wider spread)

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
    margin: float = 0.0  # Initial margin used for this position
    leverage: float = 1.0  # Leverage used for this position
    # Trailing stop tracking
    highest_price: float = 0.0  # For longs - track highest since entry
    lowest_price: float = 999999.0  # For shorts - track lowest since entry
    trailing_active: bool = False
    trailing_stop_price: float = 0.0
    highest_margin_pnl_pct: float = 0.0  # Track highest margin P/L % for trailing
    lowest_margin_pnl_pct: float = 0.0   # Track lowest margin P/L % for trailing


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
    starting_balance: float = 0.0  # Track starting balance for actual P/L
    current_balance: float = 0.0   # Track current balance


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
        print("BTC VOLUME FARMER - Follow Binance Momentum")
        print("=" * 60)
        symbol = list(LEVERAGE.keys())[0]
        leverage = LEVERAGE[symbol]
        print(f"Trading: {symbol} ({leverage}x leverage)")
        binance_ticker = BINANCE_TICKERS[symbol].upper()
        print(f"Strategy: Replicate Binance {binance_ticker} price movement")
        print(f"Position size: {POSITION_SIZE_PCT*100}% of collateral × {leverage}x leverage")
        print(f"Risk: Hard TP={TP_MARGIN_PCT*100}% | SL={SL_MARGIN_PCT*100}% on margin")
        print(f"Trailing: Activates at +{TRAILING_ACTIVATION_MARGIN_PCT*100}% | Trails {TRAILING_DISTANCE_MARGIN_PCT*100}% from peak")
        print(f"Grace period: {GRACE_PERIOD_SECONDS}s before TP/SL can trigger")
        print(f"Fees: {ROUND_TRIP_FEE_PCT*100:.3f}% round trip (maker entry + taker exit)")
        print(f"Entry: postOnly limit orders (avoids 100ms speed bump)")
        print(f"Max daily loss: ${MAX_DAILY_LOSS}")
        print(f"Momentum: {MOMENTUM_THRESHOLD*100}% move in {MOMENTUM_WINDOW}s triggers entry")
        print("=" * 60)

        try:
            # Get initial balance from collateral (for perps trading)
            collateral = await self.account.get_collateral()
            usdc_balance = self._get_usdc_balance(collateral)
            self.stats.starting_balance = usdc_balance
            self.stats.current_balance = usdc_balance
            print(f"Starting USDC balance (perps): ${usdc_balance:.2f}")

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

            # Check for grid entry opportunity
            await self._check_grid_entry(symbol, state)

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            if self.debug:
                print(f"Parse error: {e}")

    # =========================================================================
    # Backpack Price & Private Websocket
    # =========================================================================

    async def _backpack_price_loop(self) -> None:
        """Fetch Backpack prices periodically and detect wicks for non-Binance tokens."""
        first_run = True
        while self._running:
            try:
                tickers = await self.public.get_tickers()
                timestamp = time.time()

                # Debug: print all available perp symbols on first run
                if first_run:
                    first_run = False
                    if isinstance(tickers, list):
                        all_symbols = [t.get("symbol") for t in tickers]
                        perp_symbols = [s for s in all_symbols if s and "PERP" in s]
                        print(f"[DEBUG] All Backpack perp symbols ({len(perp_symbols)}): {perp_symbols[:20]}...")
                        skr_symbols = [s for s in all_symbols if s and "SKR" in s.upper()]
                        print(f"[DEBUG] SKR symbols found: {skr_symbols}")
                    elif isinstance(tickers, dict):
                        perp_symbols = [s for s in tickers.keys() if "PERP" in s]
                        print(f"[DEBUG] All Backpack perp symbols ({len(perp_symbols)}): {perp_symbols[:20]}...")
                        skr_symbols = [s for s in tickers.keys() if "SKR" in s.upper()]
                        print(f"[DEBUG] SKR symbols found: {skr_symbols}")

                if isinstance(tickers, list):
                    for ticker in tickers:
                        symbol = ticker.get("symbol")
                        if symbol in LEVERAGE:
                            last_price = ticker.get("lastPrice")
                            if last_price:
                                price = float(last_price)
                                self._backpack_prices[symbol] = price

                                # For tokens without Binance feed, use Backpack for grid entry
                                if symbol not in BINANCE_TICKERS:
                                    state = self.states[symbol]
                                    state.prices.append(PricePoint(timestamp, price))
                                    self._last_prices[symbol] = price
                                    await self._check_grid_entry(symbol, state)

                elif isinstance(tickers, dict):
                    for symbol, data in tickers.items():
                        if symbol in LEVERAGE:
                            last_price = data.get("lastPrice")
                            if last_price:
                                price = float(last_price)
                                self._backpack_prices[symbol] = price

                                # For tokens without Binance feed, use Backpack for grid entry
                                if symbol not in BINANCE_TICKERS:
                                    state = self.states[symbol]
                                    state.prices.append(PricePoint(timestamp, price))
                                    self._last_prices[symbol] = price
                                    await self._check_grid_entry(symbol, state)
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
            order_side = data.get("side")
            filled_qty = float(data.get("executedQuantity", "0") or 0)
            price = float(data.get("price", "0") or 0)

            if self.debug:
                print(f"[WS ORDER] {symbol} {status}: {order_side} {filled_qty} @ {price}")

            state = self.states.get(symbol)
            if not state:
                return

            if status == "Filled":
                # Check if this is a pending entry order
                if state.pending_entry_order_id == order_id:
                    # Entry order filled - create position and place TP
                    pending_info = self._pending_fills.get(order_id, {})
                    side = pending_info.get("side", Side.LONG if order_side == "Bid" else Side.SHORT)
                    margin = pending_info.get("margin", 0)
                    leverage = pending_info.get("leverage", LEVERAGE.get(symbol, 50))
                    quantity = filled_qty if filled_qty > 0 else pending_info.get("quantity", 0)
                    fill_price = price if price > 0 else pending_info.get("price", 0)

                    await self._on_entry_filled(symbol, side, fill_price, quantity, order_id, margin, leverage)

                    # Clean up pending fill info
                    if order_id in self._pending_fills:
                        del self._pending_fills[order_id]

                # Check if this is a TP order
                elif state.position and state.position.tp_order_id == order_id:
                    # TP filled - handled by position monitor, but log it
                    print(f"[WS] TP FILLED {symbol} @ {price}")

            elif status == "Cancelled":
                if state.pending_entry_order_id == order_id:
                    state.pending_entry_order_id = None
                    state.pending_entry_time = None
                    if order_id in self._pending_fills:
                        del self._pending_fills[order_id]
                    if self.debug:
                        print(f"[{symbol}] Entry order cancelled")
                elif state.position and state.position.tp_order_id == order_id:
                    state.position.tp_order_id = None
                    if self.debug:
                        print(f"[{symbol}] TP order cancelled")

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
        self.stats.daily_pnl += pnl_usdc
        self.stats.total_volume += volume
        self.stats.total_points += volume
        self.stats.total_trades += 1

        if pnl_usdc >= 0:
            self.stats.wins += 1
        else:
            self.stats.losses += 1

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
        self.stats.daily_pnl += pnl_usdc
        self.stats.total_volume += volume
        self.stats.total_points += volume
        self.stats.total_trades += 1

        if pnl_usdc >= 0:
            self.stats.wins += 1
        else:
            self.stats.losses += 1

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

    async def _check_grid_entry(self, symbol: str, state: SymbolState) -> None:
        """Follow Binance momentum - replicate price movement direction."""
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
            if state.pending_entry_time and time.time() - state.pending_entry_time > MAX_ORDER_AGE:
                try:
                    await self.account.cancel_order(symbol=symbol, order_id=state.pending_entry_order_id)
                    print(f"[{symbol}] Cancelled stale entry order")
                except Exception:
                    pass
                state.pending_entry_order_id = None
                state.pending_entry_time = None
            return

        # Need enough price history
        now = time.time()
        prices = state.prices
        window_prices = [p for p in prices if now - p.timestamp <= MOMENTUM_WINDOW]

        if len(window_prices) < 3:
            return

        # Calculate momentum from Binance prices
        oldest_price = window_prices[0].price
        newest_price = window_prices[-1].price
        momentum = (newest_price - oldest_price) / oldest_price

        # Check if momentum exceeds threshold
        if abs(momentum) < MOMENTUM_THRESHOLD:
            return

        # Get current Backpack price for execution
        backpack_price = self._backpack_prices.get(symbol)
        if not backpack_price:
            return

        # Validate prices are roughly in sync
        binance_price = self._last_prices.get(symbol)
        if binance_price:
            price_diff = abs(backpack_price - binance_price) / binance_price
            if price_diff > MAX_PRICE_DEVIATION:
                return

        self.stats.signals_detected += 1
        direction = "UP" if momentum > 0 else "DOWN"

        # FOLLOW the momentum (not fade it)
        if momentum > 0:
            side = Side.LONG
            print(f"[{symbol}] MOMENTUM {direction}: {momentum*100:.2f}% → LONG")
        else:
            side = Side.SHORT
            print(f"[{symbol}] MOMENTUM {direction}: {momentum*100:.2f}% → SHORT")

        # Enter position
        await self._enter_position_maker(symbol, side, backpack_price, backpack_price, backpack_price)

    # =========================================================================
    # Trade Execution (Grid Market Maker)
    # =========================================================================

    async def _enter_position_maker(
        self, symbol: str, side: Side, best_bid: float, best_ask: float, mid_price: float
    ) -> None:
        """Enter position with maker-only limit order."""
        state = self.states[symbol]
        side_str = "Long" if side == Side.LONG else "Short"

        # Check max concurrent positions limit
        current_positions = sum(1 for s in self.states.values() if s.position is not None)
        if current_positions >= MAX_CONCURRENT_POSITIONS:
            return

        try:
            # Get collateral and calculate position size (75% of collateral)
            collateral = await self.account.get_collateral()
            usdc_balance = self._get_usdc_balance(collateral)

            if usdc_balance <= 0:
                print(f"[{symbol}] No collateral available")
                return

            leverage = LEVERAGE[symbol]  # 5x for SKR
            margin = usdc_balance * POSITION_SIZE_PCT  # 75% of collateral
            notional = margin * leverage  # margin × 5x

            print(f"[{symbol}] Collateral: ${usdc_balance:.2f} | Using ${margin:.2f} margin | ${notional:.2f} notional")

            entry_price = mid_price

            if side == Side.LONG:
                order_side = "Bid"
            else:
                order_side = "Ask"

            quantity = notional / entry_price
            quantity = self._round_quantity(symbol, quantity)
            entry_price = self._round_price(symbol, entry_price)

            if quantity <= 0:
                return

            # Format quantity based on symbol precision
            qty_str = str(quantity)

            # Use postOnly limit order to AVOID the 100ms speed bump
            # Price at mid to maximize fill chance while still being maker
            limit_price = entry_price
            limit_price_str = str(self._round_price(symbol, limit_price))

            print(f"[{symbol}] LIMIT (postOnly) {side_str} qty={qty_str} @ ${limit_price:.2f} (${notional:.0f} notional)")

            # Place limit order with postOnly to avoid 100ms speed bump
            result = await self.account.execute_order(
                symbol=symbol,
                side=order_side,
                order_type="Limit",
                quantity=qty_str,
                price=limit_price_str,
                post_only=True,
            )

            # Debug: log the API response
            print(f"[{symbol}] API Response: {result}", flush=True)

            if isinstance(result, dict) and result.get("id"):
                order_id = result["id"]
                order_status = result.get("status", "")

                if order_status == "Filled":
                    # Immediate fill (unlikely with postOnly but possible)
                    fill_price = float(result.get("price") or result.get("avgPrice") or limit_price)
                    fill_qty = float(result.get("executedQuantity") or quantity)
                    await self._on_entry_filled(symbol, side, fill_price, fill_qty, order_id, margin, leverage)
                elif order_status == "Cancelled":
                    # postOnly rejected - would have taken liquidity
                    print(f"[{symbol}] postOnly rejected (would cross spread), retrying...")
                else:
                    # Order is pending - wait for fill via websocket or sync
                    state.pending_entry_order_id = order_id
                    state.pending_entry_time = time.time()
                    self._pending_fills[order_id] = {
                        "symbol": symbol,
                        "side": side,
                        "quantity": quantity,
                        "price": limit_price,
                        "margin": margin,
                        "leverage": leverage,
                    }
                    print(f"[{symbol}] Order pending, waiting for fill...")
            else:
                error_msg = result.get("message", str(result)) if isinstance(result, dict) else str(result)
                print(f"[{symbol}] Order error: {error_msg}")

        except Exception as e:
            print(f"[{symbol}] Entry error: {e}")

    async def _on_entry_filled(
        self, symbol: str, side: Side, fill_price: float, quantity: float,
        order_id: str, margin: float, leverage: float
    ) -> None:
        """Handle entry order fill - create position and place TP order."""
        state = self.states[symbol]
        state.pending_entry_order_id = None
        state.pending_entry_time = None
        state.last_trade_time = time.time()

        notional = fill_price * quantity
        side_str = "LONG" if side == Side.LONG else "SHORT"
        print(f"[{symbol}] FILLED {side_str} @ ${fill_price:.2f}")

        # Calculate TP price based on percentage target
        if side == Side.LONG:
            tp_price = fill_price * (1 + TP_PCT)
        else:
            tp_price = fill_price * (1 - TP_PCT)

        tp_price = self._round_price(symbol, tp_price)

        # Create position
        state.position = Position(
            symbol=symbol,
            side=side,
            entry_price=fill_price,
            quantity=quantity,
            entry_time=time.time(),
            order_id=order_id,
            tp_price=tp_price,
            notional=notional,
            margin=margin,
            leverage=leverage,
        )

        # Don't place TP order - we use trailing stop instead
        # The position will be closed by the monitor loop

        self.stats.total_trades += 1
        self.stats.total_volume += notional

    async def _place_tp_order_maker(self, symbol: str, position: Position) -> None:
        """Place take-profit order with post_only for maker fees."""
        try:
            close_side = "Ask" if position.side == Side.LONG else "Bid"
            tp_price = self._round_price(symbol, position.tp_price)

            result = await self.account.execute_order(
                symbol=symbol,
                side=close_side,
                order_type="Limit",
                quantity=str(position.quantity),
                price=str(tp_price),
                post_only=True,
                reduce_only=True,
            )

            if isinstance(result, dict) and result.get("id"):
                position.tp_order_id = result["id"]
                print(f"[{symbol}] TP order placed @ ${tp_price:.2f} (maker-only)")
            else:
                error_msg = result.get("message", str(result)) if isinstance(result, dict) else str(result)
                print(f"[{symbol}] TP order failed: {error_msg}")

        except Exception as e:
            print(f"[{symbol}] TP order error: {e}")

    async def _enter_position(self, symbol: str, side: Side) -> None:
        """Legacy entry function - not used in grid market maker mode."""
        # This function is kept for compatibility but not used
        # Grid market maker uses _enter_position_maker directly
        pass

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
            self.stats.daily_pnl += pnl_usdc
            self.stats.total_volume += volume
            self.stats.total_points += volume
            self.stats.total_trades += 1

            if pnl_usdc >= 0:
                self.stats.wins += 1
            else:
                self.stats.losses += 1

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
            self.stats.daily_pnl += pnl_usdc
            self.stats.total_volume += volume
            self.stats.total_points += volume
            self.stats.total_trades += 1

            if pnl_usdc >= 0:
                self.stats.wins += 1
            else:
                self.stats.losses += 1

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
        """Monitor positions for grid market maker - check TP fills and emergency exits."""
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

                    # Calculate P/L (on notional for TP/SL checks)
                    if position.side == Side.LONG:
                        unrealized_pnl = (current_price - position.entry_price) * position.quantity
                        pnl_pct = (current_price - position.entry_price) / position.entry_price
                    else:
                        unrealized_pnl = (position.entry_price - current_price) * position.quantity
                        pnl_pct = (position.entry_price - current_price) / position.entry_price

                    # Margin P/L % = notional P/L % × leverage
                    leverage = position.leverage if position.leverage else LEVERAGE.get(symbol, 5)
                    margin_pnl_pct = pnl_pct * leverage

                    # Calculate fee threshold on margin (must clear this to be profitable)
                    fee_threshold_margin = ROUND_TRIP_FEE_PCT * leverage  # ~2.6% on margin with 50x

                    time_held = now - position.entry_time

                    # ========== TRACK PEAK P/L FOR TRAILING STOP ==========
                    if margin_pnl_pct > position.highest_margin_pnl_pct:
                        position.highest_margin_pnl_pct = margin_pnl_pct

                    # ========== TRAILING STOP ACTIVATION ==========
                    if not position.trailing_active and margin_pnl_pct >= TRAILING_ACTIVATION_MARGIN_PCT:
                        position.trailing_active = True
                        print(f"[{symbol}] TRAILING ACTIVATED at {margin_pnl_pct*100:+.1f}% margin profit")

                    # ========== GRACE PERIOD - Skip all exit checks during grace period ==========
                    if time_held < GRACE_PERIOD_SECONDS:
                        # Only log occasionally during grace period
                        if int(time_held) % 5 == 0:
                            print(f"[{symbol}] Grace period: {time_held:.0f}s / {GRACE_PERIOD_SECONDS:.0f}s | {margin_pnl_pct*100:+.1f}% margin (peak={position.highest_margin_pnl_pct*100:.1f}%)")
                        continue  # Skip ALL exit checks during grace period

                    # ========== TRAILING STOP CHECK (only after grace period) ==========
                    if position.trailing_active:
                        # Calculate how far we've dropped from peak
                        drawdown_from_peak = position.highest_margin_pnl_pct - margin_pnl_pct

                        if drawdown_from_peak >= TRAILING_DISTANCE_MARGIN_PCT:
                            # Only close if current profit > fees (net positive)
                            if margin_pnl_pct > fee_threshold_margin:
                                print(f"[{symbol}] CLOSING: Trailing stop (peak={position.highest_margin_pnl_pct*100:.1f}%, now={margin_pnl_pct*100:.1f}%, drop={drawdown_from_peak*100:.1f}%)")
                                await self._close_position_emergency(
                                    symbol, position,
                                    f"TRAIL (peak {position.highest_margin_pnl_pct*100:.1f}%)",
                                    unrealized_pnl
                                )
                                continue
                            else:
                                # Don't close yet - profit doesn't cover fees
                                print(f"[{symbol}] Trail triggered but profit ({margin_pnl_pct*100:.1f}%) < fees ({fee_threshold_margin*100:.1f}%), holding...")

                    # ========== HARD TAKE PROFIT (safety cap) ==========
                    if pnl_pct >= TP_PCT:
                        print(f"[{symbol}] CLOSING: Hard TP hit ({pnl_pct*100:.3f}% >= {TP_PCT*100:.3f}%)")
                        await self._close_position_emergency(symbol, position, "TP", unrealized_pnl)
                        continue

                    # ========== STOP LOSS ==========
                    if pnl_pct <= -SL_PCT:
                        print(f"[{symbol}] CLOSING: SL hit ({pnl_pct*100:.3f}% <= -{SL_PCT*100:.3f}%)")
                        await self._close_position_emergency(symbol, position, "SL", unrealized_pnl)
                        continue

                    # ========== TIMEOUT ==========
                    if time_held >= MAX_POSITION_TIME:
                        print(f"[{symbol}] CLOSING: Timeout ({time_held:.0f}s >= {MAX_POSITION_TIME}s)")
                        await self._close_position_emergency(symbol, position, "TIMEOUT", unrealized_pnl)
                        continue

                    # Log status every 10 seconds
                    if int(time_held) % 10 == 0 and int(time_held) > 0:
                        side_str = "L" if position.side == Side.LONG else "S"
                        trail_status = "TRAIL" if position.trailing_active else ""
                        peak_info = f" peak={position.highest_margin_pnl_pct*100:.1f}%" if position.trailing_active else ""
                        print(f"[{symbol}] {side_str} | {margin_pnl_pct*100:+.1f}% on margin (${unrealized_pnl:+.2f}){peak_info} | {trail_status} {time_held:.0f}s")

            except Exception as e:
                print(f"Monitor error: {e}")

            await asyncio.sleep(0.2)  # Check every 200ms

    async def _on_tp_filled(self, symbol: str, position: Position, expected_pnl: float) -> None:
        """Handle TP order fill - update stats (maker fees already paid)."""
        state = self.states[symbol]

        # Calculate actual PnL based on TP price
        if position.side == Side.LONG:
            actual_pnl = (position.tp_price - position.entry_price) * position.quantity
        else:
            actual_pnl = (position.entry_price - position.tp_price) * position.quantity

        # Update stats
        self.stats.total_pnl += actual_pnl
        self.stats.daily_pnl += actual_pnl
        self.stats.total_volume += position.notional  # Exit volume
        self.stats.wins += 1
        state.cumulative_pnl += actual_pnl

        # Log the TP
        side_str = "LONG" if position.side == Side.LONG else "SHORT"
        win_rate = self.stats.wins / self.stats.total_trades * 100 if self.stats.total_trades > 0 else 0

        print(f"TP HIT {symbol} {side_str} | Entry=${position.entry_price:.2f} TP=${position.tp_price:.2f} | +${actual_pnl:.2f} | W/L: {self.stats.wins}/{self.stats.losses} ({win_rate:.0f}%) [MAKER]")

        # Clear position
        state.position = None

    async def _close_position_emergency(self, symbol: str, position: Position, reason: str, pnl: float) -> None:
        """Emergency close with market order (pays taker fee)."""
        state = self.states[symbol]

        try:
            # Cancel TP order first
            if position.tp_order_id:
                try:
                    await self.account.cancel_order(symbol=symbol, order_id=position.tp_order_id)
                except Exception:
                    pass

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

            # Recalculate gross PnL for logging
            if position.side == Side.LONG:
                gross_pnl = (close_price - position.entry_price) * position.quantity
            else:
                gross_pnl = (position.entry_price - close_price) * position.quantity

            # Fetch ACTUAL balance after close (this includes all fees)
            await asyncio.sleep(0.5)  # Brief wait for balance to update
            collateral = await self.account.get_collateral()
            new_balance = self._get_usdc_balance(collateral)
            self.stats.current_balance = new_balance

            # Calculate ACTUAL P/L from balance change
            actual_session_pnl = new_balance - self.stats.starting_balance

            # Update trade count and win/loss
            self.stats.total_volume += position.notional
            if gross_pnl >= 0:
                self.stats.wins += 1
            else:
                self.stats.losses += 1

            # Log the close with ACTUAL balance
            side_str = "LONG" if position.side == Side.LONG else "SHORT"
            win_rate = self.stats.wins / self.stats.total_trades * 100 if self.stats.total_trades > 0 else 0

            print(f"CLOSE {symbol} [{reason}] {side_str} | Entry=${position.entry_price:.2f} Exit=${close_price:.2f}")
            print(f"    Gross P/L: ${gross_pnl:+.2f}")
            print(f"    Balance: ${new_balance:.2f} | Session P/L: ${actual_session_pnl:+.2f}")
            print(f"    Trades: {self.stats.total_trades} | W/L: {self.stats.wins}/{self.stats.losses} ({win_rate:.0f}%)")

            # Clear position
            state.position = None

        except Exception as e:
            print(f"Emergency close error {symbol}: {e}")

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
                            # TP/SL based on USD
                            print(f"    TP: +{TP_MARGIN_PCT*100}% on margin | SL: -{SL_MARGIN_PCT*100}% on margin")
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
                                self.stats.daily_pnl += pnl_usdc
                                self.stats.total_volume += volume
                                self.stats.total_points += volume
                                self.stats.total_trades += 1

                                if pnl_usdc >= 0:
                                    self.stats.wins += 1
                                else:
                                    self.stats.losses += 1

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

            # Get leverage for this symbol (5x for SKR)
            leverage = LEVERAGE.get(symbol, 5)
            margin = notional_value / leverage

            # Create position object with margin tracking
            state.position = Position(
                symbol=symbol,
                side=side,
                entry_price=entry_price,
                quantity=quantity,
                entry_time=time.time(),  # We don't know actual entry time
                notional=notional_value,
                margin=margin,
                leverage=leverage,
                highest_price=entry_price,
                lowest_price=entry_price,
            )

            side_str = "Long" if side == Side.LONG else "Short"
            print(f"*** ADOPTED POSITION: {symbol} {side_str} {quantity:.6f} @ {entry_price:.2f} ***")
            print(f"    Notional: ${notional_value:.2f} | Margin: ${margin:.2f} | Leverage: {leverage:.0f}x")
            print(f"    TP: +{TP_MARGIN_PCT*100}% on margin | SL: -{SL_MARGIN_PCT*100}% on margin")

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
                    if time.time() - state.pending_entry_time > MAX_ORDER_AGE:
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
        actual_pnl = self.stats.current_balance - self.stats.starting_balance
        print("-" * 85)
        print(
            f"Signals: {self.stats.signals_detected} | "
            f"Trades: {self.stats.total_trades} | "
            f"W/L: {self.stats.wins}/{self.stats.losses} ({win_rate:.0f}%) | "
            f"Volume: ${self.stats.total_volume:,.0f}"
        )
        print(
            f"Balance: ${self.stats.current_balance:.2f} | "
            f"Started: ${self.stats.starting_balance:.2f} | "
            f"ACTUAL P/L: ${actual_pnl:+.2f}"
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

    def _get_usdc_balance(self, collateral: Any) -> float:
        """Extract available equity from collateral response for perps trading."""
        if isinstance(collateral, dict):
            # For perps trading, use netEquityAvailable from collateral endpoint
            net_equity_available = collateral.get("netEquityAvailable")
            if net_equity_available is not None:
                return float(net_equity_available)
            # Fallback to netEquity if netEquityAvailable not present
            net_equity = collateral.get("netEquity")
            if net_equity is not None:
                return float(net_equity)
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
            # USDC-based perps
            "SKR_USDC_PERP": 0,    # 1 SKR (integer quantities)
        }

        decimals = QTY_DECIMALS.get(symbol, 0)  # Default to integer for unknown symbols
        if decimals == 0:
            return int(quantity)  # Return as integer, not float with .0
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
        elif "SKR" in symbol:
            return round(price, 6)  # $0.000001 tick for small price tokens
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
