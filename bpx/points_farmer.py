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
- Uses Backpack private websocket for real-time fill detection
- Compares Binance and Backpack prices for optimal entry

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

# Trading pairs and their leverage
# NOTE: Only include pairs with good liquidity on Backpack
LEVERAGE: Dict[str, int] = {
    "BTC_USDC_PERP": 50,
    "ETH_USDC_PERP": 50,
    "SOL_USDC_PERP": 50,
    # ZEC removed - orderbook has 38% spread, no liquidity
    # "ZEC_USDC_PERP": 10,
    # "2Z_USDT_PERP": 10,
    # "MON_USD_PERP": 10,
}

# Map Backpack symbols to Binance stream names
BINANCE_TICKERS: Dict[str, str] = {
    "BTC_USDC_PERP": "btcusdt",
    "ETH_USDC_PERP": "ethusdt",
    "SOL_USDC_PERP": "solusdt",
}

# Reverse mapping: Binance ticker -> Backpack symbol
BINANCE_TO_BACKPACK: Dict[str, str] = {v: k for k, v in BINANCE_TICKERS.items()}

# Trading parameters
WICK_THRESHOLD = 0.001  # 0.1% price move (user's setting)
WICK_WINDOW_SECONDS = 2.0  # Time window for wick detection
LEVERAGE_USAGE = 0.30  # Use 30% of max leverage
NUM_SYMBOLS = len(LEVERAGE)  # Number of trading pairs

# Exit parameters
TP_PERCENT = 0.001  # 0.1% take profit
SL_PERCENT = 0.002  # 0.2% stop loss
PROFIT_TIMEOUT_SECONDS = 30  # Close profitable position after 30s

# Safety parameters
COOLDOWN_SECONDS = 5  # Increased cooldown per symbol after trade attempt
MAX_LOSS_PER_SYMBOL = -15.0  # Pause symbol if cumulative loss exceeds this
STALE_ORDER_TIMEOUT = 10  # Cancel unfilled orders after 10 seconds
MAX_PRICE_DEVIATION = 0.02  # 2% max deviation from Binance price

# Binance WebSocket - use combined stream endpoint
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
    wicks_detected: int = 0


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
        print("BACKPACK POINTS FARMER")
        print("=" * 60)
        print(f"Trading pairs: {list(LEVERAGE.keys())}")
        print(f"Binance feeds: {list(BINANCE_TICKERS.values())}")
        print(f"Wick threshold: {WICK_THRESHOLD * 100}%")
        print(f"TP: {TP_PERCENT * 100}% | SL: {SL_PERCENT * 100}%")
        print("=" * 60)

        try:
            # Get initial balance
            balances = await self.account.get_balances()
            usdc_balance = self._get_usdc_balance(balances)
            print(f"Starting USDC balance: ${usdc_balance:.2f}")

            # Connect to Backpack private websocket for real-time updates
            await self._connect_private_websocket()
            print("Connected to Backpack private websocket")
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
            timestamp = time.time()

            # Update price history
            state = self.states[symbol]
            state.prices.append(PricePoint(timestamp, price))
            self._last_prices[symbol] = price

            # Debug: log first few messages
            if self.debug and self._msg_count <= 5:
                print(f"[{symbol}] Price: {price}")

            # Check for wick
            await self._check_wick(symbol, state)

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            if self.debug:
                print(f"Parse error: {e}")

    # =========================================================================
    # Backpack Price & Private Websocket
    # =========================================================================

    async def _backpack_price_loop(self) -> None:
        """Fetch Backpack prices periodically to compare with Binance."""
        while self._running:
            try:
                tickers = await self.public.get_tickers()
                if isinstance(tickers, list):
                    for ticker in tickers:
                        symbol = ticker.get("symbol")
                        if symbol in LEVERAGE:
                            last_price = ticker.get("lastPrice")
                            if last_price:
                                self._backpack_prices[symbol] = float(last_price)
                elif isinstance(tickers, dict):
                    for symbol, data in tickers.items():
                        if symbol in LEVERAGE:
                            last_price = data.get("lastPrice")
                            if last_price:
                                self._backpack_prices[symbol] = float(last_price)
            except Exception as e:
                if self.debug:
                    print(f"Backpack price fetch error: {e}")
            await asyncio.sleep(1)  # Update every second

    async def _connect_private_websocket(self) -> None:
        """Connect to Backpack private websocket for real-time order/position updates."""
        import base64
        # Get the base64 encoded secret key
        private_bytes = self.account.private_key.private_bytes_raw()
        secret_key_b64 = base64.b64encode(private_bytes).decode()

        self._private_ws = PrivateWebsocket(
            public_key=self.public_key,
            secret_key=secret_key_b64,
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
                        # Place TP order
                        await self._place_tp_order(symbol, state.position)

                elif state and state.position and state.position.tp_order_id == order_id:
                    # TP order filled
                    await self._handle_tp_fill(symbol, state, float(price) if price else None)

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
        current_price = fill_price or self._last_prices.get(symbol, position.entry_price)

        # Calculate P/L
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

        pnl_sign = "+" if pnl_usdc >= 0 else ""
        print(
            f"CLOSE {symbol} (TP_FILL) | "
            f"P/L={pnl_sign}{pnl_usdc:.2f} USDC | "
            f"Volume={volume:,.0f} | "
            f"Points~{volume:,.0f}"
        )

        # Clear position
        state.position = None
        state.pending_entry_order_id = None

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

        # Calculate price change from oldest to newest in window
        oldest_price = window_prices[0].price
        newest_price = window_prices[-1].price
        price_change = (newest_price - oldest_price) / oldest_price

        # Detect wick direction
        if abs(price_change) >= WICK_THRESHOLD:
            self.stats.wicks_detected += 1
            direction = "DROP" if price_change < 0 else "SPIKE"
            print(f"[{symbol}] WICK {direction}: {price_change*100:.2f}%")

            if price_change < 0:
                # Price dropped -> go Long (buy the dip)
                await self._enter_position(symbol, Side.LONG)
            else:
                # Price spiked -> go Short (fade the pump)
                await self._enter_position(symbol, Side.SHORT)

    # =========================================================================
    # Trade Execution
    # =========================================================================

    async def _enter_position(self, symbol: str, side: Side) -> None:
        """Place a maker-only limit entry order."""
        state = self.states[symbol]

        # Set cooldown immediately to prevent duplicate signals
        state.last_trade_time = time.time()

        try:
            # Get both Binance and Backpack prices
            binance_price = self._last_prices.get(symbol)
            backpack_price = self._backpack_prices.get(symbol)

            if not binance_price:
                print(f"[{symbol}] No Binance price available")
                return

            if not backpack_price:
                # Fall back to Binance if Backpack price not available yet
                backpack_price = binance_price
                print(f"[{symbol}] Using Binance price as fallback (no Backpack price)")

            # Check price deviation between exchanges
            price_diff = abs(backpack_price - binance_price) / binance_price
            if price_diff > MAX_PRICE_DEVIATION:
                print(f"[{symbol}] Price deviation too high: {price_diff*100:.2f}% (Binance: {binance_price:.2f}, Backpack: {backpack_price:.2f})")
                return

            # USE BACKPACK PRICE for entry (this is where orders will execute!)
            # Place orders slightly inside the spread to get filled
            if side == Side.LONG:
                # Place bid slightly below Backpack's current price to be a maker
                # But close enough to get filled on the next price move
                entry_price = backpack_price * (1 - 0.0001)  # 0.01% below Backpack price
            else:
                # Place ask slightly above Backpack's current price
                entry_price = backpack_price * (1 + 0.0001)  # 0.01% above Backpack price

            # Round price to appropriate precision
            entry_price = self._round_price(symbol, entry_price)

            # Log price comparison for debugging
            if self.debug:
                print(f"[{symbol}] Binance: ${binance_price:.2f}, Backpack: ${backpack_price:.2f}, Entry: ${entry_price:.2f}")

            # Calculate position size
            balances = await self.account.get_balances()
            usdc_balance = self._get_usdc_balance(balances)

            leverage = LEVERAGE[symbol]
            notional = usdc_balance * LEVERAGE_USAGE * leverage / NUM_SYMBOLS
            quantity = notional / entry_price

            # Round quantity appropriately
            quantity = self._round_quantity(symbol, quantity)

            if quantity <= 0:
                print(f"[{symbol}] Quantity too small")
                return

            # Place maker-only limit order
            order_side = "Bid" if side == Side.LONG else "Ask"
            side_str = "Long" if side == Side.LONG else "Short"

            print(f"[{symbol}] Placing {side_str} {quantity} @ ${entry_price:.2f} (Binance: ${binance_price:.2f}, Backpack: ${backpack_price:.2f})")

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

                print(f"ENTRY {symbol} {side_str} {quantity:.6f} @ {entry_price:.2f}")
                print(f"  [{symbol}] TP @ {tp_price:.2f}, SL @ {sl_price:.2f}")
            else:
                error_msg = result.get('message', result) if isinstance(result, dict) else result
                print(f"[{symbol}] Order failed: {error_msg}")

        except Exception as e:
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
                if self.debug:
                    print(f"[{symbol}] TP order placed: {result['id']}")

        except Exception as e:
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
            print(f"Close error {symbol}: {e}")

    # =========================================================================
    # Position Monitoring
    # =========================================================================

    async def _position_monitor_loop(self) -> None:
        """Monitor positions for SL hits and profit timeouts."""
        last_position_check = 0

        while self._running:
            try:
                # Periodically poll actual positions from Backpack to sync state
                now = time.time()
                if now - last_position_check >= 2:  # Check every 2 seconds
                    last_position_check = now
                    await self._sync_positions()

                for symbol, state in self.states.items():
                    if not state.position:
                        continue

                    position = state.position

                    # Use Backpack price for SL/TP checks (that's where we're trading!)
                    current_price = self._backpack_prices.get(symbol)
                    if not current_price:
                        # Fall back to Binance price
                        current_price = self._last_prices.get(symbol)
                    if not current_price:
                        continue

                    # Skip SL/TP checks if entry order is still pending
                    if state.pending_entry_order_id:
                        continue

                    # Check SL using Backpack price
                    if position.side == Side.LONG:
                        if current_price <= position.sl_price:
                            print(f"[{symbol}] SL HIT: price {current_price:.2f} <= SL {position.sl_price:.2f}")
                            await self._close_position_market(symbol, position, "SL")
                            continue
                        in_profit = current_price > position.entry_price
                    else:
                        if current_price >= position.sl_price:
                            print(f"[{symbol}] SL HIT: price {current_price:.2f} >= SL {position.sl_price:.2f}")
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

    async def _sync_positions(self) -> None:
        """Sync local state with actual positions on Backpack."""
        try:
            positions = await self.account.get_open_positions()
            if not isinstance(positions, list):
                return

            # Build map of actual positions
            actual_positions: Dict[str, Dict] = {}
            for pos in positions:
                symbol = pos.get("symbol")
                if symbol:
                    actual_positions[symbol] = pos

            # Check each tracked symbol
            for symbol, state in self.states.items():
                actual = actual_positions.get(symbol)
                actual_size = float(actual.get("netSize", 0)) if actual else 0

                # If we think we have a position but Backpack says we don't
                if state.position and not state.pending_entry_order_id:
                    if abs(actual_size) < 0.00001:
                        # Position was closed externally (TP filled, liquidation, etc.)
                        if self.debug:
                            print(f"[{symbol}] Position closed externally, clearing state")
                        state.position = None

                # If we don't think we have a position but Backpack says we do
                # (This shouldn't happen normally, but let's log it)
                if not state.position and abs(actual_size) > 0.00001:
                    if self.debug:
                        print(f"[{symbol}] Unexpected position found: size={actual_size}")

        except Exception as e:
            if self.debug:
                print(f"Sync positions error: {e}")

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
                if abs(c) >= WICK_THRESHOLD:
                    color_prefix = "**"  # Highlight potential wick
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
        print("-" * 85)
        print(
            f"Msgs: {self._msg_count:,} | "
            f"Wicks: {self.stats.wicks_detected} | "
            f"Trades: {self.stats.total_trades} | "
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
        if "BTC" in symbol:
            return round(quantity, 4)  # 0.0001 BTC min
        elif "ETH" in symbol:
            return round(quantity, 3)  # 0.001 ETH min
        elif "SOL" in symbol:
            return round(quantity, 2)  # 0.01 SOL min
        else:
            return round(quantity, 2)  # Conservative default

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
