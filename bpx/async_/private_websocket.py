import asyncio
import json
from typing import Optional, Callable, Awaitable, List, Dict, Any, Union

# Handle websockets version compatibility (v13+ vs older)
try:
    from websockets.asyncio.client import connect, ClientConnection
except ImportError:
    from websockets import connect
    from websockets.client import WebSocketClientProtocol as ClientConnection

from websockets.exceptions import ConnectionClosed

from bpx.base.base_private_websocket import BasePrivateWebsocket


class PrivateWebsocket(BasePrivateWebsocket):
    """
    Async private websocket client for Backpack Exchange.
    Provides real-time account updates including order updates, position updates, and RFQ updates.

    Usage:
        async def on_message(message):
            print(message)

        ws = PrivateWebsocket(public_key, secret_key)
        await ws.connect()
        await ws.subscribe_order_updates(on_message)

    https://docs.backpack.exchange/#tag/Streams
    """

    # Stream name constants
    STREAM_ORDER_UPDATE = "account.orderUpdate"
    STREAM_POSITION_UPDATE = "account.positionUpdate"
    STREAM_RFQ_UPDATE = "account.rfqUpdate"

    def __init__(
        self,
        public_key: str,
        secret_key: str,
        window: int = 5000,
        debug: bool = False,
        ping_interval: Optional[float] = 20,
        ping_timeout: Optional[float] = 20,
    ):
        """
        Initialize the private websocket client.

        Args:
            public_key: Your API public key
            secret_key: Your API secret key (base64 encoded)
            window: Request window in milliseconds (default 5000, max 60000)
            debug: Enable debug logging
            ping_interval: Interval between ping messages in seconds (default 20)
            ping_timeout: Timeout for ping response in seconds (default 20)
        """
        super().__init__(public_key, secret_key, window, debug)
        self._connection: Optional[ClientConnection] = None
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._handlers: Dict[str, List[Callable[[Dict[str, Any]], Awaitable[None]]]] = {}
        self._subscribed_streams: List[str] = []
        self._running = False
        self._listen_task: Optional[asyncio.Task] = None

    @property
    def is_connected(self) -> bool:
        """Check if websocket is currently connected."""
        if self._connection is None:
            return False
        # Handle both websockets v13+ (state.name) and older versions (open property)
        if hasattr(self._connection, "state"):
            return self._connection.state.name == "OPEN"
        return getattr(self._connection, "open", False)

    async def connect(self) -> None:
        """
        Establish websocket connection to Backpack Exchange.
        """
        if self.is_connected:
            return

        self._connection = await connect(
            self.BPX_WS_URL,
            ping_interval=self._ping_interval,
            ping_timeout=self._ping_timeout,
        )
        self._running = True

        if self.debug:
            print(f"Connected to {self.BPX_WS_URL}")

    async def disconnect(self) -> None:
        """
        Close the websocket connection.
        """
        self._running = False

        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass

        if self._connection:
            await self._connection.close()
            self._connection = None
            self._subscribed_streams = []

        if self.debug:
            print("Disconnected from websocket")

    async def subscribe(
        self,
        streams: List[str],
        handler: Callable[[Dict[str, Any]], Awaitable[None]],
    ) -> None:
        """
        Subscribe to one or more private streams.

        Args:
            streams: List of stream names to subscribe to
            handler: Async callback function to handle incoming messages
        """
        if not self.is_connected:
            await self.connect()

        # Register handlers for each stream
        for stream in streams:
            if stream not in self._handlers:
                self._handlers[stream] = []
            if handler not in self._handlers[stream]:
                self._handlers[stream].append(handler)

        # Only subscribe to streams not already subscribed
        new_streams = [s for s in streams if s not in self._subscribed_streams]

        if new_streams:
            payload = self._create_subscription_payload(new_streams)
            await self._send(payload)
            self._subscribed_streams.extend(new_streams)

            if self.debug:
                print(f"Subscribed to streams: {new_streams}")

    async def unsubscribe(self, streams: List[str]) -> None:
        """
        Unsubscribe from one or more private streams.

        Args:
            streams: List of stream names to unsubscribe from
        """
        if not self.is_connected:
            return

        streams_to_unsub = [s for s in streams if s in self._subscribed_streams]

        if streams_to_unsub:
            payload = self._create_unsubscription_payload(streams_to_unsub)
            await self._send(payload)

            for stream in streams_to_unsub:
                self._subscribed_streams.remove(stream)
                if stream in self._handlers:
                    del self._handlers[stream]

            if self.debug:
                print(f"Unsubscribed from streams: {streams_to_unsub}")

    async def subscribe_order_updates(
        self,
        handler: Callable[[Dict[str, Any]], Awaitable[None]],
        symbol: Optional[str] = None,
    ) -> None:
        """
        Subscribe to order update stream.

        Args:
            handler: Async callback function to handle order updates
            symbol: Optional symbol to filter updates (e.g., "SOL_USDC")

        https://docs.backpack.exchange/#tag/Streams/Order-Update
        """
        stream = (
            f"{self.STREAM_ORDER_UPDATE}.{symbol}"
            if symbol
            else self.STREAM_ORDER_UPDATE
        )
        await self.subscribe([stream], handler)

    async def subscribe_position_updates(
        self,
        handler: Callable[[Dict[str, Any]], Awaitable[None]],
    ) -> None:
        """
        Subscribe to position update stream.

        Args:
            handler: Async callback function to handle position updates

        https://docs.backpack.exchange/#tag/Streams/Position-Update
        """
        await self.subscribe([self.STREAM_POSITION_UPDATE], handler)

    async def subscribe_rfq_updates(
        self,
        handler: Callable[[Dict[str, Any]], Awaitable[None]],
    ) -> None:
        """
        Subscribe to RFQ (Request for Quote) update stream.

        Args:
            handler: Async callback function to handle RFQ updates

        https://docs.backpack.exchange/#tag/Streams/RFQ-Update
        """
        await self.subscribe([self.STREAM_RFQ_UPDATE], handler)

    async def subscribe_all(
        self,
        handler: Callable[[Dict[str, Any]], Awaitable[None]],
    ) -> None:
        """
        Subscribe to all private streams (order updates, position updates, RFQ updates).

        Args:
            handler: Async callback function to handle all updates
        """
        streams = [
            self.STREAM_ORDER_UPDATE,
            self.STREAM_POSITION_UPDATE,
            self.STREAM_RFQ_UPDATE,
        ]
        await self.subscribe(streams, handler)

    async def listen(self) -> None:
        """
        Start listening for messages and dispatch to registered handlers.
        This is a blocking call that runs until disconnect() is called.
        """
        if not self.is_connected:
            await self.connect()

        self._running = True

        try:
            async for message in self._connection:
                if not self._running:
                    break
                await self._handle_message(message)
        except ConnectionClosed as e:
            if self.debug:
                print(f"Connection closed: {e}")
            self._running = False
        except Exception as e:
            if self.debug:
                print(f"Error in listen loop: {e}")
            raise

    async def start(self) -> None:
        """
        Start the websocket listener in the background.
        Use this when you want to continue executing other code.
        """
        if self._listen_task is None or self._listen_task.done():
            self._listen_task = asyncio.create_task(self.listen())

    async def _send(self, payload: dict) -> None:
        """Send a message to the websocket."""
        if not self.is_connected:
            raise ConnectionError("Websocket is not connected")

        message = json.dumps(payload)
        await self._connection.send(message)

        if self.debug:
            print(f"Sent: {message}")

    async def _handle_message(self, raw_message: Union[str, bytes]) -> None:
        """Process incoming message and dispatch to appropriate handlers."""
        try:
            if isinstance(raw_message, bytes):
                raw_message = raw_message.decode("utf-8")

            message = json.loads(raw_message)

            if self.debug:
                print(f"Received: {message}")

            # Handle subscription confirmation
            if message.get("result") is not None:
                if self.debug:
                    print(f"Subscription response: {message}")
                return

            # Handle error responses
            if "error" in message:
                if self.debug:
                    print(f"Error from server: {message['error']}")
                return

            # Dispatch to handlers based on stream
            stream = message.get("stream")
            if stream:
                # Check for exact match first
                if stream in self._handlers:
                    for handler in self._handlers[stream]:
                        await handler(message)

                # Check for base stream match (e.g., account.orderUpdate matches account.orderUpdate.SOL_USDC)
                base_stream = stream.rsplit(".", 1)[0] if "." in stream else stream
                if base_stream in self._handlers and base_stream != stream:
                    for handler in self._handlers[base_stream]:
                        await handler(message)

        except json.JSONDecodeError as e:
            if self.debug:
                print(f"Failed to parse message: {e}")
        except Exception as e:
            if self.debug:
                print(f"Error handling message: {e}")
            raise

    async def __aenter__(self) -> "PrivateWebsocket":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.disconnect()
