import asyncio
import threading
from typing import Optional, Callable, List, Dict, Any
from queue import Queue, Empty

from bpx.base.base_private_websocket import BasePrivateWebsocket
from bpx.async_.private_websocket import PrivateWebsocket as AsyncPrivateWebsocket


class PrivateWebsocket(BasePrivateWebsocket):
    """
    Synchronous private websocket client for Backpack Exchange.
    Provides real-time account updates including order updates, position updates, and RFQ updates.

    This is a synchronous wrapper around the async websocket client that runs
    the event loop in a background thread.

    Usage:
        def on_message(message):
            print(message)

        ws = PrivateWebsocket(public_key, secret_key)
        ws.connect()
        ws.subscribe_order_updates(on_message)
        ws.listen()  # Blocking call

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
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._async_ws: Optional[AsyncPrivateWebsocket] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._handlers: Dict[str, List[Callable[[Dict[str, Any]], None]]] = {}
        self._message_queue: Queue = Queue()
        self._running = False

    @property
    def is_connected(self) -> bool:
        """Check if websocket is currently connected."""
        return self._async_ws is not None and self._async_ws.is_connected

    def connect(self) -> None:
        """
        Establish websocket connection to Backpack Exchange.
        """
        if self.is_connected:
            return

        # Create a new event loop for the background thread
        self._loop = asyncio.new_event_loop()
        self._async_ws = AsyncPrivateWebsocket(
            public_key=self.public_key,
            secret_key=self._get_secret_key_b64(),
            window=self.window,
            debug=self.debug,
            ping_interval=self._ping_interval,
            ping_timeout=self._ping_timeout,
        )

        # Start the event loop in a background thread
        self._thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self._thread.start()

        # Connect in the background thread
        future = asyncio.run_coroutine_threadsafe(
            self._async_ws.connect(), self._loop
        )
        future.result(timeout=30)

        if self.debug:
            print(f"Connected to {self.BPX_WS_URL}")

    def disconnect(self) -> None:
        """
        Close the websocket connection.
        """
        self._running = False

        if self._async_ws and self._loop:
            future = asyncio.run_coroutine_threadsafe(
                self._async_ws.disconnect(), self._loop
            )
            try:
                future.result(timeout=10)
            except Exception:
                pass

        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

        self._async_ws = None
        self._loop = None
        self._thread = None

        if self.debug:
            print("Disconnected from websocket")

    def subscribe(
        self,
        streams: List[str],
        handler: Callable[[Dict[str, Any]], None],
    ) -> None:
        """
        Subscribe to one or more private streams.

        Args:
            streams: List of stream names to subscribe to
            handler: Callback function to handle incoming messages
        """
        if not self.is_connected:
            self.connect()

        # Register sync handlers
        for stream in streams:
            if stream not in self._handlers:
                self._handlers[stream] = []
            if handler not in self._handlers[stream]:
                self._handlers[stream].append(handler)

        # Create async wrapper that puts messages in the queue
        async def async_handler(message: Dict[str, Any]) -> None:
            self._message_queue.put((streams[0], message))

        future = asyncio.run_coroutine_threadsafe(
            self._async_ws.subscribe(streams, async_handler), self._loop
        )
        future.result(timeout=30)

    def unsubscribe(self, streams: List[str]) -> None:
        """
        Unsubscribe from one or more private streams.

        Args:
            streams: List of stream names to unsubscribe from
        """
        if not self.is_connected:
            return

        for stream in streams:
            if stream in self._handlers:
                del self._handlers[stream]

        future = asyncio.run_coroutine_threadsafe(
            self._async_ws.unsubscribe(streams), self._loop
        )
        future.result(timeout=30)

    def subscribe_order_updates(
        self,
        handler: Callable[[Dict[str, Any]], None],
        symbol: Optional[str] = None,
    ) -> None:
        """
        Subscribe to order update stream.

        Args:
            handler: Callback function to handle order updates
            symbol: Optional symbol to filter updates (e.g., "SOL_USDC")

        https://docs.backpack.exchange/#tag/Streams/Order-Update
        """
        stream = (
            f"{self.STREAM_ORDER_UPDATE}.{symbol}"
            if symbol
            else self.STREAM_ORDER_UPDATE
        )
        self.subscribe([stream], handler)

    def subscribe_position_updates(
        self,
        handler: Callable[[Dict[str, Any]], None],
    ) -> None:
        """
        Subscribe to position update stream.

        Args:
            handler: Callback function to handle position updates

        https://docs.backpack.exchange/#tag/Streams/Position-Update
        """
        self.subscribe([self.STREAM_POSITION_UPDATE], handler)

    def subscribe_rfq_updates(
        self,
        handler: Callable[[Dict[str, Any]], None],
    ) -> None:
        """
        Subscribe to RFQ (Request for Quote) update stream.

        Args:
            handler: Callback function to handle RFQ updates

        https://docs.backpack.exchange/#tag/Streams/RFQ-Update
        """
        self.subscribe([self.STREAM_RFQ_UPDATE], handler)

    def subscribe_all(
        self,
        handler: Callable[[Dict[str, Any]], None],
    ) -> None:
        """
        Subscribe to all private streams (order updates, position updates, RFQ updates).

        Args:
            handler: Callback function to handle all updates
        """
        streams = [
            self.STREAM_ORDER_UPDATE,
            self.STREAM_POSITION_UPDATE,
            self.STREAM_RFQ_UPDATE,
        ]
        self.subscribe(streams, handler)

    def listen(self, timeout: Optional[float] = None) -> None:
        """
        Start listening for messages and dispatch to registered handlers.
        This is a blocking call that runs until disconnect() is called.

        Args:
            timeout: Optional timeout for receiving messages (seconds).
                     If None, blocks indefinitely.
        """
        if not self.is_connected:
            self.connect()

        self._running = True

        # Start the async listener
        asyncio.run_coroutine_threadsafe(self._async_ws.start(), self._loop)

        try:
            while self._running:
                try:
                    stream, message = self._message_queue.get(timeout=timeout or 1.0)

                    # Dispatch to all matching handlers
                    self._dispatch_message(stream, message)

                except Empty:
                    if timeout is not None:
                        break
                    continue
        except KeyboardInterrupt:
            if self.debug:
                print("Interrupted by user")
        finally:
            self._running = False

    def receive(self, timeout: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """
        Receive a single message from the websocket.

        Args:
            timeout: Optional timeout in seconds

        Returns:
            The message dict or None if timeout
        """
        if not self.is_connected:
            self.connect()

        # Make sure async listener is running
        asyncio.run_coroutine_threadsafe(self._async_ws.start(), self._loop)

        try:
            stream, message = self._message_queue.get(timeout=timeout)
            self._dispatch_message(stream, message)
            return message
        except Empty:
            return None

    def _dispatch_message(self, stream: str, message: Dict[str, Any]) -> None:
        """Dispatch a message to registered handlers."""
        msg_stream = message.get("stream", stream)

        # Check exact match
        if msg_stream in self._handlers:
            for handler in self._handlers[msg_stream]:
                handler(message)

        # Check base stream match
        if "." in msg_stream:
            base_stream = msg_stream.rsplit(".", 1)[0]
            if base_stream in self._handlers and base_stream != msg_stream:
                for handler in self._handlers[base_stream]:
                    handler(message)

    def _run_event_loop(self) -> None:
        """Run the event loop in the background thread."""
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _get_secret_key_b64(self) -> str:
        """Get the base64 encoded secret key from the private key."""
        import base64
        private_bytes = self.private_key.private_bytes_raw()
        return base64.b64encode(private_bytes).decode()

    def __enter__(self) -> "PrivateWebsocket":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.disconnect()
