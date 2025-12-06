"""
Private Websocket Examples for Backpack Exchange

This file demonstrates how to use the private websocket client
to receive real-time account updates.

Available streams:
- account.orderUpdate: Order status changes and fills
- account.orderUpdate.<symbol>: Symbol-specific order updates (e.g., account.orderUpdate.SOL_USDC)
- account.positionUpdate: Position changes (futures)
- account.rfqUpdate: Request for Quote updates

https://docs.backpack.exchange/#tag/Streams
"""

import asyncio
from bpx.private_websocket import PrivateWebsocket
from bpx.async_.private_websocket import PrivateWebsocket as AsyncPrivateWebsocket


PUBLIC_KEY = "<PUBLIC_KEY>"
SECRET_KEY = "<SECRET_KEY>"


# ============================================
# Synchronous Examples
# ============================================


def sync_order_updates_example():
    """
    Subscribe to order updates using the synchronous client.
    This blocks the current thread while listening for messages.
    """

    def on_order_update(message):
        print(f"Order Update: {message}")

    ws = PrivateWebsocket(public_key=PUBLIC_KEY, secret_key=SECRET_KEY, debug=True)

    try:
        ws.connect()
        ws.subscribe_order_updates(on_order_update)
        print("Listening for order updates... Press Ctrl+C to stop")
        ws.listen()
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        ws.disconnect()


def sync_all_streams_example():
    """
    Subscribe to all private streams using the synchronous client.
    """

    def on_message(message):
        stream = message.get("stream", "unknown")
        print(f"[{stream}] {message}")

    # Using context manager
    with PrivateWebsocket(
        public_key=PUBLIC_KEY, secret_key=SECRET_KEY, debug=True
    ) as ws:
        ws.subscribe_all(on_message)
        print("Listening for all account updates... Press Ctrl+C to stop")
        ws.listen()


def sync_symbol_specific_example():
    """
    Subscribe to order updates for a specific symbol.
    """

    def on_sol_order_update(message):
        print(f"SOL/USDC Order Update: {message}")

    ws = PrivateWebsocket(public_key=PUBLIC_KEY, secret_key=SECRET_KEY)

    try:
        ws.connect()
        # Subscribe only to SOL_USDC order updates
        ws.subscribe_order_updates(on_sol_order_update, symbol="SOL_USDC")
        ws.listen()
    finally:
        ws.disconnect()


def sync_receive_single_message_example():
    """
    Receive messages one at a time with timeout.
    """
    ws = PrivateWebsocket(public_key=PUBLIC_KEY, secret_key=SECRET_KEY)

    try:
        ws.connect()
        ws.subscribe_order_updates(lambda msg: None)  # Dummy handler for subscription

        print("Waiting for messages (5 second timeout)...")
        for _ in range(10):  # Try to receive 10 messages
            message = ws.receive(timeout=5.0)
            if message:
                print(f"Received: {message}")
            else:
                print("Timeout waiting for message")
    finally:
        ws.disconnect()


# ============================================
# Asynchronous Examples
# ============================================


async def async_order_updates_example():
    """
    Subscribe to order updates using the async client.
    """

    async def on_order_update(message):
        print(f"Order Update: {message}")

    ws = AsyncPrivateWebsocket(public_key=PUBLIC_KEY, secret_key=SECRET_KEY, debug=True)

    try:
        await ws.connect()
        await ws.subscribe_order_updates(on_order_update)
        print("Listening for order updates... Press Ctrl+C to stop")
        await ws.listen()
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        await ws.disconnect()


async def async_all_streams_example():
    """
    Subscribe to all private streams using the async client.
    """

    async def on_message(message):
        stream = message.get("stream", "unknown")
        print(f"[{stream}] {message}")

    # Using async context manager
    async with AsyncPrivateWebsocket(
        public_key=PUBLIC_KEY, secret_key=SECRET_KEY, debug=True
    ) as ws:
        await ws.subscribe_all(on_message)
        print("Listening for all account updates... Press Ctrl+C to stop")
        await ws.listen()


async def async_multiple_handlers_example():
    """
    Subscribe to different streams with different handlers.
    """

    async def on_order_update(message):
        print(f"ORDER: {message}")

    async def on_position_update(message):
        print(f"POSITION: {message}")

    async def on_rfq_update(message):
        print(f"RFQ: {message}")

    async with AsyncPrivateWebsocket(
        public_key=PUBLIC_KEY, secret_key=SECRET_KEY
    ) as ws:
        await ws.subscribe_order_updates(on_order_update)
        await ws.subscribe_position_updates(on_position_update)
        await ws.subscribe_rfq_updates(on_rfq_update)

        print("Listening with multiple handlers...")
        await ws.listen()


async def async_background_listener_example():
    """
    Start the websocket listener in the background while doing other work.
    """

    async def on_order_update(message):
        print(f"Background received: {message}")

    ws = AsyncPrivateWebsocket(public_key=PUBLIC_KEY, secret_key=SECRET_KEY)

    try:
        await ws.connect()
        await ws.subscribe_order_updates(on_order_update)

        # Start listener in background
        await ws.start()

        # Do other work while websocket listens in background
        print("Websocket listening in background...")
        for i in range(60):
            print(f"Doing other work... ({i + 1}/60)")
            await asyncio.sleep(1)

    finally:
        await ws.disconnect()


# ============================================
# Run Examples
# ============================================


def run_sync_example():
    """Run one of the synchronous examples."""
    # Uncomment the example you want to run:
    sync_order_updates_example()
    # sync_all_streams_example()
    # sync_symbol_specific_example()
    # sync_receive_single_message_example()


def run_async_example():
    """Run one of the asynchronous examples."""
    # Uncomment the example you want to run:
    asyncio.run(async_order_updates_example())
    # asyncio.run(async_all_streams_example())
    # asyncio.run(async_multiple_handlers_example())
    # asyncio.run(async_background_listener_example())


if __name__ == "__main__":
    # Choose which type of example to run:
    run_sync_example()
    # run_async_example()
