from cryptography.hazmat.primitives.asymmetric import ed25519
import base64
from typing import List
from time import time


class BasePrivateWebsocket:
    """
    Base class for private websocket authentication and subscription management.
    Provides ED25519 signature generation for authenticated websocket streams.
    """

    BPX_WS_URL = "wss://ws.backpack.exchange/"

    def __init__(self, public_key: str, secret_key: str, window: int, debug: bool):
        self.private_key = ed25519.Ed25519PrivateKey.from_private_bytes(
            base64.b64decode(secret_key)
        )
        self.public_key = public_key
        self.window = window
        self.debug = debug

    def _create_subscription_payload(
        self, streams: List[str], window: int = None
    ) -> dict:
        """
        Creates the subscription payload with authentication headers for private streams.

        https://docs.backpack.exchange/#tag/Streams
        """
        window = self.window if window is None else window
        timestamp = int(time() * 1e3)
        signature = self._sign_subscription(streams, timestamp, window)

        payload = {
            "method": "SUBSCRIBE",
            "params": streams,
            "signature": [
                self.public_key,
                signature,
                str(timestamp),
                str(window),
            ],
        }

        if self.debug:
            print(f"Subscription payload: {payload}")

        return payload

    def _create_unsubscription_payload(self, streams: List[str]) -> dict:
        """
        Creates the unsubscription payload for private streams.
        """
        return {
            "method": "UNSUBSCRIBE",
            "params": streams,
        }

    def _sign_subscription(
        self, streams: List[str], timestamp: int, window: int
    ) -> str:
        """
        Creates an ED25519 signature for websocket subscription authentication.
        The instruction for websocket subscriptions is 'subscribe'.
        """
        sign_str = "instruction=subscribe"

        # Add streams as comma-separated list
        if streams:
            streams_str = ",".join(sorted(streams))
            sign_str += f"&params={streams_str}"

        sign_str += f"&timestamp={timestamp}&window={window}"

        if self.debug:
            print(f"Sign string: {sign_str}")

        signature_bytes = self.private_key.sign(sign_str.encode())
        encoded_signature = base64.b64encode(signature_bytes).decode()

        return encoded_signature
