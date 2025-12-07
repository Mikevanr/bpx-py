#!/usr/bin/env python3
"""
Backpack Exchange Points Farming Bot

Autonomous scalping bot that maximizes trading volume (points) by detecting
price wicks from Binance and executing maker-only trades on Backpack Exchange.

Setup:
    export BPX_PUBLIC_KEY="your_public_key"
    export BPX_SECRET_KEY="your_secret_key"
    python examples/points_farmer_example.py

Or run directly with keys:
    python examples/points_farmer_example.py --public-key KEY --secret-key KEY

Features:
    - Trades 6 perpetual futures: BTC, ETH, SOL (50x), ZEC, 2Z, MON (10x)
    - Detects 0.3% wicks from Binance real-time stream
    - Maker-only entries at best bid/ask
    - TP at +0.1%, SL at -0.2%
    - Auto-closes profitable positions after 30 seconds
    - Tracks volume and estimated points

Safety:
    - 3-second cooldown per symbol
    - Pauses symbol if cumulative loss exceeds $15
    - Cancels stale orders after 10 seconds
    - Uses only 30% of max leverage per symbol
"""

import asyncio
import argparse
import os
import sys

# Add parent directory to path for local development
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bpx.points_farmer import PointsFarmer


def main():
    parser = argparse.ArgumentParser(
        description="Backpack Exchange Points Farming Bot"
    )
    parser.add_argument(
        "--public-key",
        default=os.environ.get("BPX_PUBLIC_KEY"),
        help="API public key (or set BPX_PUBLIC_KEY env var)",
    )
    parser.add_argument(
        "--secret-key",
        default=os.environ.get("BPX_SECRET_KEY"),
        help="API secret key (or set BPX_SECRET_KEY env var)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    if not args.public_key or not args.secret_key:
        print("Error: API keys required")
        print()
        print("Set environment variables:")
        print("  export BPX_PUBLIC_KEY='your_public_key'")
        print("  export BPX_SECRET_KEY='your_secret_key'")
        print()
        print("Or use command line arguments:")
        print("  python points_farmer_example.py --public-key KEY --secret-key KEY")
        sys.exit(1)

    print("Starting Backpack Points Farmer...")
    print()

    bot = PointsFarmer(
        public_key=args.public_key,
        secret_key=args.secret_key,
        debug=args.debug,
    )

    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\nBot stopped by user")


if __name__ == "__main__":
    main()
