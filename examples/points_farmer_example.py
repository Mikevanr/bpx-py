#!/usr/bin/env python3
"""
Backpack Exchange Momentum Scalper

High-frequency momentum scalping bot for BTC, ETH, SOL perpetuals.
Follows short-term momentum with tight risk management.

Setup:
    export BPX_PUBLIC_KEY="your_public_key"
    export BPX_SECRET_KEY="your_secret_key"
    python examples/points_farmer_example.py

Or run directly with keys:
    python examples/points_farmer_example.py --public-key KEY --secret-key KEY

Strategy:
    - Trades BTC, ETH, SOL with 50x leverage
    - Detects 0.08% momentum in 3 seconds from Binance
    - Market orders for immediate fills
    - TP: 0.06% | SL: 0.10%
    - Trailing stop: activates at 0.03%, trails 0.04%

Risk Management:
    - Max 3 concurrent positions
    - Max $5 loss per position
    - Max $50 daily loss limit
    - 60 second max hold time
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

    print("Starting Momentum Scalper...")
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
