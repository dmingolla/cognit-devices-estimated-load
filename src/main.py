#!/usr/bin/env python3
"""Standalone daemon for updating estimated load for all devices."""

import asyncio
import sys
from estimated_load_daemon import daemon_loop
from cognit_logger import setup_logging
import cognit_conf as conf

def main():
    """Main entry point for the standalone daemon."""
    setup_logging(conf.LOG_LEVEL)
    
    print("Starting Cognit Devices Estimated Load Daemon...")
    print("Press Ctrl+C to stop")
    
    try:
        asyncio.run(daemon_loop())
    except KeyboardInterrupt:
        print("\nDaemon stopped by user")
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())

