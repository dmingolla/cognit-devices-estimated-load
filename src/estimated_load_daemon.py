"""Background daemon for updating estimated load for all devices."""

import asyncio
import os
import yaml
import cognit_conf as conf
import db_manager
from system_metrics import calculate_estimated_load
from cognit_logger import setup_logging, get_logger

logger = get_logger(__name__)

def load_interval() -> int:
    """Load interval from config file with priority: user_config > DEFAULT > 30."""
    interval = None
    if os.path.exists(conf.PATH):
        try:
            with open(conf.PATH, 'r') as f:
                user_config = yaml.safe_load(f) or {}
                interval = user_config.get('estimated_load_update_interval_seconds')
        except Exception:
            pass
    
    if interval is None:
        interval = conf.DEFAULT.get('estimated_load_update_interval_seconds', 30)
    
    return interval

def update_all_devices_estimated_load() -> None:
    """Update estimated_load for all devices in database.
    
    Process:
    1. Cleanup old records
    2. Get all device_ids
    3. Calculate system-wide estimated_load
    4. Update each device individually (with error handling)
    """
    db = db_manager.DBManager()
    
    try:
        db.cleanup_old_records()
    except Exception as e:
        logger.warning(f"Failed to cleanup old records: {e}")
    
    device_ids = db.get_all_device_ids()
    
    if not device_ids:
        logger.debug("No devices in database, skipping estimated load update")
        return
    
    try:
        device_count = db.get_distinct_device_count()
        estimated_load = calculate_estimated_load(device_count)
        logger.info(f"Calculated estimated_load: {estimated_load:.2f} (device_count={device_count})")
    except Exception as e:
        logger.error(f"Failed to calculate estimated_load: {e}")
        return
    
    success_count = 0
    failure_count = 0
    
    for device_id in device_ids:
        try:
            db.update_estimated_load(device_id, estimated_load)
            success_count += 1
        except Exception as e:
            logger.warning(f"Failed to update estimated_load for device {device_id}: {e}")
            failure_count += 1
    
    logger.info(f"Updated estimated_load for {success_count} devices, {failure_count} failures")


async def daemon_loop() -> None:
    """Main daemon loop that runs periodically.

    It is called in the lifespan context manager of fastapi app in main.py
    
    Checks configuration each iteration to support dynamic interval changes.
    Handles errors gracefully without stopping the daemon.
    """
    logger.info("Estimated load daemon started")
    
    while True:
        try:
            interval = load_interval()
            logger.info(f"Daemon loop frequency: {interval} seconds")
            update_all_devices_estimated_load()
            await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"Error in daemon loop: {e}")
            await asyncio.sleep(30)

if __name__ == "__main__":
    # Setup logging when running standalone
    setup_logging("INFO")
    interval = load_interval()
    print("Starting estimated load daemon (press Ctrl+C to stop)...")
    print(f"Update interval: {interval} seconds")
    try:
        asyncio.run(daemon_loop())
    except KeyboardInterrupt:
        print("\nDaemon stopped by user")
