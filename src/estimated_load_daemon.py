"""Background daemon for updating estimated load for all devices."""

import db_manager
from system_metrics import calculate_estimated_load
from cognit_logger import get_logger

logger = get_logger(__name__)

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