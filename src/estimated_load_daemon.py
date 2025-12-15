"""Background daemon for updating estimated load for all devices."""

import db_manager
from system_metrics import (
    collect_system_metrics,
    extract_flavour_from_service_name,
    calculate_estimated_load_for_service
)
from cognit_logger import get_logger

logger = get_logger(__name__)

def update_all_devices_estimated_load() -> None:
    """Update estimated_load for all devices in database per service.
    
    Process:
    1. Cleanup old records
    2. Collect metrics per service
    3. For each service:
       a. Extract flavour from service name
       b. Get devices with that flavour
       c. Calculate estimated_load = service_cpu / device_count
       d. Update devices with that flavour
    """
    db = db_manager.DBManager()
    
    try:
        db.cleanup_old_records()
    except Exception as e:
        logger.warning(f"Failed to cleanup old records: {e}")
    
    try:
        service_metrics = collect_system_metrics()
    except Exception as e:
        logger.error(f"Failed to collect system metrics: {e}")
        return
    
    if not service_metrics:
        logger.debug("No services found, skipping estimated load update")
        return
    
    total_success_count = 0
    total_failure_count = 0
    
    for service in service_metrics:
        service_id = service["service_id"]
        service_name = service["service_name"]
        service_cpu = service["sum_cpu_faas_role"] or 0.0
        queue_total = service["queue_total"]
        
        # Extract flavour from service name (lowercase for case-insensitive matching)
        flavour = extract_flavour_from_service_name(service_name)
        
        # Get devices with this flavour
        device_ids = db.get_device_ids_by_flavour(flavour)
        device_count = len(device_ids)
        
        if not device_ids:
            logger.debug(f"Service {service_id} ({service_name}): No devices found with flavour '{flavour}'")
            continue
        
        # Calculate estimated load for this service/flavour
        if queue_total > 0:
            estimated_load = 1.0
            logger.info(
                f"Service {service_id} ({service_name}): "
                f"queue_total={queue_total} > 0, setting estimated_load=1.0"
            )
        else:
            estimated_load = calculate_estimated_load_for_service(service_cpu, device_count)
            logger.info(
                f"Service {service_id} ({service_name}): "
                f"device_count={device_count}, service_cpu={service_cpu:.2f}%, "
                f"estimated_load={estimated_load:.4f} per device"
            )
        
        # Update devices with this flavour
        service_success_count = 0
        service_failure_count = 0
        
        for device_id in device_ids:
            try:
                db.update_estimated_load(device_id, estimated_load)
                service_success_count += 1
            except Exception as e:
                logger.warning(f"Failed to update estimated_load for device {device_id}: {e}")
                service_failure_count += 1
        
        total_success_count += service_success_count
        total_failure_count += service_failure_count
        
        logger.info(
            f"Service {service_id} ({service_name}): "
            f"Updated {service_success_count} devices, {service_failure_count} failures"
        )
    
    logger.info(
        f"Total: Updated estimated_load for {total_success_count} devices, "
        f"{total_failure_count} failures across {len(service_metrics)} services"
    )