"""System-wide metrics collection for estimated load calculation."""

from typing import List, Dict, Any
import json
import math
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
from pyoneai.core import Entity, EntityType, EntityUID, MonitoringConfig
from pyoneai.core import Float, MetricAttributes, MetricType
from pyoneai.core.time import Period
import cognit_conf as conf
from cognit_logger import get_logger

logger = get_logger(__name__)


def get_oneflow_services() -> List[Dict[str, Any]]:
    """Get all OneFlow services via REST API."""
    try:
        oneflow_url = conf.ONE_XMLRPC.replace(':2633/RPC2', ':2474')
        response = requests.get(
            f"{oneflow_url}/service",
            auth=HTTPBasicAuth(conf.ONE_API_USER, conf.ONE_API_PASSWORD),
            timeout=10
        )
        response.raise_for_status()
        
        data = response.json()
        docs = data.get('DOCUMENT_POOL', {}).get('DOCUMENT', [])
        return docs if isinstance(docs, list) else [docs] if docs else []
        
    except Exception as e:
        logger.error(f"Error fetching OneFlow services: {e}")
        return []


def collect_system_metrics() -> List[Dict[str, Any]]:
    """Collect metrics for each OneFlow service with Frontend role.
    
    Returns:
        List of dicts: [{"service_id": int, "service_name": str, "queue_total": int, "sum_cpu_faas_role": float}]
        - queue_total: Latest sum across Frontend VMs (SDK aggregates at role level)
        - sum_cpu_faas_role: Latest average across FaaS VMs (SDK aggregates at role level)
    """
    all_services = get_oneflow_services()

    # Filter to only services with Frontend roles
    frontend_services = []
    for service in all_services:
        if has_frontend_role(service):
            frontend_services.append(service)

    service_metrics = []

    if not frontend_services:
        return service_metrics

    try:
        # Build service topology for monitoring
        services_data = []
        for service in frontend_services:
            service_id = service.get("ID")
            service_name = service.get("NAME", f"service_{service_id}")

            # Extract VM IDs for Frontend and FaaS roles
            frontend_vms = []
            faas_vms = []

            try:
                body = service.get("TEMPLATE", {}).get("BODY", {})
                if isinstance(body, str):
                    body = json.loads(body)
                roles = body.get("roles", [])

                for role in roles:
                    role_name = role.get("name")
                    nodes = role.get("nodes", [])

                    for node in nodes:
                        vm_id = node.get("deploy_id")
                        if vm_id:
                            vm_info = {"id": vm_id}
                            if role_name == "Frontend":
                                frontend_vms.append(vm_info)
                            elif role_name == "FaaS":
                                faas_vms.append(vm_info)
            except Exception as e:
                logger.warning(f"Warning: Could not extract VM info from service {service_id}: {e}")

            services_data.append({
                "service_id": service_id,
                "service_name": service_name,
                "frontend_vms": frontend_vms,
                "faas_vms": faas_vms,
            })

        # Build service topology and create monitoring config
        service_topology = build_service_topology(services_data)
        monitoring_config = create_service_monitoring_config(service_topology)

        # Collect metrics for each service using SDK service aggregation
        for service_data in services_data:
            service_id = service_data["service_id"]
            service_name = service_data["service_name"]

            # Get metrics using SDK service aggregation
            metrics = get_service_metrics(service_id, service_name, monitoring_config)

            service_metrics.append({
                "service_id": service_id,
                "service_name": service_name,
                "queue_total": metrics["queue_total"],
                "sum_cpu_faas_role": metrics["sum_cpu_faas_role"],
            })

    except Exception as e:
        logger.error(f"Error collecting system metrics: {e}")

    return service_metrics


def has_frontend_role(service: Dict) -> bool:
    """Check if a service has a Frontend role."""
    try:
        body = service.get("TEMPLATE", {}).get("BODY", {})
        if isinstance(body, str):
            body = json.loads(body)
        roles = body.get("roles", [])
        return any(role.get("name") == "Frontend" for role in roles)
    except Exception:
        return False


def extract_flavour_from_service_name(service_name: str) -> str:
    """Extract flavour from service name and normalize to lowercase.
    
    Extracts the flavour by taking the part after the last underscore,
    or the entire name if no underscore is found.
    
    Args:
        service_name: Service name (e.g., 'ICE_GlobalOptimizer')
    
    Returns:
        Lowercase flavour string (e.g., 'globaloptimizer')
    """
    # Split by underscore and take last part, or use entire name
    if '_' in service_name:
        flavour = service_name.split('_')[-1]
    else:
        flavour = service_name
    
    # Return lowercase for case-insensitive comparison
    return flavour.lower()


def calculate_estimated_load_for_service(service_cpu_percent: float, device_count: int) -> float:
    """Calculate estimated load for a single service.
    
    Args:
        service_cpu_percent: CPU usage percentage for the service (0-100)
        device_count: Number of devices with the service's flavour
    
    Returns:
        Estimated load in range [0.0, 1.0]
        - 1.0 if device_count == 0
        - (service_cpu_percent / 100.0) / device_count otherwise
        - Capped at 1.0 maximum
    """
    if device_count == 0:
        return 1.0
    
    if service_cpu_percent == 0:
        return 0.0
    
    # Normalize CPU from percentage [0-100] to [0-1] and divide by device count
    estimated_load = (service_cpu_percent / 100.0) / device_count
    
    return min(estimated_load, 1.0)


def calculate_estimated_load(device_count: int) -> float:
    """Calculate estimated load from system metrics and device count.
    
    Args:
        device_count: Number of distinct devices in system
    
    Returns:
        Estimated load in range [0.0, 1.0]
        - 1.0 if any backlog exists
        - (sum_cpu_faas_role of all services / 100) / device_count otherwise
        - Capped at 1.0 maximum
    """
    service_metrics = collect_system_metrics()
    
    if not service_metrics:
        return 0.0
    
    total_backlog = sum(service["queue_total"] for service in service_metrics)
    
    if total_backlog > 0:
        return 1.0
    
    total_cpu_percent = 0.0
    for service in service_metrics:
        if service["sum_cpu_faas_role"] is not None:
            total_cpu_percent += service["sum_cpu_faas_role"]
    
    if total_cpu_percent == 0:
        return 0.0
    
    if device_count == 0:
        return 1.0
    
    # Normalize CPU from percentage [0-100] to [0-1] and divide by device count
    estimated_load = (total_cpu_percent / 100.0) / device_count
    
    return min(estimated_load, 1.0)


def build_service_topology(services_data: list[dict]) -> dict:
    """Build service topology mapping for SDK role-level aggregation.
    
    Args:
        services_data: List of service dicts with frontend_vms and faas_vms
    
    Returns:
        Dict mapping service_id -> {role_name: [vm_ids]}
        Example: {70: {"Frontend": [810], "FaaS": [811, 812]}}
    """
    topology = {}

    for service_data in services_data:
        if not service_data:
            continue

        service_id = service_data.get("service_id")

        if not service_id:
            continue

        roles = {}

        if service_data["frontend_vms"]:
            frontend_ids = [int(vm["id"]) for vm in service_data["frontend_vms"]]
            roles["Frontend"] = frontend_ids

        if service_data["faas_vms"]:
            faas_ids = [int(vm["id"]) for vm in service_data["faas_vms"]]
            roles["FaaS"] = faas_ids

        if roles:
            topology[int(service_id)] = roles  # Ensure service_id is integer

    return topology


def create_service_monitoring_config(service_topology: dict) -> MonitoringConfig:
    """Create MonitoringConfig for SDK role-level aggregation.
    
    Args:
        service_topology: Dict mapping service_id -> {role_name: [vm_ids]}
    
    Returns:
        MonitoringConfig with service_aggregating backend.
        SDK uses service_topology to know which VMs belong to each role.
    """
    vm_monitoring = MonitoringConfig.opennebula_db_mysql(
        **conf.DB_CONFIG,
        metric_xpath_mapping={
            "queue_total": "QUEUE_TOTAL",
            "cpu": "CPU",
        }
    )

    return MonitoringConfig(
        backend="service_aggregating",
        connection={"vm_monitoring_config": vm_monitoring},
        schema={"service_topology": service_topology},
        behavior={"monitor_interval": 60}
    )


def get_service_metrics(
    service_id: int,
    service_name: str,
    monitoring_config: MonitoringConfig
) -> dict[str, Any]:
    """Fetch latest metrics for a service using SDK role-level aggregation.
    
    SDK aggregates metrics automatically:
    - Frontend role: EntityUID(id="{service_id}_Frontend") → SDK finds VMs in topology, sums queue_total
    - FaaS role: EntityUID(id="{service_id}_FaaS") → SDK finds VMs in topology, sums CPU (total capacity used)
    
    Args:
        service_id: OneFlow service ID
        service_name: Service name for logging
        monitoring_config: Config with service_topology schema
    
    Returns:
        Dict with {"queue_total": int, "sum_cpu_faas_role": float} (latest values, already aggregated)
    """
    # Get only the latest monitoring point (last 2 minutes to ensure we get at least one sample)
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=2)
    period = Period(slice(start_time, end_time, timedelta(minutes=1)))

    results = {"queue_total": 0, "sum_cpu_faas_role": 0}

    try:
        # Get Frontend role metrics
        frontend_role = Entity(
            uid=EntityUID(type=EntityType.SERVICE_ROLE, id=f"{service_id}_Frontend"),
            metrics={
                "queue_total": MetricAttributes(
                    name="queue_total",
                    type=MetricType.GAUGE,
                    dtype=Float(),
                    aggregation_fn="sum"
                )
            },
            monitoring=monitoring_config
        )

        queue_data = frontend_role["queue_total"][period]
        if queue_data is not None and queue_data.values.size > 0:
            # Get the LATEST (last) value from the time series
            latest_queue = queue_data.values.flatten()[-1]
            if not math.isnan(latest_queue):
                results["queue_total"] = int(latest_queue)
                logger.info(f"Service {service_id} ({service_name}): queue_total={results['queue_total']} (latest)")
            else:
                logger.info(f"Service {service_id} ({service_name}): queue_total=NaN (no data)")

    except Exception as e:
        logger.warning(f"Warning: Could not fetch queue_total for service {service_id}: {e}")

    try:
        # Get FaaS role metrics (CPU is summed across all FaaS VMs = total capacity used)
        faas_role = Entity(
            uid=EntityUID(type=EntityType.SERVICE_ROLE, id=f"{service_id}_FaaS"),
            metrics={
                "cpu": MetricAttributes(
                    name="cpu",
                    type=MetricType.GAUGE,
                    dtype=Float(),
                    aggregation_fn="sum"
                )
            },
            monitoring=monitoring_config
        )

        cpu_data = faas_role["cpu"][period]
        if cpu_data is not None and cpu_data.values.size > 0:
            # Get the LATEST (last) sum of CPU across all FaaS VMs
            latest_cpu = cpu_data.values.flatten()[-1]
            if not math.isnan(latest_cpu):
                results["sum_cpu_faas_role"] = float(latest_cpu)
                logger.info(f"Service {service_id} ({service_name}): sum_cpu_faas_role={results['sum_cpu_faas_role']:.2f}% (latest)")
            else:
                logger.info(f"Service {service_id} ({service_name}): sum_cpu_faas_role=NaN (no data)")

    except Exception as e:
        logger.warning(f"Warning: Could not fetch sum_cpu_faas_role for service {service_id}: {e}")

    return results
