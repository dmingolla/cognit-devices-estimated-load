#!/usr/bin/env python3
"""Test forecast path with non-zero CPU sum history and aligned timestamps.

The SDK predictor (FourierPredictionModel) requests history as:
  hist_period = (future_time - (1 + sequence_length) * resolution, future_time - resolution)
with resolution = forecast_horizon (60s) and sequence_length = 2.
So it loads 2 points at (now - 120s, now - 60s) with 60s resolution.

If stored data is not on that grid or has gaps, the accessor fills with NaN and
Mann-Kendall fails. This script injects data exactly on that grid (no nulls)
to verify the SDK forecast path, then we can debug real-data alignment separately.

Run from repo root with venv active: python3 scripts/test_forecast_with_cpu.py [service_id]
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Repo root = parent of scripts/
_repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo_root / "src"))
import cognit_conf as conf

# Match SDK: horizon = forecast_horizon_seconds, sequence_length = 2
HORIZON_SECONDS = getattr(conf, "FORECAST_HORIZON_SECONDS", 60)
HIST_RESOLUTION = timedelta(seconds=HORIZON_SECONDS)
HIST_STEPS = 2  # FourierPredictionModel default sequence_length


def main():
    service_id = int(sys.argv[1]) if len(sys.argv) > 1 else 999001
    from system_metrics import (
        store_cpu_sum_for_service,
        get_cpu_forecast_for_service,
        store_cpu_forecast_for_service,
    )
    from cognit_logger import setup_logging, get_logger
    setup_logging(conf.LOG_LEVEL)
    logger = get_logger(__name__)

    now = datetime.now(timezone.utc).replace(microsecond=0)
    # History window the SDK will request: (now - (1+2)*60s, now - 60s) = (now-180s, now-60s)
    # with resolution 60s → points at now-120s and now-60s
    hist_start = now - (1 + HIST_STEPS) * HIST_RESOLUTION
    hist_end = now - HIST_RESOLUTION
    # Inject exactly on that grid (no nulls)
    timestamps = []
    t = hist_start
    while t <= hist_end:
        timestamps.append(t)
        t += HIST_RESOLUTION
    values = [10.0 + 5.0 * i for i in range(len(timestamps))]  # 10, 15 or 10, 15, 20

    for ts, cpu in zip(timestamps, values):
        store_cpu_sum_for_service(service_id, cpu, ts)
        logger.info(f"Injected cpu_sum={cpu} at {ts.isoformat()}")

    forecast = get_cpu_forecast_for_service(service_id)
    if forecast is not None:
        logger.info(f"Service {service_id}: forecast={forecast:.2f}%")
        store_cpu_forecast_for_service(service_id, forecast, now)
        print(f"OK: forecast={forecast:.2f}%")
    else:
        logger.warning("Forecast returned None")
        print("FAIL: forecast=None")
    return 0 if forecast is not None else 1


if __name__ == "__main__":
    sys.exit(main())
