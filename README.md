# Cognit Devices Estimated Load

A standalone daemon that periodically calculates and updates the estimated load for all registered devices based on system-wide metrics from OneFlow services.

## Overview

The daemon periodically:
1. Fetches all OneFlow services from OpenNebula frontend (via REST API)
2. Collects CPU and queue metrics from Frontend and FaaS role VMs
3. Calculates system-wide estimated load based on total CPU usage and backlog (number of messages in READY state in the RabbitMQ on the edge-cluster frontends)
4. Updates the `estimated_load` field in the database for each device

## Database Integration

**Local Database (`device_cluster_assignment.db`):**
- Stores device-to-cluster assignments with estimated load
- Shared between `cognit-frontend`, `cognit-optimizer`, and this daemon
- Schema: `device_id`, `cluster_id`, `flavour`, `app_req_id`, `app_req_json`, `estimated_load`, `last_seen`

**cognit-frontend interaction:**
- Stores initial device assignments when devices request clusters

**cognit-devices-estimated-load interaction:**
- Reads all device IDs from the database
- Calculates estimated load from OneFlow metrics
- Updates `estimated_load` for each device

## Dependencies

**Required:**
- OpenNebula frontend accessible at configured endpoint (default: `http://10.10.10.2:2633/RPC2`)
- OneFlow REST API accessible (default: `http://10.10.10.2:2474`)
- OpenNebula MySQL database accessible for VM metrics (default: `10.10.10.2:3306`)

## Setup

1. **Copy example configuration:**
   ```bash
   cp config.yaml.example config.yaml
   ```

2. **Edit configuration with your credentials:**
   ```bash
   nano config.yaml
   # Update: one_xmlrpc, one_db_password, one_api_password
   ```

3. **Create virtual environment:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

4. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

**Run single update cycle:**
```bash
python3 src/main.py
```

**Run as daemon (continuous mode):**
```bash
# Use default interval (30 seconds)
python3 src/main.py --daemon

# Use custom interval
python3 src/main.py --daemon --interval 60
```

## Configuration

Edit `config.yaml` to configure:
- Database path
- OpenNebula endpoints and credentials
- MySQL database credentials
- Update interval

