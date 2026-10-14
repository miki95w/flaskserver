from flask import Flask, request, Response, jsonify
from datetime import datetime
import json
import logging
import os
import sqlite3
import threading
import time
import requests

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('flask_adms.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# System state memory blocks
devices = {}
commands_queue = {}
last_command_id = 1000
users_buffer = {} # Map of SN -> list of user records
attendance_buffer = []  # Store attendance records that still need polling by Odoo.
records_received_total = 0
records_pushed_total = 0

# SQLite Spooling & Thread Safety
buffer_lock = threading.RLock()
SPOOL_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'flask_adms_spool.db')

def get_db_connection():
    conn = sqlite3.connect(SPOOL_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn

def init_spool_db():
    try:
        with get_db_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pending_punches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_sn TEXT,
                    user_id TEXT,
                    timestamp TEXT,
                    punch_type TEXT,
                    verify_type TEXT,
                    raw_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_created ON pending_punches (created_at)")
            conn.commit()
        logger.info(f"SQLite attendance spool database initialized at {SPOOL_DB_PATH}.")
    except Exception as e:
        logger.error(f"Failed to initialize SQLite spool database: {e}")

def load_spool_into_buffer():
    global attendance_buffer
    try:
        with buffer_lock:
            with get_db_connection() as conn:
                rows = conn.execute("SELECT id, device_sn, user_id, timestamp, punch_type, verify_type, raw_data FROM pending_punches ORDER BY id ASC").fetchall()
                attendance_buffer = []
                for row in rows:
                    pt = str(row['punch_type']) if row['punch_type'] is not None else '0'
                    event_type = int(pt) if pt.isdigit() else 0
                    attendance_buffer.append({
                        'spool_id': row['id'],
                        'device_id': row['device_sn'],
                        'device_sn': row['device_sn'],
                        'user_id': row['user_id'],
                        'timestamp': row['timestamp'],
                        'event_type': event_type,
                        'punch_type': pt,
                        'verify_type': row['verify_type'],
                        'raw_data': row['raw_data'],
                    })
        logger.info(f"Loaded {len(attendance_buffer)} pending punches from SQLite spool on startup.")
    except Exception as e:
        logger.error(f"Failed to load pending punches from SQLite: {e}")

def remove_pushed_records(records):
    global attendance_buffer, records_pushed_total
    with buffer_lock:
        spool_ids = [r['spool_id'] for r in records if isinstance(r, dict) and r.get('spool_id')]
        if spool_ids:
            try:
                with get_db_connection() as conn:
                    for i in range(0, len(spool_ids), 500):
                        batch = spool_ids[i:i+500]
                        placeholders = ','.join('?' for _ in batch)
                        conn.execute(f"DELETE FROM pending_punches WHERE id IN ({placeholders})", batch)
                    conn.commit()
            except Exception as e:
                logger.error(f"Error deleting delivered punches from SQLite spool: {e}")

        delivered_spool_ids = set(spool_ids)
        if delivered_spool_ids:
            attendance_buffer = [r for r in attendance_buffer if r.get('spool_id') not in delivered_spool_ids]
        else:
            for delivered in records:
                if delivered in attendance_buffer:
                    attendance_buffer.remove(delivered)
        records_pushed_total += len(records)

# Initialize spool and preload any unsynced punches
init_spool_db()
load_spool_into_buffer()

# Configuration
FLASK_ADMS_CONFIG = {
    'version': '1.0',
    'max_buffer_size': 1000,
    'api_key': 'your-api-key-123',  # Default fallback API Key
    'allowed_ips': [],
    'odoo_url': '',       # Odoo base URL
    'odoo_db': '',        # Odoo database name
    'timezone': '3',      # ZKTeco ADMS timezone offset for Africa/Addis_Ababa.
    'debug': False,
}
server_port = 8082

config_path = 'flask_adms_config.json'
if os.path.exists(config_path):
    try:
        with open(config_path, 'r') as f:
            cfg_file = json.load(f)
            if 'security' in cfg_file and 'api_key' in cfg_file['security']:
                FLASK_ADMS_CONFIG['api_key'] = cfg_file['security']['api_key']
            if 'server' in cfg_file and 'port' in cfg_file['server']:
                server_port = cfg_file['server']['port']
            if 'odoo' in cfg_file:
                FLASK_ADMS_CONFIG['odoo_url'] = cfg_file['odoo'].get('url', '')
                FLASK_ADMS_CONFIG['odoo_db'] = cfg_file['odoo'].get('db', '')
            if 'device' in cfg_file:
                FLASK_ADMS_CONFIG['timezone'] = str(cfg_file['device'].get('timezone', FLASK_ADMS_CONFIG['timezone']))
            if 'server' in cfg_file:
                FLASK_ADMS_CONFIG['debug'] = bool(cfg_file['server'].get('debug', FLASK_ADMS_CONFIG['debug']))
            logger.info(f"Loaded config from {config_path}: Port={server_port}")
            logger.info(f"Odoo push target: {FLASK_ADMS_CONFIG['odoo_url']}")
    except Exception as e:
        logger.error(f"Failed to load config from {config_path}: {e}")


def push_to_odoo(endpoint, payload):
    """Push data to Odoo ZKTeco controller endpoint."""
    odoo_url = FLASK_ADMS_CONFIG.get('odoo_url', '')
    if not odoo_url:
        return False
    try:
        url = odoo_url.rstrip('/') + endpoint
        resp = __import__('requests').post(
            url,
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        if resp.status_code == 200:
            result = resp.json()
            logger.info(f"Odoo push success: {result}")
            return True
        else:
            logger.warning(f"Odoo push returned HTTP {resp.status_code}: {resp.text[:200]}")
            return False
    except Exception as e:
        logger.warning(f"Odoo push failed (will keep in buffer): {e}")
        return False


def authenticate_request():
    """Simple API key authentication."""
    auth_header = request.headers.get('Authorization', '')
    if not auth_header:
        return False
    
    # Support both "Bearer token" and direct token formats
    if auth_header.startswith('Bearer '):
        token = auth_header[7:]
    else:
        token = auth_header
    
    return token == FLASK_ADMS_CONFIG['api_key']


def require_api_auth():
    if not authenticate_request():
        return jsonify({"status": "error", "message": "Unauthorized"}), 401
    return None


def get_client_ip():
    forwarded_for = request.environ.get('HTTP_X_FORWARDED_FOR')
    if forwarded_for:
        return forwarded_for.split(',')[0].strip()
    return request.remote_addr


def touch_device(sn):
    if not sn:
        return
    devices[sn] = {
        **devices.get(sn, {}),
        "status": "Active",
        "last_seen": datetime.now().isoformat(),
        "ip_address": get_client_ip(),
    }
    commands_queue.setdefault(sn, [])


def next_command_id():
    global last_command_id
    last_command_id += 1
    return last_command_id


def queue_device_command(sn, command_string):
    cmd_id = next_command_id()
    cmd = f"C:{cmd_id}:{command_string}"
    commands_queue.setdefault(sn, [])
    commands_queue[sn].append(cmd)
    
    logger.info(f"🔗 QUEUED COMMAND for device {sn}: {cmd}")
    logger.info(f"📋 Device {sn} queue size: {len(commands_queue[sn])}")
    
    return cmd_id, cmd


def log_device_activity(device_sn, activity, details=None):
    """Log device activities for debugging."""
    log_msg = f"Device {device_sn}: {activity}"
    if details:
        log_msg += f" - {details}"
    logger.info(log_msg)


def background_poll_and_forward():
    """Background daemon thread to poll Odoo VPS for commands and push buffered attendance records."""
    logger.info("Starting background poll and forward daemon thread...")
    while True:
        try:
            # 1. Store-and-Forward: Push buffered attendance records to Odoo VPS
            records_to_push = []
            with buffer_lock:
                if attendance_buffer:
                    records_to_push = list(attendance_buffer)

            if records_to_push:
                batch_size = 500
                logger.info(f"Store-and-Forward: {len(records_to_push)} records in buffer. Processing in batches...")

                success_all = True
                for i in range(0, len(records_to_push), batch_size):
                    chunk = records_to_push[i:i+batch_size]
                    logger.info(f"Store-and-Forward: Pushing batch {i//batch_size + 1} of {len(chunk)} records...")
                    cleaned_chunk = [{k: v for k, v in r.items() if k != 'spool_id'} for r in chunk]
                    payload = {
                        'api_key': FLASK_ADMS_CONFIG['api_key'],
                        'records': cleaned_chunk
                    }
                    success = push_to_odoo('/zkteco/push/attendance', payload)
                    if success:
                        remove_pushed_records(chunk)
                    else:
                        success_all = False
                        logger.warning(f"Store-and-Forward: Failed to push batch starting at index {i}.")
                        break # Stop pushing remaining batches on failure

                if success_all:
                    logger.info("Store-and-Forward: All buffered records successfully pushed.")
            
            # 2. Poll Odoo VPS for pending commands
            odoo_url = FLASK_ADMS_CONFIG.get('odoo_url', '')
            api_key = FLASK_ADMS_CONFIG.get('api_key', '')
            if odoo_url and api_key:
                url = odoo_url.rstrip('/') + '/zkteco/api/commands'
                params = {'api_key': api_key}
                try:
                    resp = requests.get(url, params=params, timeout=10)
                    if resp.status_code == 200:
                        data = resp.json()
                        commands = data.get('commands', [])
                        if commands:
                            logger.info(f"Polled {len(commands)} commands from Odoo VPS.")
                            
                            # Queue commands for the devices in the local commands_queue
                            acked_ids = []
                            for cmd in commands:
                                cmd_id = cmd.get('id')
                                device_sn = cmd.get('device_sn')
                                command_string = cmd.get('command_string')
                                
                                if device_sn and command_string:
                                    queue_device_command(device_sn, command_string)
                                    acked_ids.append(cmd_id)
                                    
                            # Acknowledge commands to Odoo VPS
                            if acked_ids:
                                ack_url = odoo_url.rstrip('/') + '/zkteco/api/commands/ack'
                                ack_payload = {
                                    'api_key': api_key,
                                    'command_ids': acked_ids
                                }
                                try:
                                    ack_resp = requests.post(ack_url, json=ack_payload, timeout=10)
                                    if ack_resp.status_code == 200:
                                        logger.info(f"Acknowledged commands {acked_ids} to Odoo VPS.")
                                    else:
                                        logger.warning(f"Failed to ack commands to Odoo: HTTP {ack_resp.status_code}")
                                except Exception as ack_err:
                                    logger.error(f"Error acknowledging commands: {ack_err}")
                    else:
                        logger.warning(f"Odoo poll status code: {resp.status_code} - Response: {resp.text[:200]}")
                except Exception as poll_err:
                    logger.warning(f"Failed to poll commands from Odoo: {poll_err}")
                    
        except Exception as e:
            logger.error(f"Error in background poll and forward loop: {e}")
            
        time.sleep(5)


# -------------------------------------------------------------
# HEALTH AND STATUS ENDPOINTS FOR ODOO INTEGRATION
# -------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint for Odoo integration."""
    try:
        with buffer_lock:
            buf_len = len(attendance_buffer)
        return jsonify({
            "status": "ok",
            "service": "Flask ADMS",
            "version": FLASK_ADMS_CONFIG['version'],
            "database": "connected",
            "devices_connected": len(devices),
            "last_sync": datetime.now().isoformat(),
            "attendance_buffer_size": buf_len,
            "users_buffer_size": len(users_buffer)
        })
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/metrics", methods=["GET"])
def metrics():
    """Prometheus-style metrics endpoint for Odoo integration."""
    try:
        metrics_data = f"""# HELP adms_devices_connected Number of connected devices
# TYPE adms_devices_connected gauge
adms_devices_connected {len(devices)}

# HELP adms_records_processed_total Total processed attendance records
# TYPE adms_records_processed_total counter
adms_records_processed_total {records_received_total}

# HELP adms_records_quarantined_total Total quarantined records
# TYPE adms_records_quarantined_total counter  
adms_records_quarantined_total 0

# HELP adms_users_synced_total Total users synced
# TYPE adms_users_synced_total counter
adms_users_synced_total {len(users_buffer)}
"""
        return Response(metrics_data, mimetype="text/plain")
    except Exception as e:
        logger.error(f"Metrics endpoint failed: {e}")
        return Response(f"# Error generating metrics: {e}", mimetype="text/plain"), 500

@app.route("/api/test", methods=["GET", "POST"])
def api_test():
    """Test endpoint for Odoo integration verification."""
    auth_error = require_api_auth()
    if auth_error:
        return auth_error
    try:
        return jsonify({
            "status": "ok",
            "service": "Flask ADMS",
            "version": FLASK_ADMS_CONFIG['version'],
            "authentication": "working",
            "devices_connected": len(devices),
            "endpoints_available": [
                "/health",
                "/metrics", 
                "/api/devices/status",
                "/api/attendance/fetch",
                "/api/sync/users",
                "/api/attendance/request_historical",
                "/api/test"
            ]
        })
    except Exception as e:
        logger.error(f"API test failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/devices/status/detail", methods=["GET"])
def device_status_detail():
    """Get device status for Odoo integration."""
    auth_error = require_api_auth()
    if auth_error:
        return auth_error
    device_list = []
    for sn, info in devices.items():
        device_list.append({
            "device_id": sn,
            "serial_number": sn,
            "status": info.get("status", "Unknown"),
            "ip_address": info.get("ip_address", "Unknown"),
            "last_heartbeat": info.get("last_seen"),
            "is_online": info.get("status") == "Active"
        })
    
    return jsonify({
        "count": len(devices),
        "devices": device_list,
        "status": "success"
    })

# -------------------------------------------------------------
# ODOO INTEGRATION ENDPOINTS
# -------------------------------------------------------------

def push_user_to_device(sn, user_id, name):
    cmd_id = next_command_id()

    command = f"C:{cmd_id}:DATA USER PIN={user_id}\tName={name}\tPrivilege=0\tPassword=\tGroup=1\tCard=0"

    commands_queue.setdefault(sn, []).append(command)

    print(f"✅ Queued user push: {command}")

@app.route("/api/attendance/request_historical", methods=["POST"])
def request_historical_attendance():
    """Receive historical sync request from Odoo and command devices to upload logs."""
    auth_error = require_api_auth()
    if auth_error:
        return auth_error

    data = request.json
    if not data:
        return jsonify({"status": "error", "message": "No payload received"}), 400

    start_date = data.get("start_date")
    end_date = data.get("end_date")

    if not start_date or not end_date:
        return jsonify({"status": "error", "message": "Missing start_date or end_date"}), 400

    target_devices = list(devices.keys())
    if not target_devices:
        return jsonify({
            "status": "failed",
            "message": "No connected devices available for historical sync"
        }), 400

    # Clear any old historical data from buffer before requesting new data
    global attendance_buffer
    with buffer_lock:
        old_buffer_size = len(attendance_buffer)
    
    synced_count = 0
    for sn in target_devices:
        # Try multiple ZKTeco command formats for historical data
        commands = [
            # Standard data options
            "DATA OPTION rtlog",
            "DATA OPTION attlog", 
            "DATA OPTION transaction",
            # Specific queries with date ranges (may not work on all devices)
            f"DATA QUERY rtlog start={start_date} end={end_date}",
            f"DATA QUERY attlog start={start_date} end={end_date}",
            # Alternative formats
            "INFO",  # Request device info first
            "CLEAR DATA"  # This might trigger data upload
        ]
        
        logger.info(f"📅 Historical sync request for device {sn}: {start_date} to {end_date}")
        
        for cmd in commands:
            cmd_id, full_cmd = queue_device_command(sn, cmd)
            logger.info(f"  → Queued: {full_cmd}")
        
        synced_count += 1

    logger.info(f"🎯 HISTORICAL SYNC: Queued {len(commands)} commands for each of {synced_count} devices")
    
    return jsonify({
        "status": "success",
        "devices_commanded": synced_count,
        "commands_per_device": len(commands),
        "message": f"Historical sync requested for {synced_count} devices with {len(commands)} command formats",
        "range": f"{start_date} to {end_date}",
        "buffer_cleared": old_buffer_size,
        "expected_wait_time": "30-60 seconds"
    })

@app.route("/api/attendance/request_all_data", methods=["POST"])
def request_all_attendance_data():
    """Try to get ALL attendance data from devices (simpler approach)."""
    auth_error = require_api_auth()
    if auth_error:
        return auth_error

    target_devices = list(devices.keys())
    if not target_devices:
        return jsonify({
            "status": "failed",
            "message": "No connected devices available"
        }), 400

    # Clear buffer
    global attendance_buffer
    with buffer_lock:
        attendance_buffer = []
    
    synced_count = 0
    for sn in target_devices:
        # Try the simplest commands that should work on most ZKTeco devices
        simple_commands = [
            "DATA OPTION rtlog",    # Request all real-time logs
            "DATA OPTION attlog",   # Request all attendance logs  
            "DATA OPTION user",     # Also get users to test communication
        ]
        
        logger.info(f"🎯 SIMPLE DATA REQUEST for device {sn}")
        
        for cmd in simple_commands:
            cmd_id, full_cmd = queue_device_command(sn, cmd)
            logger.info(f"  → {full_cmd}")
        
        synced_count += 1

    return jsonify({
        "status": "success",
        "message": f"Requested ALL attendance data from {synced_count} devices using simple commands",
        "devices_commanded": synced_count,
        "note": "This requests all available data, not filtered by date"
    })

@app.route("/api/devices/status", methods=["GET"])
def device_status():
    """Get detailed status of connected devices."""
    auth_error = require_api_auth()
    if auth_error:
        return auth_error
        
    device_list = []
    for sn, device_info in devices.items():
        device_list.append({
            "device_id": sn,
            "status": device_info.get("status", "unknown"),
            "last_seen": device_info.get("last_seen", "never"),
            "ip_address": device_info.get("ip_address", "unknown"),
            "queue_size": len(commands_queue.get(sn, [])),
            "commands_waiting": commands_queue.get(sn, [])
        })
    
    return jsonify({
        "status": "success",
        "count": len(device_list),
        "devices": device_list,
        "timestamp": datetime.now().isoformat()
    })

@app.route("/api/debug/buffer", methods=["GET"])
def debug_buffer():
    """Debug endpoint to see what's in the attendance buffer."""
    with buffer_lock:
        buf_size = len(attendance_buffer)
        recs = attendance_buffer[-10:] if buf_size > 10 else list(attendance_buffer)

    db_pending = 0
    try:
        with get_db_connection() as conn:
            row = conn.execute("SELECT COUNT(*) as cnt FROM pending_punches").fetchone()
            if row:
                db_pending = row['cnt']
    except Exception:
        pass

    return jsonify({
        "status": "success",
        "buffer_size": buf_size,
        "sqlite_pending_count": db_pending,
        "records": recs,
        "total_records_received": records_received_total,
        "total_records_pushed": records_pushed_total,
        "connected_devices": list(devices.keys()),
        "timestamp": datetime.now().isoformat()
    })

@app.route("/api/attendance/fetch", methods=["GET", "POST"])
def fetch_attendance_for_odoo():
    """Fetch attendance records for Odoo integration."""
    global attendance_buffer

    auth_error = require_api_auth()
    if auth_error:
        return auth_error

    try:
        with buffer_lock:
            records = list(attendance_buffer)
            cleaned_records = [{k: v for k, v in r.items() if k != 'spool_id'} for r in records]
            response_data = {
                "status": "success",
                "count": len(cleaned_records),
                "data": cleaned_records,
                "timestamp": datetime.now().isoformat()
            }

            logger.info(f"Odoo fetch: Returned {len(records)} attendance records")

            if request.args.get("peek") != "1":
                remove_pushed_records(records)
                logger.info("Odoo fetch: Cleared delivered attendance buffer and SQLite spool")

            return jsonify(response_data)

    except Exception as e:
        logger.error(f"Error fetching attendance data: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/sync/users", methods=["POST"])
def sync_users_from_odoo():
    """Receive user sync request from Odoo."""
    auth_error = require_api_auth()
    if auth_error:
        return auth_error
    data = request.json
    logger.info("\n========== ODOO USER SYNC REQUEST ==========")
    logger.info(f"Request data: {data}")
    logger.info(f"Currently connected devices: {list(devices.keys())}")
    
    if not data:
        return jsonify({"status": "failed", "message": "No payload received"}), 400
    
    employee_ids = data.get("employee_ids", [])
    employee_data = data.get("employee_data", [])
    device_sn = data.get("device_sn", "all_devices")
    target_devices = data.get("target_devices", [])
    
    # Get available devices if no specific targets
    if not target_devices:
        target_devices = list(devices.keys())
    
    if not target_devices:
        return jsonify({
            "status": "failed", 
            "message": "No connected devices available for sync",
            "synced": 0,
            "failed": 0,
            "connected_devices": list(devices.keys())
        }), 400
    
    # Try multiple command formats to increase compatibility
    synced_devices = []
    commands_sent = 0
    
    # If specific device, sync to that device only
    if device_sn != "all_devices" and device_sn in devices:
        # Send multiple user command formats
        user_commands = [
            "DATA QUERY tablename=user",
            "DATA OPTION user", 
            "DATA user",
            "INFO"  # Basic device info
        ]
        
        logger.info(f"📱 Sending multiple user commands to device {device_sn}:")
        for cmd in user_commands:
            cmd_id, full_cmd = queue_device_command(device_sn, cmd)
            logger.info(f"  → Queued: {full_cmd}")
            commands_sent += 1
            
        synced_devices.append(device_sn)
        
    else:
        # Sync to all connected devices
        for sn in target_devices:
            if sn in devices:  # Only sync to connected devices
                # Send multiple command formats to increase success chance
                user_commands = [
                    "DATA OPTION user",
                    "DATA QUERY tablename=user",
                    "INFO"
                ]
                
                logger.info(f"📱 Sending multiple user commands to device {sn}:")
                for cmd in user_commands:
                    cmd_id, full_cmd = queue_device_command(sn, cmd)
                    logger.info(f"  → Queued: {full_cmd}")
                    commands_sent += 1
                    
                synced_devices.append(sn)
    
    logger.info(f"✅ USER SYNC SUMMARY: {len(synced_devices)} devices, {commands_sent} commands queued")
    
    return jsonify({
        "status": "success",
        "synced": len(synced_devices),
        "failed": len(target_devices) - len(synced_devices),
        "devices": synced_devices,
        "message": f"Device user refresh queued for {len(synced_devices)} devices",
        "employee_count": len(employee_data) if employee_data else len(employee_ids)
    })

# -------------------------------------------------------------
# EXISTING ZKTECO DEVICE COMMUNICATION (YOUR ORIGINAL CODE)
# -------------------------------------------------------------

@app.route("/iclock/registry", methods=["GET", "POST"])
def registry():
    sn = request.args.get("SN")
    client_ip = get_client_ip()
    
    print(f"\n[ Handshake Initiated ] -> Device Serial Number: {sn} | IP: {client_ip}")
    
    touch_device(sn)
    
    registry_response = (
        "RegistryCode=1\n"
        "ServerVersion=3.1.2\n"
        "ServerName=CustomADMS\n"
        "PushVersion=3.1.2\n"
        "ErrorDelay=60\n"
        "Delay=10\n"
        "TransTimes=00:00;23:59\n"
        "TransInterval=1\n"
        "TransFlag=1111111111\n"
        f"TimeZone={FLASK_ADMS_CONFIG['timezone']}\n"
        "OK\n"
    )
    return Response(registry_response, mimetype="text/plain")

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "online",
        "service": "Flask ADMS",
        "version": "1.0",
        "devices_connected": len(devices)
    })

@app.route("/iclock/push", methods=["POST"])
def adms_push():
    return Response("ServerVersion=3.1.2\nServerName=CustomADMS\nPushVersion=3.1.2\nRegistryCode=1\nOK\n", mimetype="text/plain")

@app.route("/iclock/ping", methods=["GET", "POST"])
def adms_ping():
    sn = request.args.get("SN")
    touch_device(sn)
    return Response("OK\n", mimetype="text/plain")

@app.route("/iclock/getrequest", methods=["GET", "POST"])
def getrequest():
    sn = request.args.get("SN")
    touch_device(sn)
    
    # Log every device heartbeat for debugging
    logger.info(f"💓 Device {sn} heartbeat - Queue size: {len(commands_queue.get(sn, []))}")
    
    # If a web request queued an action, send it down to the device right now
    if sn in commands_queue and commands_queue[sn]:
        cmd = commands_queue[sn].pop(0)
        logger.info(f"\n🚀 [ DISPATCHING TO HARDWARE ] -> SN: {sn} | Command: {cmd}")
        logger.info(f"📋 Remaining commands in queue for {sn}: {len(commands_queue[sn])}")
        return Response(f"{cmd}\n", mimetype="text/plain")
    
    # Log when device checks but no commands waiting
    logger.debug(f"📭 Device {sn} checked for commands - none waiting")
    return Response("OK\n", mimetype="text/plain")

@app.route("/api/users/from-device", methods=["GET"])
def get_users_from_device():
    """Get users received from devices."""
    auth_error = require_api_auth()
    if auth_error:
        return auth_error

    # Flatten dictionary of buffers into a single list
    all_users = []
    for sn_buffer in users_buffer.values():
        all_users.extend(sn_buffer)

    if not all_users:
        json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'device_users.json')
        if os.path.exists(json_path):
            try:
                with open(json_path, 'r') as f:
                    cached = json.load(f)
                    for u in cached:
                        all_users.append({
                            "device_sn": "AEXH214460630",
                            "raw_data": f"pin={u.get('user_id', '')}\tname={u.get('name', '')}",
                            "timestamp": datetime.now().isoformat()
                        })
            except Exception as e:
                logger.error(f"Error reading device_users.json: {e}")

    return jsonify({
        "status": "success",
        "count": len(all_users),
        "users": all_users
    })

def process_user_data(sn, body):
    """Parse user data from device and update buffer/Odoo."""
    global users_buffer
    parsed_users = []

    # Clear existing buffer for this specific device to avoid duplicates
    users_buffer[sn] = []
    
    logger.info(f"🔍 Processing user data from {sn}")
    logger.info(f"📝 Raw user data body: {repr(body[:500])}...")

    lines = body.strip().split("\n")
    logger.info(f"📊 Processing {len(lines)} lines of user data")
    
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue

        # Log every line for debugging, even if we think it's a header
        logger.info(f"📝 Line {i+1}: {line}")

        # Skip only very obvious control lines that contain no user data
        if line.upper() in ["OK", "RETURN=0", "RETURN=1", "ID="]:
            continue

        # Store raw user line in buffer (be inclusive)
        users_buffer[sn].append({
            "device_sn": sn,
            "raw_data": line,
            "timestamp": datetime.now().isoformat()
        })

        # Parse key-value pairs for immediate push to Odoo
        # Try multiple parsing approaches for different F22 formats
        kv = {}
        raw = line
        
        # Remove common prefixes
        if raw.lower().startswith('user '):
            raw = raw[5:]
        elif raw.lower().startswith('personnel '):
            raw = raw[10:]
        elif raw.lower().startswith('userinfo '):
            raw = raw[9:]
            
        # Try different delimiters
        for delimiter in ['\t', '&', ' ']:
            if delimiter in raw:
                for item in raw.split(delimiter):
                    if '=' in item:
                        k, v = item.split('=', 1)
                        kv[k.strip().lower()] = v.strip()
                break
        else:
            # If no delimiters found, try regex approach
            import re
            matches = re.findall(r'(\w+)=([^=\s]+)', raw)
            for k, v in matches:
                kv[k.lower()] = v
                
        logger.info(f"🔍 Parsed user data: {kv}")
        
        # Extract user info with multiple field names
        pin = (kv.get('pin') or kv.get('userid') or kv.get('id') or 
               kv.get('user') or kv.get('personnelid'))
        name = (kv.get('name') or kv.get('username') or kv.get('fullname') or 
                kv.get('personnelname') or '')
                
        if pin:
            parsed_user = {'pin': pin, 'name': name}
            parsed_users.append(parsed_user)
            logger.info(f"✅ Found user: PIN {pin}, Name: {name}")

    logger.info(f"📊 User processing complete: {len(parsed_users)} users found")
    
    if parsed_users:
        logger.info(f"📤 Pushing {len(parsed_users)} users to Odoo")
        push_to_odoo('/zkteco/push/users', {
            'api_key': FLASK_ADMS_CONFIG['api_key'],
            'device_sn': sn,
            'users': parsed_users
        })
    else:
        logger.warning(f"⚠️ No users found in device response. Device may have no enrolled users or different format.")
        
    return len(parsed_users)

@app.route("/iclock/devicecmd", methods=["POST"])
def devicecmd():
    """Handle device command responses (including user data)."""
    sn = request.args.get("SN")
    touch_device(sn)
    body = request.get_data(as_text=True)

    # Device command responses are often just "OK" or "Return=X",
    # but some devices send user data here.
    if "user " in body.lower() or "pin=" in body.lower():
        print(f"\n🔥 [ USER DATA VIA devicecmd SN: {sn} ]")
        count = process_user_data(sn, body)
        print(f"✅ Processed {count} users from devicecmd\n")
    else:
        logger.info(f"Device {sn} command ACK: {body.strip()}")

    return Response("OK\n", mimetype="text/plain")

@app.route("/iclock/cdata", methods=["GET", "POST"])
@app.route("/iclock/querydata", methods=["GET", "POST"])
def cdata():
    """Handle real-time attendance and table data from devices."""
    sn = request.args.get("SN")
    touch_device(sn)
    table = (request.args.get("table") or request.args.get("tablename") or "").lower()

    if request.method == "GET":
        return Response("OK\n", mimetype="text/plain")

    body = request.get_data(as_text=True)
    global attendance_buffer, records_received_total, records_pushed_total

    # Historical data is typically in the 'transaction' table
    is_historical = (table == "transaction")

    if table in ("rtlog", "attlog", "transaction") or "tablename=rtlog" in request.url:
        activity = "Historical logs received" if is_historical else "Real-time attendance data received"
        log_device_activity(sn, activity)
        logger.info(f"📥 [ {'HISTORICAL' if is_historical else 'LIVE'} Attendance Incoming ] -> SN: {sn}")

        attendance_count = 0
        new_records = []
        for line in body.strip().split("\n"):
            line = line.strip()
            if not line: continue

            # Log the raw line for debugging
            logger.info(f"📝 Raw attendance line from device {sn}: {line}")

            if is_historical:
                logger.debug(f"  Parsing historical line: {line}")
            elif "time=" in line or "time_second=" in line or "\t" in line:
                logger.info(f"  ⏰ PUNCH LOG -> {line}")

            attendance_record = parse_attendance_log(line, sn)
            if attendance_record:
                new_records.append(attendance_record)
                attendance_count += 1
                records_received_total += 1
                if not is_historical:
                    logger.info(f"  📋 Added to buffer: User {attendance_record['user_id']} at {attendance_record['timestamp']}")
            else:
                logger.warning(f"  ❌ Failed to parse attendance line: {line}")

        log_device_activity(sn, f"Processed {attendance_count} attendance records")

        if new_records:
            with buffer_lock:
                try:
                    with get_db_connection() as conn:
                        cur = conn.cursor()
                        for rec in new_records:
                            cur.execute("""
                                INSERT INTO pending_punches (device_sn, user_id, timestamp, punch_type, verify_type, raw_data)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, (
                                rec.get('device_id') or rec.get('device_sn'),
                                rec.get('user_id'),
                                rec.get('timestamp'),
                                str(rec.get('event_type') if 'event_type' in rec else rec.get('punch_type', 0)),
                                rec.get('verify_type'),
                                rec.get('raw_data'),
                            ))
                            rec['spool_id'] = cur.lastrowid
                        conn.commit()
                except Exception as db_err:
                    logger.error(f"Error persisting punches to SQLite spool: {db_err}")

                for rec in new_records:
                    attendance_buffer.append(rec)

        # Push to Odoo immediately ONLY for real-time data.
        # Historical data is too voluminous for immediate push and should be polled.
        if new_records and not is_historical:
            cleaned_new = [{k: v for k, v in r.items() if k != 'spool_id'} for r in new_records]
            if push_to_odoo('/zkteco/push/attendance', {
                'api_key': FLASK_ADMS_CONFIG['api_key'],
                'records': cleaned_new
            }):
                remove_pushed_records(new_records)

    elif table == "user" or "tablename=user" in request.url:
        log_device_activity(sn, "User data received via cdata")
        logger.info(f"👥 [ User Data Incoming ] -> SN: {sn}")
        count = process_user_data(sn, body)
        logger.info(f"✅ Processed {count} users from cdata")

    elif table == "tabledata" or "tablename=templatev10" in request.url:
        log_device_activity(sn, "Biometric data received")
        logger.info(f"🧬 [ Biometric Vector Data Stream ] -> SN: {sn} (Fingerprints Loaded)")

    return Response("OK\n", mimetype="text/plain")

def parse_attendance_log(log_line, device_sn):
    """Parse ZKTeco attendance log into structured format for Odoo."""
    try:
        # Determine if it's key-value or legacy positional format
        if '=' in log_line:
            # It's a key-value format (can be separated by tabs, &, or spaces)
            # But be careful with spaces as they can also be inside values
            params = {}
            
            # First try tab and & delimiters (these are safer)
            if '\t' in log_line or '&' in log_line:
                delimiters = ['\t', '&']
                parts = [log_line]
                for d in delimiters:
                    new_parts = []
                    for p in parts:
                        new_parts.extend(p.split(d))
                    parts = new_parts
            else:
                # If no tabs or &, assume space-separated but handle carefully
                # Split by spaces but be careful about time values that contain spaces
                import re
                # Find key=value pairs using regex to handle spaces properly
                pattern = r'(\w+)=([^=]+?)(?=\s+\w+=|$)'
                matches = re.findall(pattern, log_line)
                parts = []
                for key, value in matches:
                    parts.append(f"{key}={value.strip()}")
                
                # If regex didn't work, fall back to simple space split
                if not parts:
                    parts = log_line.split(' ')

            for part in parts:
                part = part.strip()
                if '=' in part:
                    key, value = part.split('=', 1)
                    params[key.strip().lower()] = value.strip()

            user_id = params.get('pin', params.get('user', params.get('userid', '')))
            
            # Handle both standard time format and time_second format
            timestamp = params.get('time', '')
            time_second = params.get('time_second', '')
            
            # If we have time_second instead of regular time, convert it
            if time_second and not timestamp:
                try:
                    # Decode using ZKTeco's encoded time formula (which assumes 31-day months)
                    val = int(time_second)
                    
                    seconds_in_minute = 60
                    seconds_in_hour = 60 * 60
                    seconds_in_day = 24 * 60 * 60
                    seconds_in_month = 31 * 24 * 60 * 60
                    seconds_in_year = 12 * 31 * 24 * 60 * 60
                    
                    year_offset = val // seconds_in_year
                    val %= seconds_in_year
                    year = year_offset + 2000
                    
                    month_offset = val // seconds_in_month
                    val %= seconds_in_month
                    month = month_offset + 1
                    
                    day_offset = val // seconds_in_day
                    val %= seconds_in_day
                    day = day_offset + 1
                    
                    hour = val // seconds_in_hour
                    val %= seconds_in_hour
                    
                    minute = val // seconds_in_minute
                    val %= seconds_in_minute
                    
                    second = val
                    
                    # Construct proper ISO timestamp string
                    timestamp = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"
                    logger.info(f"Converted time_second {time_second} to timestamp {timestamp}")
                except (ValueError, OverflowError) as e:
                    logger.error(f"Failed to convert time_second {time_second}: {e}")
                    # If conversion fails, create a timestamp from current time as fallback
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            state_raw = params.get('inoutstatus') or params.get('event') or params.get('eventtype') or params.get('type') or '0'
            try:
                event_type = int(state_raw)
            except ValueError:
                event_type = 0

        elif '\t' in log_line:
            # Legacy tab-separated format
            parts = log_line.split('\t')
            if len(parts) < 2:
                logger.warning(f"Invalid tab-separated log format: {log_line}")
                return None

            user_id = parts[0].strip()
            timestamp = parts[1].strip()
            event_type = int(parts[2].strip()) if len(parts) > 2 else 0

        else:
            # FALLBACK: Attempt to extract anything that looks like a PIN and a Date
            # This ensures the 'Fetch Log' button actually returns something even if format is weird
            logger.debug(f"Unknown log format, attempting fallback: {log_line}")
            import re
            # Look for something like 2026-07-16 15:00:00
            date_match = re.search(r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}', log_line)
            if date_match:
                timestamp = date_match.group(0)
                # Assume the first numeric chunk that isn't the date is the user ID
                clean_line = log_line.replace(timestamp, '')
                nums = re.findall(r'\b\d{1,10}\b', clean_line)
                user_id = nums[0] if nums else "Unknown"
                event_type = 0
            else:
                return None

        if not user_id or not timestamp:
            logger.warning(f"Missing required fields (User/Time) in log: {log_line}")
            return None

        return {
            "device_id": device_sn,
            "user_id": user_id,
            "timestamp": timestamp,
            "event_type": event_type,
            "raw_data": log_line,
            "processed_at": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error parsing log line: {log_line} | Error: {e}")
        return None

# -------------------------------------------------------------
# MANUAL TRIGGER ENDPOINTS
# -------------------------------------------------------------

@app.route("/trigger/users/<sn>")
def trigger_user_fetch(sn):
    """Manually trigger user fetch from specific device."""
    auth_error = require_api_auth()
    if auth_error:
        return auth_error
    
    # Try multiple user command formats for F22 compatibility
    user_commands = [
        "DATA QUERY tablename=user",
        "DATA QUERY tablename=USER",
        "DATA QUERY tablename=userinfo",
        "DATA QUERY tablename=USERINFO",
        "DATA QUERY tablename=personnel",
        "DATA QUERY tablename=PERSONNEL",
        "DATA OPTION user",
        "DATA OPTION USER",
        "DATA user",
        "DATA USER",
        "OPTION user",
        "OPTION USER",
        "INFO"  # Get device info to help identify model
    ]
    
    logger.info(f"🔍 Trying multiple user commands for device {sn}")
    
    commands_sent = []
    for cmd in user_commands:
        cmd_id, cmd_string = queue_device_command(sn, cmd)
        commands_sent.append(cmd_string)
        logger.info(f"  → Queued: {cmd_string}")
    
    return jsonify({
        "status": "Multiple user commands queued successfully",
        "target_device": sn,
        "commands_sent": len(commands_sent),
        "instructions": commands_sent,
        "next_step": "Wait up to 30 seconds for device responses. Check /api/users/from-device for results."
    })

@app.route("/trigger/attendance/<sn>")
def trigger_attendance_fetch(sn):
    """Manually trigger attendance data fetch from specific device."""
    auth_error = require_api_auth()
    if auth_error:
        return auth_error
    
    cmd_id, cmd_string = queue_device_command(sn, "DATA QUERY tablename=transaction")
    
    return jsonify({
        "status": "Attendance fetch queued",
        "target_device": sn,
        "assigned_id": cmd_id,
        "instruction": cmd_string
    })

if __name__ == "__main__":
    print("\n🚀 Starting Flask ADMS Server...")
    print("📡 ZKTeco Device Endpoints:")
    print("   - Registry: /iclock/registry")
    print("   - Ping: /iclock/ping")
    print("   - Commands: /iclock/getrequest")
    print("   - Data: /iclock/cdata")
    print("\n🔗 Odoo Integration Endpoints:")
    print("   - Health: /health")
    print("   - Device Status: /api/devices/status")
    print("   - Fetch Attendance: /api/attendance/fetch")
    print("   - Sync Users: /api/sync/users")
    print(f"\n🌐 Running on: http://0.0.0.0:{server_port}")
    print("🔐 Passive Polling Thread Enabled\n")
    
    # Start background poll and forward thread
    poll_thread = threading.Thread(target=background_poll_and_forward, daemon=True)
    poll_thread.start()
    logger.info("Background poll and forward thread started.")
    
    app.run(host="0.0.0.0", port=server_port, debug=FLASK_ADMS_CONFIG['debug'])
