"""
PIAC Phase 4 — Cloud Backend Server
====================================
Receives metrics from MATLAB via HTTP POST,
stores them in memory (or InfluxDB if configured),
and exposes a REST API for the live dashboard.

Run:
    pip install flask flask-cors
    python PIAC_Phase4_Server.py

Endpoints:
    POST /api/metrics          — receive a single JSON metric
    POST /api/metrics (NDJSON) — receive a batch (newline-delimited JSON)
    GET  /api/latest           — latest metric for each unit
    GET  /api/history?unit=TB-01&n=300  — last N frames for one unit
    GET  /api/units            — list all known unit IDs
    GET  /health               — heartbeat check
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
from collections import deque, defaultdict
import json
import time
import threading

app = Flask(__name__)
CORS(app)   # allow the dashboard HTML to call from any origin

# ── In-memory store ──────────────────────────────────────────────────────
# Each unit gets a deque of the last MAX_HISTORY frames.
# Thread-safe with a lock — MATLAB POST and dashboard GET run concurrently.
MAX_HISTORY = 2000
store       = defaultdict(lambda: deque(maxlen=MAX_HISTORY))
store_lock  = threading.Lock()
latest      = {}   # unit_id → most recent metric dict

# ── Optional InfluxDB integration ────────────────────────────────────────
# Set INFLUX_URL and INFLUX_TOKEN env vars to enable.
# Leave unset to use in-memory store only.
import os
INFLUX_URL    = os.getenv("INFLUX_URL", "")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN", "")
INFLUX_ORG    = os.getenv("INFLUX_ORG", "piac")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "piac_metrics")
influx_client = None

if INFLUX_URL and INFLUX_TOKEN:
    try:
        from influxdb_client import InfluxDBClient
        from influxdb_client.client.write_api import SYNCHRONOUS
        influx_client = InfluxDBClient(
            url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        influx_write  = influx_client.write_api(write_options=SYNCHRONOUS)
        print(f"[P4] InfluxDB connected: {INFLUX_URL}")
    except Exception as e:
        print(f"[P4] InfluxDB not available: {e}")


# ── Helpers ───────────────────────────────────────────────────────────────

def ingest(metric: dict):
    """Store one parsed metric dict. Called for both single and batch."""
    unit = metric.get("unit", "unknown")
    # Stamp server-side receive time if client ts is missing
    if "ts" not in metric:
        metric["ts"] = time.time()
    # Derive RAG status from wear + alarm
    w_t   = metric.get("w_t", 0)
    alarm = metric.get("alarm", 0)
    metric["rag"] = "red" if alarm else ("yellow" if w_t > 0.3 else "green")

    with store_lock:
        store[unit].append(metric)
        latest[unit] = metric

    # Mirror to InfluxDB if configured
    if influx_client:
        try:
            line = (
                f"piac,unit={unit} "
                f"w_t={metric.get('w_t', 0)},"
                f"St_plus={metric.get('St_plus', 0)},"
                f"h_wt={metric.get('h_wt', 0)},"
                f"alarm={int(metric.get('alarm', 0))}i "
                f"{int(metric['ts'] * 1e9)}"   # nanoseconds
            )
            influx_write.write(INFLUX_BUCKET, INFLUX_ORG, line)
        except Exception as e:
            print(f"[P4] InfluxDB write error: {e}")


def check_api_key():
    """Return True if no key configured, or if request header matches."""
    configured = os.getenv("PIAC_API_KEY", "")
    if not configured:
        return True
    return request.headers.get("X-API-Key", "") == configured


# ── Routes ────────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "units": len(latest), "ts": time.time()})


@app.route("/api/metrics", methods=["POST"])
def receive_metrics():
    if not check_api_key():
        return jsonify({"error": "Unauthorized"}), 401

    ct = request.content_type or ""

    # NDJSON batch (from publishBatch())
    if "ndjson" in ct or "plain" in ct:
        lines   = request.data.decode("utf-8").strip().splitlines()
        ingested = 0
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                ingest(json.loads(line))
                ingested += 1
            except json.JSONDecodeError:
                pass
        return jsonify({"ingested": ingested}), 200

    # Single JSON (from publishMetrics())
    try:
        metric = request.get_json(force=True)
        if not isinstance(metric, dict):
            return jsonify({"error": "Expected JSON object"}), 400
        ingest(metric)
        return jsonify({"ok": True}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/latest", methods=["GET"])
def get_latest():
    """Latest metric for every known unit — used by dashboard status cards."""
    with store_lock:
        return jsonify(dict(latest))


@app.route("/api/history", methods=["GET"])
def get_history():
    """Last N frames for one unit — used by dashboard time-series charts."""
    unit = request.args.get("unit", "")
    n    = min(int(request.args.get("n", 300)), MAX_HISTORY)

    if not unit:
        return jsonify({"error": "unit parameter required"}), 400

    with store_lock:
        frames = list(store.get(unit, []))

    return jsonify(frames[-n:])


@app.route("/api/units", methods=["GET"])
def get_units():
    with store_lock:
        return jsonify(list(latest.keys()))


if __name__ == "__main__":
    port = int(os.getenv("PIAC_PORT", 5000))
    print(f"[P4] PIAC server running on http://localhost:{port}")
    print(f"[P4] Dashboard: open PIAC_Dashboard.html in browser")
    print(f"[P4] Test: curl http://localhost:{port}/health")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
