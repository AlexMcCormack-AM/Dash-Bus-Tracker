import time
import io
import csv
import zipfile
import threading
import requests
from flask import Flask, jsonify, render_template_string, request
from google.transit import gtfs_realtime_pb2
from dotenv import load_dotenv
import os
from collections import defaultdict

load_dotenv()

app = Flask(__name__)

API_KEY        = os.getenv("SWIFTLY_API_KEY", "4af18b965e8a21f6015686f2f208f95f")
SMS_GATEWAY    = os.getenv("SMS_GATEWAY", "")
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")

# ── Active alert (one at a time) ─────────────────────────────────────────────
_alert_lock = threading.Lock()
_active_alert = None  # {stop_id, route_id, headsign, threshold, fired}

# ── GTFS data (loaded once at startup) ──────────────────────────────────────

_routes = {}          # route_id -> {name, color, text_color}
_headsigns = {}       # route_id -> [headsign, ...]
_route_stops = {}     # (route_id, headsign) -> [(stop_id, stop_name), ...]
_stop_names = {}      # stop_id -> stop_name
_trip_headsigns = {}  # trip_id -> headsign


def load_gtfs():
    global _routes, _headsigns, _route_stops, _stop_names

    print("Downloading GTFS data...")
    r = requests.get("https://www.dashbus.com/google_transit.zip", timeout=15)
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:

        # Routes
        with z.open("routes.txt") as f:
            for row in csv.DictReader(io.TextIOWrapper(f)):
                _routes[row["route_id"]] = {
                    "id": row["route_id"],
                    "short_name": row["route_short_name"],
                    "long_name": row["route_long_name"],
                    "color": "#" + row["route_color"] if row["route_color"] else "#3b82f6",
                    "text_color": "#" + row["route_text_color"] if row["route_text_color"] else "#ffffff",
                }

        # Stops
        with z.open("stops.txt") as f:
            for row in csv.DictReader(io.TextIOWrapper(f)):
                _stop_names[row["stop_id"]] = row["stop_name"]

        # Trips — map trip_id -> (route_id, headsign, direction_id)
        route_headsigns = {}  # (route_id, headsign) -> representative trip_id
        with z.open("trips.txt") as f:
            for row in csv.DictReader(io.TextIOWrapper(f)):
                _trip_headsigns[row["trip_id"]] = row["trip_headsign"]
                # Keep one representative trip per route+headsign
                key = (row["route_id"], row["trip_headsign"])
                if key not in route_headsigns:
                    route_headsigns[key] = row["trip_id"]

        # Build headsigns list per route
        for (route_id, headsign) in route_headsigns:
            if route_id not in _headsigns:
                _headsigns[route_id] = []
            if headsign not in _headsigns[route_id]:
                _headsigns[route_id].append(headsign)

        # Stop times — build ordered stop list per (route, headsign)
        rep_trips = set(route_headsigns.values())
        trip_stops = defaultdict(list)  # trip_id -> [(seq, stop_id)]
        with z.open("stop_times.txt") as f:
            for row in csv.DictReader(io.TextIOWrapper(f)):
                if row["trip_id"] in rep_trips:
                    trip_stops[row["trip_id"]].append(
                        (int(row["stop_sequence"]), row["stop_id"])
                    )

        for (route_id, headsign), trip_id in route_headsigns.items():
            stops = sorted(trip_stops[trip_id], key=lambda x: x[0])
            _route_stops[(route_id, headsign)] = [
                {"id": sid, "name": _stop_names.get(sid, sid)}
                for _, sid in stops
            ]

    print(f"Loaded {len(_routes)} routes, {len(_stop_names)} stops")


# ── Real-time ────────────────────────────────────────────────────────────────

def get_upcoming_buses(stop_id):
    r = requests.get(
        "https://api.goswift.ly/real-time/alexandria-dash/gtfs-rt-trip-updates",
        headers={"Authorization": API_KEY},
        timeout=10,
    )
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)

    # Build trip_id -> headsign from static data
    from_trips = {}
    for (route_id, headsign), trip_id in {}.items():
        from_trips[trip_id] = headsign

    now = int(time.time())
    buses = []
    for entity in feed.entity:
        if entity.HasField("trip_update"):
            trip_id = entity.trip_update.trip.trip_id
            route_id = entity.trip_update.trip.route_id
            for stu in entity.trip_update.stop_time_update:
                if stu.stop_id == stop_id:
                    arrival = stu.arrival.time if stu.HasField("arrival") else None
                    if arrival and arrival > now:
                        mins = round((arrival - now) / 60, 1)
                        # Find headsign by matching trip_id against static trips
                        headsign = _get_headsign_for_trip(trip_id)
                        buses.append({
                            "trip_id": trip_id,
                            "route": route_id,
                            "headsign": headsign,
                            "mins": mins,
                        })

    return sorted(buses, key=lambda x: x["mins"])


def _get_headsign_for_trip(trip_id):
    """Look up headsign from cached GTFS trips data."""
    return _trip_headsigns.get(trip_id, "")


# ── API routes ───────────────────────────────────────────────────────────────

@app.route("/api/routes")
def api_routes():
    return jsonify(list(_routes.values()))


@app.route("/api/directions")
def api_directions():
    route_id = request.args.get("route_id", "")
    headsigns = _headsigns.get(route_id, [])
    filtered = [h for h in headsigns if "(SHORT TRIP)" not in h and "(SPECIAL)" not in h and "(SCHOOL SPECIAL)" not in h]
    headsigns = filtered if filtered else headsigns

    results = []
    for headsign in headsigns:
        stops = _route_stops.get((route_id, headsign), [])
        origin = stops[0]["name"] if stops else ""
        results.append({
            "headsign": headsign,
            "origin": origin,
            "label": f"{origin} → {headsign}" if origin else headsign,
        })
    return jsonify(results)


@app.route("/api/stops")
def api_stops():
    route_id = request.args.get("route_id", "")
    headsign = request.args.get("headsign", "")
    stops = _route_stops.get((route_id, headsign), [])
    return jsonify(stops)


@app.route("/api/buses")
def api_buses():
    stop_id = request.args.get("stop_id", "")
    stop_name = _stop_names.get(stop_id, stop_id)
    try:
        buses = get_upcoming_buses(stop_id)
        return jsonify({
            "stop": stop_name,
            "stop_id": stop_id,
            "buses": buses,
            "updated": int(time.time())
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── SMS ──────────────────────────────────────────────────────────────────────

def send_text(message):
    if not RESEND_API_KEY or not SMS_GATEWAY:
        print(f"[SMS skipped — no credentials]: {message}")
        return
    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "from": "DASH Bus Tracker <onboarding@resend.dev>",
                "to": [SMS_GATEWAY],
                "subject": "Bus Alert",
                "text": message,
            },
            timeout=10,
        )
        print(f"Resend response {resp.status_code}: {resp.text}")
        if resp.status_code == 200:
            print(f"Text sent: {message}")
    except Exception as e:
        import traceback
        print(f"SMS error: {e}")
        print(traceback.format_exc())


# ── Alert endpoints ───────────────────────────────────────────────────────────

@app.route("/api/alert", methods=["POST"])
def set_alert():
    global _active_alert
    data = request.json
    with _alert_lock:
        _active_alert = {
            "stop_id":   data["stop_id"],
            "route_id":  data["route_id"],
            "headsign":  data["headsign"],
            "threshold": int(data["threshold"]),
            "fired":     False,
        }
    print(f"Alert set: Route {data['route_id']} at stop {data['stop_id']} within {data['threshold']} min")
    return jsonify({"status": "ok"})


@app.route("/api/alert", methods=["DELETE"])
def cancel_alert():
    global _active_alert
    with _alert_lock:
        _active_alert = None
    return jsonify({"status": "ok"})


# ── Background alert thread ───────────────────────────────────────────────────

def alert_worker():
    global _active_alert
    while True:
        try:
            with _alert_lock:
                alert = dict(_active_alert) if _active_alert else None

            if alert and not alert["fired"]:
                buses = get_upcoming_buses(alert["stop_id"])
                print(f"Alert check: looking for route={alert['route_id']} headsign='{alert['headsign']}' threshold={alert['threshold']}")
                print(f"  Found buses: {[(b['route'], b['headsign'], b['mins']) for b in buses]}")
                for bus in buses:
                    if bus["route"] == alert["route_id"] and bus["headsign"] == alert["headsign"]:
                        if bus["mins"] <= alert["threshold"] + 0.9:
                            stop_name = _stop_names.get(alert["stop_id"], alert["stop_id"])
                            send_text(f"Route {bus['route']} is {int(bus['mins'])} min away at {stop_name}")
                            with _alert_lock:
                                if _active_alert:
                                    _active_alert["fired"] = True
                            break
        except Exception as e:
            print(f"Alert worker error: {e}")

        time.sleep(30)


@app.route("/")
def index():
    return render_template_string(HTML)


# ── Frontend ─────────────────────────────────────────────────────────────────

HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>DASH Bus Tracker</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: #0f172a;
      color: #f1f5f9;
      min-height: 100vh;
      padding: 0;
      max-width: 480px;
      margin: 0 auto;
    }

    .header {
      padding: 20px 16px 12px;
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .back-btn {
      background: #1e293b;
      border: none;
      color: #94a3b8;
      font-size: 1.2rem;
      width: 36px; height: 36px;
      border-radius: 10px;
      cursor: pointer;
      display: none;
      align-items: center;
      justify-content: center;
      flex-shrink: 0;
    }
    .back-btn.visible { display: flex; }
    .header-title { font-size: 1.2rem; font-weight: 700; }
    .header-sub { font-size: 0.8rem; color: #64748b; margin-top: 2px; }

    .screen { display: none; padding: 0 16px 24px; }
    .screen.active { display: block; }

    /* Route grid */
    .route-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    .route-btn {
      border: none;
      border-radius: 14px;
      padding: 16px;
      cursor: pointer;
      text-align: left;
      transition: opacity 0.1s;
    }
    .route-btn:active { opacity: 0.8; }
    .route-num { font-size: 1.4rem; font-weight: 800; }
    .route-name { font-size: 0.7rem; font-weight: 500; margin-top: 2px; opacity: 0.85; }

    /* Direction & stop lists */
    .list-item {
      background: #1e293b;
      border-radius: 14px;
      padding: 16px;
      margin-bottom: 10px;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: space-between;
      font-size: 1rem;
    }
    .list-item:active { background: #334155; }
    .list-item .arrow { color: #475569; }
    .stop-seq {
      font-size: 0.75rem;
      color: #475569;
      margin-right: 12px;
      min-width: 20px;
    }

    /* Bus cards */
    .bus-card {
      background: #1e293b;
      border-radius: 16px;
      padding: 16px 20px;
      margin-bottom: 10px;
    }
    .bus-card.alert-match { border-left: 3px solid #f97316; }
    .bus-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .route-badge {
      color: white;
      font-size: 1rem;
      font-weight: 700;
      padding: 6px 14px;
      border-radius: 8px;
    }
    .mins { font-size: 2rem; font-weight: 800; }
    .mins span { font-size: 0.9rem; font-weight: 400; color: #94a3b8; margin-left: 3px; }
    .mins.urgent { color: #f97316; }
    .mins.soon   { color: #facc15; }
    .mins.ok     { color: #4ade80; }
    .headsign {
      font-size: 0.8rem;
      color: #64748b;
      margin-top: 6px;
    }

    /* Active alert banner */
    .alert-banner {
      background: #1e3a5f;
      border: 1px solid #3b82f6;
      border-radius: 12px;
      padding: 12px 16px;
      margin-bottom: 16px;
      display: none;
      align-items: center;
      justify-content: space-between;
      font-size: 0.85rem;
    }
    .alert-banner.active { display: flex; }
    .alert-banner-text { color: #93c5fd; }
    .alert-banner-text strong { color: #f1f5f9; display: block; font-size: 0.9rem; }
    .cancel-alert {
      background: none; border: none; color: #64748b;
      font-size: 1.1rem; cursor: pointer; padding: 4px;
    }

    /* Modal overlay */
    .modal-overlay {
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(0,0,0,0.7);
      z-index: 100;
      align-items: flex-end;
      justify-content: center;
    }
    .modal-overlay.open { display: flex; }
    .modal {
      background: #1e293b;
      border-radius: 20px 20px 0 0;
      padding: 24px 20px 40px;
      width: 100%;
      max-width: 480px;
    }
    .modal-title { font-size: 1.1rem; font-weight: 700; margin-bottom: 4px; }
    .modal-sub { font-size: 0.8rem; color: #64748b; margin-bottom: 24px; }
    .modal-threshold-row {
      display: flex; align-items: center;
      justify-content: space-between; margin-bottom: 8px;
    }
    .modal-threshold-label { font-size: 0.9rem; color: #94a3b8; }
    .modal-threshold-val { font-size: 1rem; font-weight: 700; color: #3b82f6; }
    input[type=range] { width: 100%; accent-color: #3b82f6; margin-bottom: 24px; }
    .set-alert-btn {
      display: block; width: 100%; padding: 15px;
      background: #3b82f6; color: white; border: none;
      border-radius: 14px; font-size: 1rem; font-weight: 700; cursor: pointer;
    }
    .set-alert-btn:active { background: #2563eb; }
    .cancel-modal-btn {
      display: block; width: 100%; padding: 13px; margin-top: 10px;
      background: #334155; color: #94a3b8; border: none;
      border-radius: 14px; font-size: 0.9rem; cursor: pointer;
    }

    .section-label {
      font-size: 0.7rem;
      font-weight: 600;
      color: #475569;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      margin-bottom: 10px;
    }
    .updated { text-align: center; color: #334155; font-size: 0.75rem; margin-top: 16px; }
    .empty { text-align: center; color: #475569; padding: 40px 0; }
    .refresh-btn {
      display: block; width: 100%; margin-top: 12px;
      padding: 13px; background: #1e293b; color: #64748b;
      border: none; border-radius: 12px; font-size: 0.9rem; cursor: pointer;
    }
    .refresh-btn:active { background: #334155; }
  </style>
</head>
<body>

<div class="header">
  <button class="back-btn" id="back-btn" onclick="goBack()">&#8592;</button>
  <div>
    <div class="header-title" id="header-title">DASH Bus Tracker</div>
    <div class="header-sub" id="header-sub">Alexandria, VA</div>
  </div>
</div>

<!-- Screen 1: Routes -->
<div class="screen active" id="screen-routes">
  <div class="section-label">Select a route</div>
  <div class="route-grid" id="route-grid"></div>
</div>

<!-- Screen 2: Directions -->
<div class="screen" id="screen-directions">
  <div class="section-label">Select direction</div>
  <div id="direction-list"></div>
</div>

<!-- Screen 3: Stops -->
<div class="screen" id="screen-stops">
  <div class="section-label">Select your stop</div>
  <div id="stop-list"></div>
</div>

<!-- Screen 4: Arrivals -->
<div class="screen" id="screen-arrivals">
  <div class="alert-banner" id="alert-banner">
    <div class="alert-banner-text">
      <strong id="alert-banner-title">Alert active</strong>
      <span id="alert-banner-sub"></span>
    </div>
    <button class="cancel-alert" onclick="cancelAlert()">✕</button>
  </div>
  <div class="section-label">Tap a bus to set an alert</div>
  <div id="buses"></div>
  <div class="updated" id="updated"></div>
  <button class="refresh-btn" onclick="loadBuses()">Refresh</button>
</div>

<!-- Alert modal -->
<div class="modal-overlay" id="modal-overlay" onclick="closeModal(event)">
  <div class="modal">
    <div class="modal-title" id="modal-title">Set alert</div>
    <div class="modal-sub" id="modal-sub"></div>
    <div class="modal-threshold-row">
      <span class="modal-threshold-label">Alert me when within</span>
      <span class="modal-threshold-val" id="modal-threshold-val">10 min</span>
    </div>
    <input type="range" id="modal-threshold" min="3" max="30" value="10" step="1">
    <button class="set-alert-btn" onclick="confirmAlert()">Set Alert</button>
    <button class="cancel-modal-btn" onclick="closeModal()">Cancel</button>
  </div>
</div>

<script>
  // State
  let state = { route: null, headsign: null, stop: null };
  let alertThreshold = 10;
  let refreshTimer = null;
  let screenStack = ['routes'];

  // Route colors cache
  let routeColors = {};

  // ── Navigation ──────────────────────────────────────────────────────────

  function showScreen(name) {
    document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
    document.getElementById('screen-' + name).classList.add('active');
    screenStack.push(name);
    document.getElementById('back-btn').classList.toggle('visible', screenStack.length > 1);
  }

  function goBack() {
    screenStack.pop();
    const prev = screenStack[screenStack.length - 1];
    document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
    document.getElementById('screen-' + prev).classList.add('active');
    document.getElementById('back-btn').classList.toggle('visible', screenStack.length > 1);
    updateHeader();
    if (refreshTimer) { clearInterval(refreshTimer); refreshTimer = null; }
  }

  function updateHeader() {
    const cur = screenStack[screenStack.length - 1];
    const titles = {
      routes: ['DASH Bus Tracker', 'Alexandria, VA'],
      directions: [state.route ? `Route ${state.route.short_name}` : 'Route', state.route ? state.route.long_name : ''],
      stops: [state.headsign || 'Direction', 'Select your stop'],
      arrivals: [state.stop ? state.stop.name : 'Stop', state.headsign || ''],
    };
    const [title, sub] = titles[cur] || ['DASH', ''];
    document.getElementById('header-title').textContent = title;
    document.getElementById('header-sub').textContent = sub;
  }

  // ── Screen 1: Routes ────────────────────────────────────────────────────

  async function loadRoutes() {
    const res = await fetch('/api/routes');
    const routes = await res.json();
    const grid = document.getElementById('route-grid');
    grid.innerHTML = routes.map(r => {
      routeColors[r.id] = r.color;
      return `<button class="route-btn"
        style="background:${r.color};color:${r.text_color}"
        onclick="selectRoute(${JSON.stringify(r).replace(/"/g,'&quot;')})">
        <div class="route-num">${r.short_name}</div>
        <div class="route-name">${r.long_name}</div>
      </button>`;
    }).join('');
  }

  async function selectRoute(route) {
    state.route = route;
    screenStack = ['routes'];
    showScreen('directions');
    updateHeader();

    const res = await fetch('/api/directions?route_id=' + route.id);
    const dirs = await res.json();
    document.getElementById('direction-list').innerHTML = dirs.map(d =>
      `<div class="list-item" onclick="selectDirection('${d.headsign.replace(/'/g,"\\'")}')">
        <span>${d.label}</span><span class="arrow">›</span>
      </div>`
    ).join('');
  }

  // ── Screen 2: Directions ────────────────────────────────────────────────

  async function selectDirection(headsign) {
    state.headsign = headsign;
    showScreen('stops');
    updateHeader();

    const res = await fetch(`/api/stops?route_id=${state.route.id}&headsign=${encodeURIComponent(headsign)}`);
    const stops = await res.json();
    document.getElementById('stop-list').innerHTML = stops.map((s, i) =>
      `<div class="list-item" onclick="selectStop(${JSON.stringify(s).replace(/"/g,'&quot;')})">
        <span class="stop-seq">${i + 1}</span>
        <span style="flex:1">${s.name}</span>
        <span class="arrow">›</span>
      </div>`
    ).join('');
  }

  // ── Screen 3: Stops ─────────────────────────────────────────────────────

  function selectStop(stop) {
    state.stop = stop;
    showScreen('arrivals');
    updateHeader();
    loadBuses();
    refreshTimer = setInterval(loadBuses, 30000);
  }

  // ── Screen 4: Arrivals ──────────────────────────────────────────────────

  let lastBuses = [];
  let activeAlert = null;   // { trip_id, route, headsign, threshold }
  let pendingBus = null;    // bus being configured in modal

  function minsClass(m) {
    if (m <= 5) return 'urgent';
    if (m <= 10) return 'soon';
    return 'ok';
  }

  function renderBuses(buses) {
    lastBuses = buses;
    const container = document.getElementById('buses');
    if (!buses || buses.length === 0) {
      container.innerHTML = '<div class="empty">No buses found right now</div>';
      return;
    }
    const color = state.route ? routeColors[state.route.id] : '#3b82f6';
    container.innerHTML = buses.map(b => {
      const m = Math.round(b.mins);
      const cls = minsClass(b.mins);
      const isAlerted = activeAlert && activeAlert.trip_id === b.trip_id;
      return `<div class="bus-card ${isAlerted ? 'alert-match' : ''}" onclick="openAlertModal(${JSON.stringify(b).replace(/"/g,'&quot;')})">
        <div class="bus-top">
          <div class="route-badge" style="background:${color}">Route ${b.route}</div>
          <div class="mins ${cls}">${m}<span>min</span></div>
        </div>
        ${b.headsign ? `<div class="headsign">toward ${b.headsign}</div>` : ''}
        ${isAlerted ? `<div class="headsign" style="color:#93c5fd;margin-top:4px">🔔 Alert set — ${activeAlert.threshold} min</div>` : ''}
      </div>`;
    }).join('');
  }

  async function loadBuses() {
    if (!state.stop) return;
    try {
      const res = await fetch('/api/buses?stop_id=' + state.stop.id);
      const data = await res.json();
      renderBuses(data.buses || []);
      const d = new Date(data.updated * 1000);
      document.getElementById('updated').textContent = 'Updated ' + d.toLocaleTimeString();
    } catch(e) {
      document.getElementById('buses').innerHTML = '<div class="empty">Error loading buses</div>';
    }
  }

  // ── Alert modal ──────────────────────────────────────────────────────────

  function openAlertModal(bus) {
    pendingBus = bus;
    document.getElementById('modal-title').textContent = `Route ${bus.route}`;
    document.getElementById('modal-sub').textContent = bus.headsign ? `toward ${bus.headsign}` : '';
    const slider = document.getElementById('modal-threshold');
    slider.value = activeAlert ? activeAlert.threshold : 10;
    document.getElementById('modal-threshold-val').textContent = slider.value + ' min';
    document.getElementById('modal-overlay').classList.add('open');
  }

  function closeModal(e) {
    if (e && e.target !== document.getElementById('modal-overlay')) return;
    document.getElementById('modal-overlay').classList.remove('open');
    pendingBus = null;
  }

  document.getElementById('modal-threshold').addEventListener('input', function() {
    document.getElementById('modal-threshold-val').textContent = this.value + ' min';
  });

  async function confirmAlert() {
    if (!pendingBus) return;
    const threshold = parseInt(document.getElementById('modal-threshold').value);
    activeAlert = {
      trip_id: pendingBus.trip_id,
      route: pendingBus.route,
      headsign: pendingBus.headsign,
      threshold,
    };

    // Register alert server-side
    await fetch('/api/alert', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        stop_id:   state.stop.id,
        route_id:  pendingBus.route,
        headsign:  pendingBus.headsign,
        threshold,
      })
    });

    document.getElementById('alert-banner').classList.add('active');
    document.getElementById('alert-banner-title').textContent = `Route ${activeAlert.route} alert active`;
    document.getElementById('alert-banner-sub').textContent = `Notifying at ${activeAlert.threshold} min`;
    document.getElementById('modal-overlay').classList.remove('open');
    pendingBus = null;
    renderBuses(lastBuses);
  }

  async function cancelAlert() {
    activeAlert = null;
    await fetch('/api/alert', { method: 'DELETE' });
    document.getElementById('alert-banner').classList.remove('active');
    renderBuses(lastBuses);
  }

  // ── Init ─────────────────────────────────────────────────────────────────
  loadRoutes();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    load_gtfs()
    t = threading.Thread(target=alert_worker, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=5001, debug=False)
