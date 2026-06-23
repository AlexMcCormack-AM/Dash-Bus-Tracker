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

API_KEY        = os.getenv("SWIFTLY_API_KEY", "")
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
_stop_coords = {}     # stop_id -> [lat, lon]
_stops_by_name = {}   # stop_name -> [stop_id, ...]  (names repeat across directions)
_stop_routes = {}     # stop_id -> set(route_id)  (routes serving a stop)
_trip_headsigns = {}  # trip_id -> headsign
_trip_routes = {}     # trip_id -> route_id  (backfills blank route_id in vehicle feed)
_shapes = {}          # shape_id -> [[lat, lon], ...] in order
_route_shapes = {}    # (route_id, headsign) -> shape_id


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
                _stops_by_name.setdefault(row["stop_name"], []).append(row["stop_id"])
                try:
                    _stop_coords[row["stop_id"]] = [float(row["stop_lat"]), float(row["stop_lon"])]
                except (ValueError, KeyError):
                    pass

        # Trips — map trip_id -> (route_id, headsign, direction_id)
        route_headsigns = {}  # (route_id, headsign) -> representative trip_id
        with z.open("trips.txt") as f:
            for row in csv.DictReader(io.TextIOWrapper(f)):
                _trip_headsigns[row["trip_id"]] = row["trip_headsign"]
                _trip_routes[row["trip_id"]] = row["route_id"]
                # Keep one representative trip per route+headsign
                key = (row["route_id"], row["trip_headsign"])
                if key not in route_headsigns:
                    route_headsigns[key] = row["trip_id"]
                # First shape seen for a route+headsign is good enough to draw the line
                if key not in _route_shapes and row.get("shape_id"):
                    _route_shapes[key] = row["shape_id"]

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
                {"id": sid, "name": _stop_names.get(sid, sid),
                 "lat": _stop_coords.get(sid, [None, None])[0],
                 "lon": _stop_coords.get(sid, [None, None])[1]}
                for _, sid in stops
            ]

        # stop_id -> set of routes serving it (for "stops near me")
        for (route_id, _hs), slist in _route_stops.items():
            for s in slist:
                _stop_routes.setdefault(s["id"], set()).add(route_id)

        # Shapes — only those referenced by a route we keep (saves memory)
        wanted_shapes = set(_route_shapes.values())
        shape_pts = defaultdict(list)  # shape_id -> [(seq, lat, lon)]
        if "shapes.txt" in z.namelist():
            with z.open("shapes.txt") as f:
                for row in csv.DictReader(io.TextIOWrapper(f)):
                    if row["shape_id"] in wanted_shapes:
                        shape_pts[row["shape_id"]].append((
                            int(row["shape_pt_sequence"]),
                            float(row["shape_pt_lat"]),
                            float(row["shape_pt_lon"]),
                        ))
            for sid, pts in shape_pts.items():
                _shapes[sid] = [[lat, lon] for _, lat, lon in sorted(pts)]

    print(f"Loaded {len(_routes)} routes, {len(_stop_names)} stops, {len(_shapes)} shapes")


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


def get_vehicle_positions(route_id=""):
    """Live bus locations from the GTFS-RT vehicle-positions feed."""
    r = requests.get(
        "https://api.goswift.ly/real-time/alexandria-dash/gtfs-rt-vehicle-positions",
        headers={"Authorization": API_KEY},
        timeout=10,
    )
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)

    vehicles = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle
        trip_id = v.trip.trip_id
        # route_id is sometimes blank in the feed — backfill from static GTFS
        rid = v.trip.route_id or _trip_routes.get(trip_id, "")
        if route_id and rid != route_id:
            continue
        if not v.HasField("position"):
            continue
        status = ""
        if v.HasField("current_status"):
            status = {0: "INCOMING_AT", 1: "STOPPED_AT", 2: "IN_TRANSIT_TO"}.get(v.current_status, "")
        vehicles.append({
            "route": rid,
            "trip_id": trip_id,
            "headsign": _trip_headsigns.get(trip_id, ""),
            "lat": v.position.latitude,
            "lon": v.position.longitude,
            "stop_id": v.stop_id if v.HasField("stop_id") else "",
            "status": status,
        })
    return vehicles


def _arrivals_at_stops(stop_ids):
    """Fetch the trip-updates feed once, return upcoming arrivals for any of stop_ids."""
    r = requests.get(
        "https://api.goswift.ly/real-time/alexandria-dash/gtfs-rt-trip-updates",
        headers={"Authorization": API_KEY},
        timeout=10,
    )
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)
    now = int(time.time())
    out = []
    stop_ids = set(stop_ids)
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        for stu in tu.stop_time_update:
            if stu.stop_id in stop_ids:
                arrival = stu.arrival.time if stu.HasField("arrival") else None
                if arrival and arrival > now:
                    out.append({
                        "stop_id": stu.stop_id,
                        "route": tu.trip.route_id,
                        "headsign": _trip_headsigns.get(tu.trip.trip_id, ""),
                        "mins": round((arrival - now) / 60, 1),
                    })
    return out


def find_direct_trips(origin_ids, dest_ids):
    """Direct (single-bus) connections: route+direction where an origin stop
    precedes a destination stop in the ordered stop list. Direction is inferred."""
    origin_ids, dest_ids = set(origin_ids), set(dest_ids)
    results = []
    for (route_id, headsign), stops in _route_stops.items():
        idx = {s["id"]: i for i, s in enumerate(stops)}
        o_hits = [idx[s] for s in origin_ids if s in idx]
        d_hits = [idx[s] for s in dest_ids if s in idx]
        if not o_hits or not d_hits:
            continue
        if min(o_hits) < max(d_hits):
            # earliest valid boarding stop on this pattern
            board = min((s for s in origin_ids if s in idx), key=lambda s: idx[s])
            results.append({"route": route_id, "headsign": headsign, "origin_stop_id": board})
    return results


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


@app.route("/api/shape")
def api_shape():
    """Ordered [[lat, lon], ...] polyline for a route+direction."""
    route_id = request.args.get("route_id", "")
    headsign = request.args.get("headsign", "")
    shape_id = _route_shapes.get((route_id, headsign))
    return jsonify(_shapes.get(shape_id, []))


@app.route("/api/vehicles")
def api_vehicles():
    """Live bus positions, optionally filtered to one route."""
    route_id = request.args.get("route_id", "")
    try:
        return jsonify({
            "vehicles": get_vehicle_positions(route_id),
            "updated": int(time.time()),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stopnames")
def api_stopnames():
    """Unique stop names for the destination/origin pickers."""
    return jsonify(sorted(_stops_by_name.keys()))


@app.route("/api/trip")
def api_trip():
    """Next direct buses from an origin stop toward a destination stop (by name)."""
    origin_name = request.args.get("origin", "").strip()
    dest_name = request.args.get("dest", "").strip()
    origin_ids = _stops_by_name.get(origin_name, [])
    dest_ids = _stops_by_name.get(dest_name, [])
    if not origin_ids or not dest_ids:
        return jsonify({"error": "Pick a valid origin and destination", "trips": []}), 200

    direct = find_direct_trips(origin_ids, dest_ids)
    if not direct:
        return jsonify({"trips": [], "direct": False, "dest": dest_name,
                        "updated": int(time.time())})

    want = {(d["route"], d["headsign"]) for d in direct}
    board_ids = {d["origin_stop_id"] for d in direct}
    try:
        arrivals = _arrivals_at_stops(board_ids)
    except Exception as e:
        return jsonify({"error": str(e), "trips": []}), 500

    trips = []
    for a in arrivals:
        if (a["route"], a["headsign"]) in want:
            trips.append({
                "route": a["route"],
                "mins": a["mins"],
                "color": _routes.get(a["route"], {}).get("color", "#3b82f6"),
            })
    trips.sort(key=lambda x: x["mins"])
    return jsonify({"trips": trips, "direct": True, "dest": dest_name,
                    "updated": int(time.time())})


@app.route("/api/allshapes")
def api_allshapes():
    """Every route's polyline (decimated) + color, for the faint base layer."""
    seen = set()
    out = []
    for (route_id, headsign), sid in _route_shapes.items():
        if sid in seen:
            continue
        seen.add(sid)
        pts = _shapes.get(sid, [])
        if not pts:
            continue
        out.append({
            "route": route_id,
            "color": _routes.get(route_id, {}).get("color", "#888888"),
            "points": pts[::3] if len(pts) > 60 else pts,  # thin out for a light base layer
        })
    return jsonify(out)


@app.route("/api/nearby")
def api_nearby():
    """Nearest stops to a lat/lon, with the routes serving each."""
    import math
    try:
        lat = float(request.args.get("lat"))
        lon = float(request.args.get("lon"))
    except (TypeError, ValueError):
        return jsonify({"error": "Need lat and lon", "stops": []}), 400

    def meters(a, b, c, d):
        R = 6371000.0
        p1, p2 = math.radians(a), math.radians(c)
        dphi, dl = math.radians(c - a), math.radians(d - b)
        x = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
        return 2 * R * math.asin(math.sqrt(x))

    out = []
    for sid, coord in _stop_coords.items():
        routes = sorted(_stop_routes.get(sid, []))
        if not routes:
            continue  # skip stops not on any kept route
        out.append({
            "id": sid,
            "name": _stop_names.get(sid, sid),
            "meters": round(meters(lat, lon, coord[0], coord[1])),
            "routes": [{"id": r, "color": _routes.get(r, {}).get("color", "#888888")} for r in routes],
        })
    out.sort(key=lambda x: x["meters"])
    return jsonify({"stops": out[:8]})


# ── SMS ──────────────────────────────────────────────────────────────────────

def send_text(message):
    resend_key = os.environ.get("RESEND_API_KEY", "")
    sms_gateway = os.environ.get("SMS_GATEWAY", "")
    print(f"send_text: RESEND_API_KEY={'set ('+resend_key[:8]+')' if resend_key else 'EMPTY'} SMS_GATEWAY='{sms_gateway}'")
    if not resend_key or not sms_gateway:
        print(f"[SMS skipped — no credentials]: {message}")
        return
    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {resend_key}",
                "Content-Type": "application/json",
            },
            json={
                "from": "DASH Bus Tracker <onboarding@resend.dev>",
                "to": [sms_gateway],
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


@app.route("/trip")
def trip_page():
    return render_template_string(TRIP_HTML)


@app.route("/explore")
def explore_page():
    return render_template_string(EXPLORE_HTML)


# ── Trip prototype page (destination-first, direct trips only) ────────────────

TRIP_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Where to?</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: #0f172a; color: #f1f5f9; max-width: 480px; margin: 0 auto;
      padding: 24px 16px;
    }
    h1 { font-size: 1.3rem; margin-bottom: 4px; }
    .sub { color: #64748b; font-size: 0.85rem; margin-bottom: 24px; }
    label { display: block; font-size: 0.7rem; font-weight: 600; color: #475569;
            text-transform: uppercase; letter-spacing: 0.06em; margin: 14px 0 6px; }
    input {
      width: 100%; padding: 14px; font-size: 1rem;
      background: #1e293b; color: #f1f5f9; border: 1px solid #334155;
      border-radius: 12px;
    }
    input:focus { outline: none; border-color: #3b82f6; }
    .go {
      width: 100%; margin-top: 20px; padding: 15px;
      background: #3b82f6; color: #fff; border: none; border-radius: 14px;
      font-size: 1rem; font-weight: 700; cursor: pointer;
    }
    .go:active { background: #2563eb; }
    #results { margin-top: 24px; }
    .trip-card {
      background: #1e293b; border-radius: 16px; padding: 16px 20px;
      margin-bottom: 10px; display: flex; align-items: center;
      justify-content: space-between;
    }
    .badge { color: #fff; font-weight: 700; padding: 6px 14px; border-radius: 8px; }
    .mins { font-size: 1.8rem; font-weight: 800; }
    .mins span { font-size: 0.85rem; font-weight: 400; color: #94a3b8; margin-left: 3px; }
    .toward { color: #64748b; font-size: 0.85rem; margin: 18px 0 8px; }
    .empty { text-align: center; color: #64748b; padding: 30px 10px; line-height: 1.5; }
    .meta { text-align: center; color: #334155; font-size: 0.75rem; margin-top: 16px; }
  </style>
</head>
<body>
  <h1>Where are you headed?</h1>
  <div class="sub">Direct buses only · prototype</div>

  <label>From</label>
  <input id="origin" list="stops" placeholder="Your stop" autocomplete="off">
  <label>To</label>
  <input id="dest" list="stops" placeholder="Destination stop" autocomplete="off">
  <datalist id="stops"></datalist>

  <button class="go" onclick="findTrips()">Find buses</button>

  <div id="results"></div>
  <div class="meta" id="meta"></div>

<script>
  // populate stop pickers
  fetch('/api/stopnames').then(r => r.json()).then(names => {
    document.getElementById('stops').innerHTML =
      names.map(n => `<option value="${n.replace(/"/g,'&quot;')}">`).join('');
  });

  async function findTrips() {
    const origin = document.getElementById('origin').value.trim();
    const dest = document.getElementById('dest').value.trim();
    const results = document.getElementById('results');
    const meta = document.getElementById('meta');
    meta.textContent = '';
    if (!origin || !dest) { results.innerHTML = '<div class="empty">Pick a from and a to.</div>'; return; }
    results.innerHTML = '<div class="empty">Looking…</div>';

    try {
      const res = await fetch(`/api/trip?origin=${encodeURIComponent(origin)}&dest=${encodeURIComponent(dest)}`);
      const data = await res.json();
      if (data.error) { results.innerHTML = `<div class="empty">${data.error}</div>`; return; }
      if (!data.direct) {
        results.innerHTML = `<div class="empty">No single bus connects these two stops.<br>You'd need a transfer — out of scope for this prototype.</div>`;
        return;
      }
      if (!data.trips.length) {
        results.innerHTML = `<div class="empty">A direct route exists, but nothing's coming toward ${dest} right now.</div>`;
        return;
      }
      results.innerHTML = `<div class="toward">Next buses toward ${dest}</div>` + data.trips.map(t => {
        const m = Math.round(t.mins);
        return `<div class="trip-card">
          <div class="badge" style="background:${t.color}">Route ${t.route}</div>
          <div class="mins">${m}<span>min</span></div>
        </div>`;
      }).join('');
      meta.textContent = 'Updated ' + new Date((data.updated)*1000).toLocaleTimeString();
    } catch (e) {
      results.innerHTML = '<div class="empty">Something went wrong.</div>';
    }
  }
</script>
</body>
</html>
"""


# ── Explore prototype page (spatial route view) ───────────────────────────────

EXPLORE_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Explore routes</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    html, body { height: 100%; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: #0f172a; color: #f1f5f9;
      max-width: 480px; margin: 0 auto;
      display: flex; flex-direction: column; height: 100vh;
    }
    .pills {
      display: flex; gap: 8px; overflow-x: auto; padding: 14px 12px;
      flex-shrink: 0; -webkit-overflow-scrolling: touch;
    }
    .pills::-webkit-scrollbar { display: none; }
    .pill {
      flex-shrink: 0; border: 2px solid transparent; border-radius: 999px;
      padding: 8px 16px; font-size: 0.95rem; font-weight: 800;
      cursor: pointer; opacity: 0.55; transition: opacity .1s;
    }
    .pill.selected { opacity: 1; border-color: #fff; }
    #map { flex: 1; width: 100%; }
    .leaflet-container { background: #0f172a; font: inherit; }

    .dir-bar {
      display: none; align-items: center; justify-content: space-between;
      gap: 10px; padding: 10px 14px; flex-shrink: 0; background: #111c33;
    }
    .dir-bar.show { display: flex; }
    .dir-label { font-size: 0.85rem; color: #cbd5e1; font-weight: 600;
                 white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .dir-flip {
      flex-shrink: 0; background: #1e293b; color: #93c5fd; border: 1px solid #334155;
      border-radius: 10px; padding: 8px 12px; font-size: 0.8rem; font-weight: 700; cursor: pointer;
    }

    .carousel {
      display: none; gap: 10px; overflow-x: auto; flex-shrink: 0;
      padding: 12px calc(50% - 65px); scroll-snap-type: x mandatory;
      -webkit-overflow-scrolling: touch; background: #0b1424;
    }
    .carousel.show { display: flex; }
    .carousel::-webkit-scrollbar { display: none; }
    .stop-card {
      flex-shrink: 0; width: 130px; scroll-snap-align: center;
      background: #1e293b; border-radius: 14px; padding: 12px;
      border: 2px solid transparent; cursor: pointer;
    }
    .stop-card.active { border-color: #fff; background: #334155; }
    .stop-seq { font-size: 0.7rem; color: #64748b; font-weight: 700; }
    .stop-name { font-size: 0.85rem; margin-top: 4px; line-height: 1.25; }

    .hint { position: absolute; top: 50%; left: 0; right: 0; text-align: center;
            color: #475569; font-size: 0.9rem; pointer-events: none; }
    .lm-label {
      background: rgba(15,23,42,0.85); color: #e2e8f0; font-size: 0.7rem; font-weight: 600;
      padding: 2px 6px; border-radius: 6px; white-space: nowrap; border: 1px solid #334155;
    }
  </style>
</head>
<body>
  <div class="pills" id="pills"></div>
  <div style="position:relative; flex:1; display:flex;">
    <div id="map"></div>
    <div class="hint" id="hint">Tap a route above to highlight it</div>
  </div>
  <div class="dir-bar" id="dir-bar">
    <span class="dir-label" id="dir-label"></span>
    <button class="dir-flip" id="dir-flip" onclick="flipDirection()">⇄ Flip</button>
  </div>
  <div class="carousel" id="carousel"></div>

<script>
  const LANDMARKS = [
    { name: 'King St Metro',     lat: 38.8061, lon: -77.0611 },
    { name: 'Braddock Rd Metro', lat: 38.8140, lon: -77.0537 },
    { name: 'Market Square',     lat: 38.8049, lon: -77.0429 },
  ];

  let map, baseLayer, highlightLayer, stopLayer, userLatLng = null;
  let routes = [], routeColors = {};
  let selected = null;          // route object
  let directions = [], dirIdx = 0;
  let stopMarkers = [], activeStop = -1;

  function initMap() {
    map = L.map('map', { zoomControl: false, attributionControl: false })
           .setView([38.806, -77.055], 13);
    L.control.zoom({ position: 'bottomright' }).addTo(map);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', { maxZoom: 19 }).addTo(map);
    baseLayer = L.layerGroup().addTo(map);
    highlightLayer = L.layerGroup().addTo(map);
    stopLayer = L.layerGroup().addTo(map);

    // landmarks
    LANDMARKS.forEach(l => {
      L.marker([l.lat, l.lon], {
        icon: L.divIcon({ className: '', html: `<div class="lm-label">◆ ${l.name}</div>`,
                          iconSize: [10, 10], iconAnchor: [5, 5] })
      }).addTo(map);
    });

    // geolocation
    if (navigator.geolocation) {
      navigator.geolocation.getCurrentPosition(p => {
        userLatLng = [p.coords.latitude, p.coords.longitude];
        L.marker(userLatLng, {
          icon: L.divIcon({ className: '',
            html: '<div style="width:16px;height:16px;border-radius:50%;background:#3b82f6;border:3px solid #fff;box-shadow:0 0 0 4px rgba(59,130,246,0.3)"></div>',
            iconSize: [16, 16], iconAnchor: [8, 8] })
        }).addTo(map);
      }, () => {}, { enableHighAccuracy: true, timeout: 6000 });
    }
  }

  async function loadBase() {
    const shapes = await (await fetch('/api/allshapes')).json();
    shapes.forEach(s => {
      L.polyline(s.points, { color: s.color, weight: 2, opacity: 0.22 }).addTo(baseLayer);
    });
  }

  async function loadPills() {
    routes = await (await fetch('/api/routes')).json();
    const el = document.getElementById('pills');
    el.innerHTML = routes.map(r => {
      routeColors[r.id] = r.color;
      return `<button class="pill" data-id="${r.id}"
                style="background:${r.color};color:${r.text_color}"
                onclick='selectRoute(${JSON.stringify(r).replace(/'/g,"&#39;").replace(/"/g,"&quot;")})'>${r.short_name}</button>`;
    }).join('');
  }

  async function selectRoute(route) {
    selected = route;
    document.getElementById('hint').style.display = 'none';
    document.querySelectorAll('.pill').forEach(p =>
      p.classList.toggle('selected', p.dataset.id === route.id));

    directions = await (await fetch('/api/directions?route_id=' + route.id)).json();
    dirIdx = 0;
    document.getElementById('dir-bar').classList.add('show');
    drawDirection();
  }

  async function drawDirection() {
    if (!directions.length) return;
    const dir = directions[dirIdx];
    document.getElementById('dir-label').textContent = dir.label;
    document.getElementById('dir-flip').style.display = directions.length > 1 ? 'block' : 'none';

    const color = routeColors[selected.id] || '#3b82f6';
    highlightLayer.clearLayers();
    stopLayer.clearLayers();
    stopMarkers = []; activeStop = -1;

    // highlighted route line
    const pts = await (await fetch(`/api/shape?route_id=${selected.id}&headsign=${encodeURIComponent(dir.headsign)}`)).json();
    if (pts.length) {
      const line = L.polyline(pts, { color, weight: 5, opacity: 0.95 }).addTo(highlightLayer);
      map.fitBounds(line.getBounds(), { padding: [40, 40] });
    }

    // stops + carousel
    const stops = await (await fetch(`/api/stops?route_id=${selected.id}&headsign=${encodeURIComponent(dir.headsign)}`)).json();
    stops.forEach((s, i) => {
      if (s.lat == null) { stopMarkers.push(null); return; }
      const m = L.circleMarker([s.lat, s.lon],
        { radius: 5, color: '#fff', weight: 1.5, fillColor: color, fillOpacity: 1 }).addTo(stopLayer);
      m.on('click', () => focusStop(i, true));
      stopMarkers.push(m);
    });

    const car = document.getElementById('carousel');
    car.classList.add('show');
    car.innerHTML = stops.map((s, i) =>
      `<div class="stop-card" data-i="${i}" onclick="focusStop(${i}, true)">
         <div class="stop-seq">STOP ${i + 1}</div>
         <div class="stop-name">${s.name}</div>
       </div>`).join('');
    car.scrollLeft = 0;

    // nearest stop to the user → likely boarding point
    if (userLatLng) {
      let best = -1, bestD = Infinity;
      stops.forEach((s, i) => {
        if (s.lat == null) return;
        const d = (s.lat - userLatLng[0]) ** 2 + (s.lon - userLatLng[1]) ** 2;
        if (d < bestD) { bestD = d; best = i; }
      });
      if (best >= 0) focusStop(best, true);
    } else {
      focusStop(0, false);
    }
  }

  function focusStop(i, pan) {
    if (activeStop >= 0 && stopMarkers[activeStop])
      stopMarkers[activeStop].setStyle({ radius: 5, weight: 1.5 });
    activeStop = i;
    const m = stopMarkers[i];
    if (m) { m.setStyle({ radius: 9, weight: 3 }); if (pan) map.panTo(m.getLatLng()); }
    document.querySelectorAll('.stop-card').forEach(c =>
      c.classList.toggle('active', +c.dataset.i === i));
    const card = document.querySelector(`.stop-card[data-i="${i}"]`);
    if (card) card.scrollIntoView({ behavior: 'smooth', inline: 'center', block: 'nearest' });
  }

  function flipDirection() {
    dirIdx = (dirIdx + 1) % directions.length;
    drawDirection();
  }

  // carousel scroll → highlight centered stop on the map
  let scrollT = null;
  document.getElementById('carousel').addEventListener('scroll', e => {
    clearTimeout(scrollT);
    scrollT = setTimeout(() => {
      const step = 140; // card width + gap
      const i = Math.round(e.target.scrollLeft / step);
      if (i !== activeStop) {
        if (activeStop >= 0 && stopMarkers[activeStop]) stopMarkers[activeStop].setStyle({ radius: 5, weight: 1.5 });
        activeStop = i;
        const m = stopMarkers[i];
        if (m) { m.setStyle({ radius: 9, weight: 3 }); map.panTo(m.getLatLng()); }
        document.querySelectorAll('.stop-card').forEach(c => c.classList.toggle('active', +c.dataset.i === i));
      }
    }, 80);
  });

  initMap();
  loadBase();
  loadPills();
</script>
</body>
</html>
"""


# ── Frontend ─────────────────────────────────────────────────────────────────

HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>DASH Bus Tracker</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@3.0.0/dist/tabler-icons.min.css"/>
  <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800&display=swap"/>
  <style>
    :root {
      --cream: #FBF3E4; --cream-2: #F1E6CC; --surface: #FFFDF8;
      --border: #EADFC8; --border-2: #E7D9BE;
      --ink: #3B3026; --ink-2: #8A7B62; --ink-3: #B0A084;
      --brick: #C0492F; --brick-d: #93341F; --potomac: #2F6E8F; --gold: #C9962F;
      --green-bg: #DCEFE1; --green-tx: #1F5C3C;
      --amber-bg: #F7E4C6; --amber-tx: #7A4E12;
      --r: 16px;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Nunito', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: var(--cream);
      color: var(--ink);
      min-height: 100vh;
      padding: 0;
      max-width: 480px;
      margin: 0 auto;
    }

    .header { padding: 20px 16px 12px; display: flex; align-items: center; gap: 12px; }
    .back-btn {
      background: var(--surface); border: 1px solid var(--border); color: var(--ink-2);
      font-size: 1.2rem; width: 36px; height: 36px; border-radius: 10px; cursor: pointer;
      display: none; align-items: center; justify-content: center; flex-shrink: 0;
    }
    .back-btn.visible { display: flex; }
    .header-title { font-size: 1.2rem; font-weight: 800; }
    .header-sub { font-size: 0.8rem; color: var(--ink-2); margin-top: 2px; }

    .screen { display: none; padding: 0 16px 24px; }
    .screen.active { display: block; }

    /* Nearby + favorites */
    .nearby-btn {
      display: flex; align-items: center; justify-content: center; gap: 8px;
      width: 100%; margin-bottom: 16px; padding: 15px;
      background: var(--brick); color: #fff; border: none; border-radius: 14px;
      font-size: 1rem; font-weight: 800; cursor: pointer;
    }
    .nearby-btn:active { background: var(--brick-d); }
    .fav-row {
      background: var(--surface); border: 1px solid var(--border); border-radius: var(--r);
      padding: 14px 16px; margin-bottom: 10px; cursor: pointer; display: flex;
      align-items: center; gap: 10px;
    }
    .fav-row:active { background: var(--cream-2); }
    .fav-row .ti-star { color: var(--gold); font-size: 18px; }
    .fav-row .fav-name { flex: 1; font-weight: 700; }
    .nearby-item {
      background: var(--surface); border: 1px solid var(--border); border-radius: var(--r);
      padding: 14px 16px; margin-bottom: 10px; cursor: pointer;
    }
    .nearby-item:active { background: var(--cream-2); }
    .nearby-top { display: flex; align-items: center; justify-content: space-between; gap: 10px; }
    .nearby-name { font-weight: 700; }
    .nearby-dist { font-size: 0.78rem; color: var(--ink-2); white-space: nowrap; }
    .route-chips { display: flex; flex-wrap: wrap; gap: 5px; margin-top: 8px; }
    .route-chip { color: #fff; font-size: 0.72rem; font-weight: 800; padding: 2px 8px; border-radius: 7px; }
    .fav-toggle {
      display: inline-flex; align-items: center; gap: 6px; margin-bottom: 14px;
      padding: 8px 14px; background: var(--surface); color: var(--ink-2);
      border: 1px solid var(--border); border-radius: 999px;
      font-size: 0.85rem; font-weight: 700; cursor: pointer;
    }
    .fav-toggle.saved { color: var(--gold); border-color: var(--gold); }

    /* Route grid */
    .route-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
    .route-btn {
      border: none; border-radius: var(--r); padding: 16px; cursor: pointer;
      text-align: left; transition: opacity 0.1s;
    }
    .route-btn:active { opacity: 0.85; }
    .route-num { font-size: 1.4rem; font-weight: 800; }
    .route-name { font-size: 0.7rem; font-weight: 600; margin-top: 2px; opacity: 0.9; }

    /* Direction & stop lists */
    .list-item {
      background: var(--surface); border: 1px solid var(--border); border-radius: var(--r);
      padding: 16px; margin-bottom: 10px; cursor: pointer; display: flex;
      align-items: center; justify-content: space-between; font-size: 1rem;
    }
    .list-item:active { background: var(--cream-2); }
    .list-item .arrow { color: var(--ink-3); }
    .stop-seq { font-size: 0.75rem; color: var(--ink-3); margin-right: 12px; min-width: 20px; font-weight: 700; }

    /* Bus cards */
    .bus-card { background: var(--surface); border: 1px solid var(--border); border-radius: var(--r); padding: 16px 20px; margin-bottom: 10px; }
    .bus-card.alert-match { border-color: var(--brick); border-left: 4px solid var(--brick); }
    .bus-top { display: flex; align-items: center; justify-content: space-between; }
    .route-badge { color: #fff; font-size: 1rem; font-weight: 800; padding: 6px 14px; border-radius: 10px; }
    .mins { font-size: 2rem; font-weight: 800; }
    .mins span { font-size: 0.9rem; font-weight: 600; color: var(--ink-2); margin-left: 3px; }
    .mins.urgent { color: var(--brick); }
    .mins.soon   { color: var(--gold); }
    .mins.ok     { color: var(--green-tx); }
    .headsign { font-size: 0.8rem; color: var(--ink-2); margin-top: 6px; }

    /* Active alert banner */
    .alert-banner {
      background: #E8F0F4; border: 1px solid var(--potomac); border-radius: 12px;
      padding: 12px 16px; margin-bottom: 16px; display: none;
      align-items: center; justify-content: space-between; font-size: 0.85rem;
    }
    .alert-banner.active { display: flex; }
    .alert-banner-text { color: var(--potomac); }
    .alert-banner-text strong { color: var(--ink); display: block; font-size: 0.9rem; }
    .cancel-alert { background: none; border: none; color: var(--ink-2); font-size: 1.1rem; cursor: pointer; padding: 4px; }

    /* Modal overlay */
    .modal-overlay {
      display: none; position: fixed; inset: 0; background: rgba(59,48,38,0.45);
      z-index: 100; align-items: flex-end; justify-content: center;
    }
    .modal-overlay.open { display: flex; }
    .modal { background: var(--surface); border-radius: 24px 24px 0 0; padding: 24px 20px 40px; width: 100%; max-width: 480px; }
    .modal-title { font-size: 1.1rem; font-weight: 800; margin-bottom: 4px; }
    .modal-sub { font-size: 0.8rem; color: var(--ink-2); margin-bottom: 24px; }
    .modal-threshold-row { display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px; }
    .modal-threshold-label { font-size: 0.9rem; color: var(--ink-2); }
    .modal-threshold-val { font-size: 1rem; font-weight: 800; color: var(--brick); }
    input[type=range] { width: 100%; accent-color: var(--brick); margin-bottom: 24px; }
    .set-alert-btn {
      display: block; width: 100%; padding: 15px; background: var(--brick); color: #fff;
      border: none; border-radius: 14px; font-size: 1rem; font-weight: 800; cursor: pointer;
    }
    .set-alert-btn:active { background: var(--brick-d); }
    .cancel-modal-btn {
      display: block; width: 100%; padding: 13px; margin-top: 10px;
      background: var(--cream-2); color: var(--ink-2); border: none;
      border-radius: 14px; font-size: 0.9rem; cursor: pointer;
    }

    .section-label { font-size: 0.7rem; font-weight: 700; color: var(--ink-3); text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 10px; }
    .updated { text-align: center; color: var(--ink-3); font-size: 0.75rem; margin-top: 16px; }
    .empty { text-align: center; color: var(--ink-3); padding: 40px 0; }
    .refresh-btn {
      display: block; width: 100%; margin-top: 12px; padding: 13px;
      background: var(--surface); color: var(--ink-2); border: 1px solid var(--border);
      border-radius: 12px; font-size: 0.9rem; cursor: pointer;
    }
    .refresh-btn:active { background: var(--cream-2); }

    /* Map + stop carousel */
    #map {
      height: 56vh; width: 100%; border-radius: var(--r); overflow: hidden;
      background: var(--cream-2); transition: height 0.25s ease; border: 1px solid var(--border-2);
    }
    #map.shrunk { height: 34vh; }
    .leaflet-container { background: var(--cream-2); font: inherit; }
    .leaflet-marker-icon { transition: transform 0.9s linear; }
    .stop-carousel {
      display: flex; gap: 10px; overflow-x: auto; margin-top: 12px;
      padding-bottom: 4px; scroll-snap-type: x mandatory;
      -webkit-overflow-scrolling: touch;
      padding-left: calc(50% - 75px); padding-right: calc(50% - 75px);
    }
    .stop-carousel::-webkit-scrollbar { display: none; }
    .stop-card {
      flex-shrink: 0; width: 150px; scroll-snap-align: center;
      background: var(--surface); border-radius: var(--r); padding: 14px;
      border: 2px solid var(--border); cursor: pointer;
    }
    .stop-card:active { background: var(--cream-2); }
    .stop-card.active { border-color: var(--brick); }
    .stop-seq { font-size: 0.7rem; color: var(--ink-3); font-weight: 700; letter-spacing: 0.04em; }
    .stop-name { font-size: 0.9rem; margin-top: 4px; line-height: 1.25; font-weight: 700; }
    .bus-flag { display: none; align-items: center; gap: 4px; margin-top: 8px;
                font-size: 0.68rem; font-weight: 700; padding: 3px 8px; border-radius: 8px; }
    .bus-flag.here { display: inline-flex; background: var(--green-bg); color: var(--green-tx); }
    .bus-flag.near { display: inline-flex; background: var(--amber-bg); color: var(--amber-tx); }
    .stop-card.has-bus { border-color: #8FCBAA; }
    .list-item.has-bus { border-color: #8FCBAA; }
    .list-item .bus-flag { margin-right: 8px; }
    /* map / list toggle */
    .stop-view-toggle { display: flex; justify-content: flex-end; margin-bottom: 10px; }
    .stop-view-toggle button {
      background: var(--surface); color: var(--potomac); border: 1px solid var(--border);
      border-radius: 10px; padding: 7px 12px; font-size: 0.8rem; font-weight: 700; cursor: pointer;
    }
    .stop-list-v { display: none; }
    #screen-stops.list-mode #map,
    #screen-stops.list-mode #stop-carousel,
    #screen-stops.list-mode .carousel-hint { display: none; }
    #screen-stops.list-mode .stop-list-v { display: block; }
    .carousel-hint { text-align: center; color: var(--ink-3); font-size: 0.75rem; margin-top: 10px; }
    .bus-dot {
      width: 28px; height: 28px; border-radius: 50%;
      border: 2.5px solid #fff; display: flex; align-items: center;
      justify-content: center; font-size: 15px; color: #fff;
    }
    .stop-detail { display: none; margin-top: 14px; }
    .stop-detail.show { display: block; }
    .detail-head { font-size: 0.95rem; font-weight: 800; margin-bottom: 10px; }
    .detail-head span { color: var(--ink-2); font-weight: 600; font-size: 0.8rem; }
    .detail-empty { color: var(--ink-2); text-align: center; padding: 18px; font-size: 0.9rem; }
    .more-routes {
      display: none; width: 100%; margin-top: 6px; padding: 13px;
      background: var(--surface); color: var(--potomac); border: 1px solid var(--border);
      border-radius: 12px; font-size: 0.9rem; font-weight: 700; cursor: pointer;
    }
    .more-routes:active { background: var(--cream-2); }
    .detail-alert {
      display: none; align-items: center; justify-content: space-between;
      background: #E8F0F4; border: 1px solid var(--potomac); border-radius: 12px;
      padding: 10px 14px; margin-bottom: 12px; font-size: 0.85rem; color: var(--potomac);
    }
    .detail-alert button {
      background: none; border: none; color: var(--potomac); font-weight: 700;
      cursor: pointer; font-size: 0.8rem;
    }
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
  <button class="nearby-btn" onclick="openNearby()"><i class="ti ti-current-location" aria-hidden="true"></i> Stops near me</button>
  <div id="fav-section"></div>
  <div class="section-label">Select a route</div>
  <div class="route-grid" id="route-grid"></div>
</div>

<!-- Screen: Nearby stops -->
<div class="screen" id="screen-nearby">
  <div class="section-label">Stops near you</div>
  <div id="nearby-list"></div>
</div>

<!-- Screen 2: Directions -->
<div class="screen" id="screen-directions">
  <div class="section-label">Select direction</div>
  <div id="direction-list"></div>
</div>

<!-- Screen 3: Stops (map + linked carousel) -->
<div class="screen" id="screen-stops">
  <div class="stop-view-toggle">
    <button id="toggle-view" onclick="toggleStopView()">Collapse map ▾</button>
  </div>
  <div id="map"></div>
  <div class="stop-carousel" id="stop-carousel"></div>
  <div class="carousel-hint">Swipe the stops · tap one for arrivals</div>
  <div class="stop-list-v" id="stop-list-v"></div>
  <div class="stop-detail" id="stop-detail">
    <div class="detail-alert" id="detail-alert">
      <span id="detail-alert-text"></span>
      <button onclick="cancelAlert()">Cancel</button>
    </div>
    <div class="detail-head" id="detail-head"></div>
    <div id="detail-arrivals"></div>
    <button class="more-routes" id="more-routes-btn" onclick="openFullArrivals()"></button>
  </div>
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
  <button class="fav-toggle" id="fav-toggle" onclick="toggleCurrentFav()"></button>
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
  let vehicleTimer = null;
  let screenStack = ['routes'];

  // Map objects
  let map = null;
  let routeLine = null;
  let stopLayer = null;
  let busLayer = null;

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
    if (vehicleTimer) { clearInterval(vehicleTimer); vehicleTimer = null; }
  }

  function updateHeader() {
    const cur = screenStack[screenStack.length - 1];
    const titles = {
      routes: ['DASH Bus Tracker', 'Alexandria, VA'],
      directions: [state.route ? `Route ${state.route.short_name}` : 'Route', state.route ? state.route.long_name : ''],
      stops: [state.headsign || 'Direction', 'Select your stop'],
      arrivals: [state.stop ? state.stop.name : 'Stop', state.headsign || ''],
      map: [state.route ? `Route ${state.route.short_name}` : 'Map', state.headsign || ''],
      nearby: ['Stops near me', 'Closest first'],
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
    await renderStopMap();
  }

  // ── Screen 3: Stops ─────────────────────────────────────────────────────

  function selectStop(stop) {
    if (vehicleTimer) { clearInterval(vehicleTimer); vehicleTimer = null; }
    state.stop = stop;
    showScreen('arrivals');
    updateHeader();
    updateFavToggle();
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
    container.innerHTML = buses.map(b => {
      const color = routeColors[b.route] || '#888888';
      const m = Math.round(b.mins);
      const cls = minsClass(b.mins);
      const isAlerted = activeAlert && activeAlert.trip_id === b.trip_id;
      return `<div class="bus-card ${isAlerted ? 'alert-match' : ''}" onclick="openAlertModal(${JSON.stringify(b).replace(/"/g,'&quot;')})">
        <div class="bus-top">
          <div class="route-badge" style="background:${color}">Route ${b.route}</div>
          <div class="mins ${cls}" data-arrival="${Date.now()/1000 + b.mins*60}"><span class="mins-n">${m}</span><span>min</span></div>
        </div>
        ${b.headsign ? `<div class="headsign">toward ${b.headsign}</div>` : ''}
        ${isAlerted ? `<div class="headsign" style="color:var(--potomac);margin-top:4px"><i class="ti ti-bell"></i> Alert set — ${activeAlert.threshold} min</div>` : ''}
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
    if (detailOpen()) renderDetailArrivals();   // instant 🔔 on the stop screen
  }

  async function cancelAlert() {
    activeAlert = null;
    await fetch('/api/alert', { method: 'DELETE' });
    document.getElementById('alert-banner').classList.remove('active');
    renderBuses(lastBuses);
    if (detailOpen()) renderDetailArrivals();
  }

  // ── Screen 3: Stops — map + linked carousel ──────────────────────────────

  let stopMarkers = [], activeStopIdx = -1, currentStops = [];

  async function renderStopMap() {
    const color = state.route ? routeColors[state.route.id] : '#3b82f6';

    if (vehicleTimer) { clearInterval(vehicleTimer); vehicleTimer = null; }
    // reset any open stop detail from a previous route/direction
    document.getElementById('stop-detail').classList.remove('show');
    document.getElementById('map').classList.remove('shrunk');
    if (!map) {
      map = L.map('map', { zoomControl: false, attributionControl: false })
             .setView([38.8048, -77.0469], 13);  // Alexandria, VA
      L.control.zoom({ position: 'bottomright' }).addTo(map);
      L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
        maxZoom: 19,
      }).addTo(map);
      stopLayer = L.layerGroup().addTo(map);
      busLayer = L.layerGroup().addTo(map);   // drawn above stops
    }
    // recalc size once the screen is visible
    setTimeout(() => map.invalidateSize(), 60);

    // highlighted route line
    if (routeLine) { map.removeLayer(routeLine); routeLine = null; }
    try {
      const pts = await (await fetch(`/api/shape?route_id=${state.route.id}&headsign=${encodeURIComponent(state.headsign)}`)).json();
      if (pts.length) {
        routeLine = L.polyline(pts, { color, weight: 5, opacity: 0.85 }).addTo(map);
        map.fitBounds(routeLine.getBounds(), { padding: [30, 30] });
      }
    } catch (e) { /* leave default view */ }

    // stops -> map dots + carousel cards
    const stops = await (await fetch(`/api/stops?route_id=${state.route.id}&headsign=${encodeURIComponent(state.headsign)}`)).json();
    currentStops = stops;
    stopLayer.clearLayers();
    if (busLayer) busLayer.clearLayers();
    busMarkers = {};
    stopMarkers = []; activeStopIdx = -1;
    stops.forEach((s) => {
      if (s.lat == null) { stopMarkers.push(null); return; }
      const m = L.circleMarker([s.lat, s.lon],
        { radius: 5, color: '#fff', weight: 1.5, fillColor: color, fillOpacity: 1 }).addTo(stopLayer);
      m.on('click', () => openStopDetail(s));
      stopMarkers.push(m);
    });

    const esc = (s) => JSON.stringify(s).replace(/"/g, '&quot;');
    const car = document.getElementById('stop-carousel');
    car.innerHTML = stops.map((s, i) =>
      `<div class="stop-card" data-i="${i}" data-stop="${s.id}" onclick="openStopDetail(${esc(s)})">
         <div class="stop-seq">STOP ${i + 1}</div>
         <div class="stop-name">${s.name}</div>
         <div class="bus-flag"></div>
       </div>`).join('');
    car.scrollLeft = 0;

    // vertical list (production-style), shown when the map is collapsed
    document.getElementById('stop-list-v').innerHTML = stops.map((s, i) =>
      `<div class="list-item" data-stop="${s.id}" onclick="openStopDetail(${esc(s)})">
         <span class="stop-seq">${i + 1}</span>
         <span style="flex:1">${s.name}</span>
         <span class="bus-flag"></span>
         <span class="arrow">›</span>
       </div>`).join('');

    highlightStopIdx(0, false);

    // live buses on this route
    loadStopVehicles();
    vehicleTimer = setInterval(loadStopVehicles, 15000);
  }

  let busMarkers = {};  // trip_id -> marker (reused so positions glide, not teleport)

  async function loadStopVehicles() {
    if (!state.route || !busLayer) return;
    const color = routeColors[state.route.id] || '#3b82f6';
    try {
      const data = await (await fetch('/api/vehicles?route_id=' + state.route.id)).json();
      const vs = data.vehicles || [];
      const seen = new Set();
      vs.forEach(v => {
        seen.add(v.trip_id);
        let m = busMarkers[v.trip_id];
        if (m) {
          m.setLatLng([v.lat, v.lon]);   // CSS transition animates the move
        } else {
          const icon = L.divIcon({
            className: '',
            html: `<div class="bus-dot" style="background:${color}"><i class="ti ti-bus"></i></div>`,
            iconSize: [28, 28], iconAnchor: [14, 14],
          });
          m = L.marker([v.lat, v.lon], { icon, zIndexOffset: 1000 }).addTo(busLayer);
          busMarkers[v.trip_id] = m;
        }
        m.bindPopup(`<b>Route ${v.route}</b>${v.headsign ? '<br>toward ' + v.headsign : ''}`);
      });
      Object.keys(busMarkers).forEach(tid => {
        if (!seen.has(tid)) { busLayer.removeLayer(busMarkers[tid]); delete busMarkers[tid]; }
      });
      applyBusIndicators(vs);
      // keep the inline arrivals fresh while a stop is open
      if (state.stop && document.getElementById('stop-detail').classList.contains('show')) {
        loadDetailArrivals(state.stop);
      }
    } catch (e) { /* keep last positions */ }
  }

  // mark which stops (carousel + list) have a bus at / approaching them
  function applyBusIndicators(vehicles) {
    document.querySelectorAll('#screen-stops [data-stop]').forEach(el => {
      el.classList.remove('has-bus');
      const f = el.querySelector('.bus-flag');
      if (f) { f.className = 'bus-flag'; f.textContent = ''; }
    });
    (vehicles || []).forEach(v => {
      if (!v.stop_id) return;
      document.querySelectorAll(`#screen-stops [data-stop="${v.stop_id}"]`).forEach(el => {
        const f = el.querySelector('.bus-flag');
        if (!f) return;
        if (v.status === 'STOPPED_AT') {
          f.className = 'bus-flag here'; f.innerHTML = '<i class="ti ti-bus"></i> Bus here';
          el.classList.add('has-bus');
        } else if (!f.classList.contains('here')) {
          f.className = 'bus-flag near'; f.innerHTML = '<i class="ti ti-bus"></i> Approaching';
        }
      });
    });
  }

  function toggleStopView() {
    const scr = document.getElementById('screen-stops');
    const listMode = scr.classList.toggle('list-mode');
    document.getElementById('toggle-view').textContent = listMode ? 'Show map ▴' : 'Collapse map ▾';
    if (!listMode && map) setTimeout(() => map.invalidateSize(), 60);
  }

  // ── Inline stop detail (map stays, shrinks) ──────────────────────────────

  function openStopDetail(stop) {
    state.stop = stop;
    const i = currentStops.findIndex(s => s.id === stop.id);
    if (i >= 0) highlightStopIdx(i, true);

    document.getElementById('map').classList.add('shrunk');
    setTimeout(() => { if (map) map.invalidateSize(); }, 280);

    document.getElementById('stop-detail').classList.add('show');
    document.getElementById('detail-head').innerHTML =
      `${stop.name} <span>· Route ${state.route.id}</span>`;
    loadDetailArrivals(stop);
  }

  let detailBuses = [];

  async function loadDetailArrivals(stop) {
    try {
      const data = await (await fetch('/api/buses?stop_id=' + stop.id)).json();
      detailBuses = data.buses || [];
    } catch (e) {
      document.getElementById('detail-arrivals').innerHTML = '<div class="detail-empty">Error loading arrivals.</div>';
      document.getElementById('more-routes-btn').style.display = 'none';
      return;
    }
    renderDetailArrivals();
  }

  // synchronous render from cache so alert changes show instantly
  function renderDetailArrivals() {
    const wrap = document.getElementById('detail-arrivals');
    const moreBtn = document.getElementById('more-routes-btn');
    const color = routeColors[state.route.id] || '#3b82f6';

    // active-alert banner (lets you cancel without leaving the stop screen)
    const aBanner = document.getElementById('detail-alert');
    if (activeAlert) {
      document.getElementById('detail-alert-text').innerHTML =
        `<i class="ti ti-bell"></i> Route ${activeAlert.route} alert · ${activeAlert.threshold} min`;
      aBanner.style.display = 'flex';
    } else {
      aBanner.style.display = 'none';
    }
    const mine = detailBuses.filter(b => b.route === state.route.id);
    const others = detailBuses.filter(b => b.route !== state.route.id);

    if (!mine.length) {
      wrap.innerHTML = `<div class="detail-empty">No Route ${state.route.id} buses inbound right now.</div>`;
    } else {
      wrap.innerHTML = mine.map(b => {
        const m = Math.round(b.mins);
        const cls = minsClass(b.mins);
        const isAlerted = activeAlert && activeAlert.trip_id === b.trip_id;
        return `<div class="bus-card ${isAlerted ? 'alert-match' : ''}" onclick="openAlertModal(${JSON.stringify(b).replace(/"/g,'&quot;')})">
          <div class="bus-top">
            <div class="route-badge" style="background:${color}">Route ${b.route}</div>
            <div class="mins ${cls}" data-arrival="${Date.now()/1000 + b.mins*60}"><span class="mins-n">${m}</span><span>min</span></div>
          </div>
          ${b.headsign ? `<div class="headsign">toward ${b.headsign}</div>` : ''}
          ${isAlerted ? `<div class="headsign" style="color:var(--potomac);margin-top:4px"><i class="ti ti-bell"></i> Alert set — ${activeAlert.threshold} min</div>` : ''}
        </div>`;
      }).join('');
    }

    const otherRoutes = [...new Set(others.map(b => b.route))];
    if (otherRoutes.length) {
      moreBtn.style.display = 'block';
      moreBtn.textContent = `Show ${otherRoutes.length} other route${otherRoutes.length > 1 ? 's' : ''} at this stop →`;
    } else {
      moreBtn.style.display = 'none';
    }
  }

  function detailOpen() {
    return document.getElementById('stop-detail').classList.contains('show');
  }

  function openFullArrivals() {
    selectStop(state.stop);  // existing full all-routes arrivals screen
  }

  function highlightStopIdx(i, pan) {
    if (activeStopIdx >= 0 && stopMarkers[activeStopIdx])
      stopMarkers[activeStopIdx].setStyle({ radius: 5, weight: 1.5 });
    activeStopIdx = i;
    const m = stopMarkers[i];
    if (m) { m.setStyle({ radius: 9, weight: 3 }); if (pan) map.panTo(m.getLatLng()); }
    document.querySelectorAll('#stop-carousel .stop-card').forEach(c =>
      c.classList.toggle('active', +c.dataset.i === i));
  }

  // carousel scroll → highlight the centered stop on the map
  (function () {
    const car = document.getElementById('stop-carousel');
    let t = null;
    car.addEventListener('scroll', () => {
      clearTimeout(t);
      t = setTimeout(() => {
        const step = 160; // card width + gap
        const i = Math.round(car.scrollLeft / step);
        if (i !== activeStopIdx && i >= 0 && i < currentStops.length) highlightStopIdx(i, true);
      }, 80);
    });
  })();

  // ── Stops near me + favorites ────────────────────────────────────────────

  function openNearby() {
    showScreen('nearby');
    updateHeader();
    const list = document.getElementById('nearby-list');
    if (!navigator.geolocation) {
      list.innerHTML = '<div class="empty">Location isn\\'t available on this device.</div>';
      return;
    }
    list.innerHTML = '<div class="empty">Finding stops near you…</div>';
    navigator.geolocation.getCurrentPosition(async pos => {
      try {
        const data = await (await fetch(`/api/nearby?lat=${pos.coords.latitude}&lon=${pos.coords.longitude}`)).json();
        renderNearby(data.stops || []);
      } catch (e) {
        list.innerHTML = '<div class="empty">Couldn\\'t load nearby stops.</div>';
      }
    }, () => {
      list.innerHTML = '<div class="empty">Location blocked. On a phone this needs the secure (https) site.</div>';
    }, { enableHighAccuracy: true, timeout: 8000 });
  }

  function fmtDist(m) {
    if (m < 1000) return (Math.round(m / 0.3048 / 10) * 10) + ' ft';
    return (m / 1609).toFixed(1) + ' mi';
  }

  function renderNearby(stops) {
    const list = document.getElementById('nearby-list');
    if (!stops.length) { list.innerHTML = '<div class="empty">No stops found nearby.</div>'; return; }
    list.innerHTML = stops.map(s => {
      const chips = s.routes.map(r => `<span class="route-chip" style="background:${r.color}">${r.id}</span>`).join('');
      return `<div class="nearby-item" onclick="openStopFromList(${JSON.stringify({id:s.id,name:s.name}).replace(/"/g,'&quot;')})">
        <div class="nearby-top">
          <span class="nearby-name">${s.name}</span>
          <span class="nearby-dist"><i class="ti ti-walk"></i> ${fmtDist(s.meters)}</span>
        </div>
        <div class="route-chips">${chips}</div>
      </div>`;
    }).join('');
  }

  function openStopFromList(stop) {
    state.headsign = '';
    selectStop(stop);
  }

  function getFavs() {
    try { return JSON.parse(localStorage.getItem('dash_favs') || '[]'); } catch (e) { return []; }
  }
  function setFavs(f) { localStorage.setItem('dash_favs', JSON.stringify(f)); }
  function isFav(id) { return getFavs().some(f => f.id === id); }

  function toggleCurrentFav() {
    if (!state.stop) return;
    let favs = getFavs();
    favs = isFav(state.stop.id) ? favs.filter(f => f.id !== state.stop.id)
                                : favs.concat([{ id: state.stop.id, name: state.stop.name }]);
    setFavs(favs);
    updateFavToggle();
    renderFavs();
  }

  function updateFavToggle() {
    const btn = document.getElementById('fav-toggle');
    if (!btn || !state.stop) return;
    const saved = isFav(state.stop.id);
    btn.className = 'fav-toggle' + (saved ? ' saved' : '');
    btn.innerHTML = `<i class="ti ti-star"></i> ${saved ? 'Saved stop' : 'Save this stop'}`;
  }

  function renderFavs() {
    const sec = document.getElementById('fav-section');
    const favs = getFavs();
    if (!favs.length) { sec.innerHTML = ''; return; }
    sec.innerHTML = '<div class="section-label">Favorites</div>' + favs.map(f =>
      `<div class="fav-row" onclick="openStopFromList(${JSON.stringify(f).replace(/"/g,'&quot;')})">
        <i class="ti ti-star" aria-hidden="true"></i><span class="fav-name">${f.name}</span>
        <i class="ti ti-chevron-right" style="color:var(--ink-3)" aria-hidden="true"></i>
      </div>`).join('');
  }

  // live-ticking countdown between data refreshes
  function tickCountdowns() {
    const now = Date.now() / 1000;
    document.querySelectorAll('.mins[data-arrival]').forEach(el => {
      const rem = Math.max(0, (parseFloat(el.dataset.arrival) - now) / 60);
      const n = el.querySelector('.mins-n');
      if (n) n.textContent = Math.round(rem);
      el.className = 'mins ' + minsClass(rem);
    });
  }
  setInterval(tickCountdowns, 1000);

  // ── Init ─────────────────────────────────────────────────────────────────
  loadRoutes();
  renderFavs();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    load_gtfs()
    t = threading.Thread(target=alert_worker, daemon=True)
    t.start()
    # Railway sets PORT; use its presence only to gate the debugger/reloader.
    # The Railway service routes to port 5001 (its configured target), so we
    # always bind 5001 — that's the known-working production port.
    on_railway = "PORT" in os.environ
    app.run(
        host="0.0.0.0",
        port=5001,
        debug=not on_railway,
        use_reloader=not on_railway,
    )
