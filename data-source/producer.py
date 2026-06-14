
"""
Data Source Producer — adsb.lol
----------------------------------
Polls the adsb.lol API (free, no API key required).
Each aircraft object in the "ac" array is flattened into a JSON event.
Events are POSTed to Flume's HTTP source.

Endpoints supported (set via ADSBLOL_MODE):
  point  -> /v2/point/{lat}/{lon}/{radius}   (radius up to 250nm) [default]
  mil    -> /v2/mil      (all military-registered aircraft, global)
  ladd   -> /v2/ladd     (aircraft on the LADD privacy filter)
  pia    -> /v2/pia      (aircraft with PIA / privacy ICAO addresses)

adsb.lol aircraft object fields used (subset of the full schema):
  hex          ICAO24 transponder address (hex string)
  flight       callsign, often padded with spaces
  r            registration (tail number)
  t            ICAO aircraft type designator (e.g. "A320")
  category     ADS-B emitter category (e.g. "A3")
  lat, lon     position
  alt_baro     barometric altitude, ft. Can be the STRING "ground" instead
               of a number when the aircraft is on the ground.
  alt_geom     geometric altitude, ft
  gs           ground speed, knots
  track        true track, degrees
  baro_rate    vertical rate (barometric), ft/min
  geom_rate    vertical rate (geometric), ft/min
  squawk       transponder squawk code
  emergency    emergency status string
  spi          special position indicator (0/1)
  messages     total messages received from this aircraft
  seen         seconds since last message
  seen_pos     seconds since last position update
  rssi         signal strength

Flume's JSONHandler expects a list of objects, each with:
  { "headers": {...}, "body": "<string>" }
"""

import os
import json
import time
import logging
import requests
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [data-source] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
FLUME_HTTP_URL  = os.getenv("FLUME_HTTP_URL", "http://flume-producer:44444")
POLL_INTERVAL   = int(os.getenv("POLL_INTERVAL_SECONDS", "10"))
MAX_AIRCRAFT    = int(os.getenv("MAX_AIRCRAFT_PER_BATCH", "300"))

ADSBLOL_BASE = "https://api.adsb.lol/v2"
ADSBLOL_MODE = os.getenv("ADSBLOL_MODE", "point").lower()  # point | mil | ladd | pia

# Used only when ADSBLOL_MODE == "point"
ADSBLOL_LAT    = os.getenv("ADSBLOL_LAT", "30.0444")    # default: Cairo
ADSBLOL_LON    = os.getenv("ADSBLOL_LON", "31.2357")
ADSBLOL_RADIUS = os.getenv("ADSBLOL_RADIUS", "250")     # nautical miles, max 250


def build_url() -> str:
    if ADSBLOL_MODE == "point":
        return f"{ADSBLOL_BASE}/point/{ADSBLOL_LAT}/{ADSBLOL_LON}/{ADSBLOL_RADIUS}"
    if ADSBLOL_MODE in ("mil", "ladd", "pia"):
        return f"{ADSBLOL_BASE}/{ADSBLOL_MODE}"
    log.warning(f"Unknown ADSBLOL_MODE={ADSBLOL_MODE!r}, falling back to 'point'")
    return f"{ADSBLOL_BASE}/point/{ADSBLOL_LAT}/{ADSBLOL_LON}/{ADSBLOL_RADIUS}"


# ── Fetch aircraft list ───────────────────────────────────────
def fetch_aircraft() -> list[dict]:
    url = build_url()
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("ac") or []
    except requests.exceptions.HTTPError as e:
        log.warning(f"adsb.lol request failed ({url}): {e}")
        return []
    except Exception as e:
        log.warning(f"Failed to fetch from adsb.lol ({url}): {e}")
        return []


# ── Transform ─────────────────────────────────────────────────
def _num(value):
    """Return a float for numeric fields, or None if missing/non-numeric
    (e.g. alt_baro can be the string 'ground')."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def transform_aircraft(ac: dict) -> dict:
    """Flatten an adsb.lol aircraft object into a flat JSON event."""

    flight = ac.get("flight")
    flight = flight.strip() if isinstance(flight, str) else None

    alt_baro_raw = ac.get("alt_baro")
    on_ground = (alt_baro_raw == "ground")
    alt_baro = 0.0 if on_ground else _num(alt_baro_raw)

    return {
        "hex":            ac.get("hex"),
        "flight":         flight,
        "registration":   ac.get("r"),
        "aircraft_type":  ac.get("t"),
        "category":       ac.get("category"),
        "latitude":       _num(ac.get("lat")),
        "longitude":      _num(ac.get("lon")),
        "alt_baro":       alt_baro,
        "alt_geom":       _num(ac.get("alt_geom")),
        "ground_speed":   _num(ac.get("gs")),
        "track":          _num(ac.get("track")),
        "baro_rate":      _num(ac.get("baro_rate")),
        "geom_rate":      _num(ac.get("geom_rate")),
        "squawk":         ac.get("squawk"),
        "emergency":      ac.get("emergency"),
        "on_ground":      on_ground,
        "spi":            bool(ac.get("spi", 0)),
        "messages":       ac.get("messages"),
        "seen":           _num(ac.get("seen")),
        "seen_pos":       _num(ac.get("seen_pos")),
        "rssi":           _num(ac.get("rssi")),
        "ingested_at":    datetime.now(timezone.utc).isoformat(),
    }


# ── Send to Flume ────────────────────────────────────────────
def send_to_flume(events: list[dict]) -> bool:
    """POST events to Flume HTTP source (JSONHandler format)."""
    flume_payload = [
        {
            "headers": {
                "source": "adsblol",
                "content-type": "application/json"
            },
            "body": json.dumps(event)
        }
        for event in events
    ]

    try:
        resp = requests.post(FLUME_HTTP_URL, json=flume_payload, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        log.warning(f"Failed to send to Flume: {e}")
        return False


# ── Main loop ────────────────────────────────────────────────
def main():
    log.info(f"Starting adsb.lol data source. Flume URL: {FLUME_HTTP_URL}")
    log.info(f"Mode: {ADSBLOL_MODE} | Poll interval: {POLL_INTERVAL}s | Max aircraft/batch: {MAX_AIRCRAFT}")
    if ADSBLOL_MODE == "point":
        log.info(f"Center: ({ADSBLOL_LAT}, {ADSBLOL_LON}), radius: {ADSBLOL_RADIUS}nm")
    log.info(f"Fetch URL: {build_url()}")

    # Give Flume time to start up
    time.sleep(15)

    total_sent = 0
    while True:
        start = time.time()
        aircraft = fetch_aircraft()

        if aircraft:
            # Keep only aircraft with a known position
            aircraft = [a for a in aircraft if a.get("lat") is not None and a.get("lon") is not None]

            if len(aircraft) > MAX_AIRCRAFT:
                aircraft = aircraft[:MAX_AIRCRAFT]

            events = [transform_aircraft(a) for a in aircraft]

            if events:
                success = send_to_flume(events)
                if success:
                    total_sent += len(events)
                    types = {e["aircraft_type"] for e in events if e["aircraft_type"]}
                    log.info(
                        f"Sent {len(events)} aircraft "
                        f"(total: {total_sent}, distinct types: {len(types)})"
                    )
        else:
            log.info("No aircraft returned this poll")

        elapsed = time.time() - start
        sleep_time = max(0, POLL_INTERVAL - elapsed)
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
