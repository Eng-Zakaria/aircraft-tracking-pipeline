# Big Data Pipeline — adsb.lol Aircraft States (Docker)

A complete end-to-end streaming pipeline ingesting **live aircraft state
vectors** from the [adsb.lol](https://api.adsb.lol/docs) API — a free, open
source ADS-B data API published under the
[Open Data Commons Open Database License (ODbL) v1.0](https://opendatacommons.org/licenses/odbl/1-0/).


### Tech Stack

<p align="center">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" height="40" alt="python logo"  />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/apachekafka/apachekafka-original.svg" height="40" alt="kafka logo"  />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/apachespark/apachespark-original.svg" height="40" alt="spark logo"  />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/hadoop/hadoop-original.svg" height="40" alt="hadoop logo"  />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/influxdb/influxdb-original.svg" height="40" alt="influxdb logo"  />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/grafana/grafana-original.svg" height="40" alt="grafana logo"  />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/docker/docker-original.svg" height="40" alt="docker logo"  />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/linux/linux-original.svg" height="40" alt="docker logo"  />
</p>


![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Hadoop](https://img.shields.io/badge/Apache%20Hadoop-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=black)
![InfluxDB](https://img.shields.io/badge/InfluxDB-22ADF6?style=for-the-badge&logo=InfluxDB&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
![Linux](https://img.shields.io/badge/Linux-FCC624?style=for-the-badge&logo=linux&logoColor=black)

```
adsb.lol API  (/v2/point, /v2/mil, /v2/ladd, /v2/pia)
      │
      ▼
 data-source (Python)
      │  HTTP POST
      ▼
 Flume Producer  (HTTP Source → Kafka Sink)
      │
      ▼
   Kafka  (topic: aircraft-states)
      │
      ├─────────────────────────────────────┐
      │                                     │
      ▼                                     ▼
 Flume Consumer                    Spark Structured Streaming
 (Kafka Source → HDFS Sink)        (Kafka Source → InfluxDB)
      │                                     │
      ▼                                     ▼
   HDFS                                 InfluxDB v2
 (partitioned JSON)                         │
                                            ▼
                                         Grafana
                                  (live map + dashboards)
```

---

## Prerequisites

- Docker Engine ≥ 24
- Docker Compose v2
- 8 GB RAM recommended (Hadoop + Spark are heavy)

---

## About the adsb.lol API

[adsb.lol](https://api.adsb.lol/docs) is a community-run, free and open
ADS-B aggregation API (OpenAPI 0.0.2). No API key is currently required.

**Terms of service (summary):**
- Free to use today; an API key may be required in the future, obtainable by
  [feeding data to adsb.lol](https://adsb.lol).
- If you plan to use it for a production application, the project asks that
  you get in touch first so they don't break your integration by accident.
- All data is published under **ODbL** — the same license used by
  OpenStreetMap. If you redistribute or publish derived data/dashboards,
  attribute adsb.lol accordingly.

### Endpoints used by this pipeline

The data source (`data-source/producer.py`) supports four of the API's `v2`
endpoints, selected via `ADSBLOL_MODE`:

| `ADSBLOL_MODE` | Endpoint | Description |
|---|---|---|
| `point` (default) | `/v2/point/{lat}/{lon}/{radius}` | All aircraft within `radius` nautical miles (max 250) of a coordinate |
| `mil` | `/v2/mil` | All military-registered aircraft, globally |
| `ladd` | `/v2/ladd` | Aircraft on the LADD (Limiting Aircraft Data Displayed) list |
| `pia` | `/v2/pia` | Aircraft using PIA (Privacy ICAO Address) |

### Other endpoints available (not currently used)

The adsb.lol API exposes several more endpoints that could be wired in for
future extensions of this pipeline:

- `/v2/sqk/{squawk}` or `/v2/squawk/{squawk}` — aircraft on a specific squawk
  (e.g. `7700` for emergencies)
- `/v2/type/{aircraft_type}` — aircraft of a specific type (e.g. `A320`)
- `/v2/reg/{registration}` or `/v2/registration/{registration}` — aircraft by
  tail number (e.g. `G-KELS`)
- `/v2/icao/{icao_hex}` or `/v2/hex/{icao_hex}` — aircraft by transponder hex
  (e.g. `4CA87C`)
- `/v2/callsign/{callsign}` — aircraft by callsign (e.g. `JBU1942`)
- `/v2/closest/{lat}/{lon}/{radius}` — single closest aircraft to a point
- `/api/0/airport/{icao}`, `/api/0/routeset`, `/0/me`, `/0/my` — airport
  lookup, route info, and feeder/receiver stats

---

## Project Structure

```
aircraft-tracking-pipeline/
├── docker-compose.yml
├── data-source/
│   ├── Dockerfile
│   └── producer.py          # polls adsb.lol, flattens aircraft state vectors
├── flume-producer/
│   ├── Dockerfile
│   └── flume-producer.conf   # HTTP Source → Kafka Sink
├── flume-consumer/
│   ├── Dockerfile
│   ├── flume-consumer.conf   # Kafka Source → HDFS Sink
│   └── core-site.xml
├── spark/
│   ├── Dockerfile
│   └── streaming_job.py      # Kafka → InfluxDB (raw + windowed agg)
└── grafana/
    └── provisioning/
        ├── datasources/influxdb.yml
        └── dashboards/
            ├── dashboard.yml
            └── aircraft-states.json
```

---

## Quick Start

### 1. (Optional) Choose a query mode and region

By default the pipeline runs in `point` mode centered on Cairo with a 250nm
radius — no credentials needed. To change this, edit
`docker-compose.yml` → `data-source.environment`:

```yaml
# adsb.lol mode: point | mil | ladd | pia
ADSBLOL_MODE: "point"

# Used only when ADSBLOL_MODE=point (radius in nautical miles, max 250)
ADSBLOL_LAT: "30.0444"
ADSBLOL_LON: "31.2357"
ADSBLOL_RADIUS: "250"
```

Switch `ADSBLOL_MODE` to `mil`, `ladd`, or `pia` for a global feed instead of
a point/radius search (in those modes, `ADSBLOL_LAT/LON/RADIUS` are ignored).

### 2. Build and start everything

```bash
cd aircraft-tracking-pipeline
docker compose up --build -d
```

> First build takes ~5-10 minutes (downloads Flume, Hadoop, Spark).

### 3. Watch logs

```bash
docker compose logs -f data-source        # poll results, aircraft counts
docker compose logs -f flume-producer
docker compose logs -f flume-consumer
docker compose logs -f spark-streaming
```

### 4. Verify each layer

#### Kafka — check messages arriving
```bash
docker exec -it kafka \
  kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic aircraft-states \
  --from-beginning \
  --max-messages 5
```

#### HDFS — check files landing
```bash
open http://localhost:9870

docker exec -it namenode hdfs dfs -ls /aircraft-states/
```

#### InfluxDB — check data
```bash
open http://localhost:8086
# Login: admin / adminpassword
# Org: pipeline-org
# Bucket: aircraft-states
# Measurements: aircraft_states_raw, aircraft_states_agg
```

#### Grafana — live dashboard
```bash
open http://localhost:3000
# Login: admin / admin
# Dashboard: "adsb.lol Aircraft Pipeline"
#   - Live Aircraft Positions (Geomap)
#   - Aircraft Count by Type (current)
#   - Total Tracked Aircraft Over Time
#   - Average Ground Speed by Aircraft Type
#   - Average Baro Altitude by Aircraft Type
#   - Aircraft On Ground vs Airborne (current)
```

---

## Useful Commands

### Stop everything
```bash
docker compose down
```

### Stop and remove all data volumes
```bash
docker compose down -v
```

### Rebuild a single service
```bash
docker compose up --build spark-streaming -d
```

### Check Kafka topic
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --all-groups
```

### HDFS CLI
```bash
docker exec -it namenode bash
hdfs dfs -ls /aircraft-states/
hdfs dfs -cat "/aircraft-states/dt=2026-06-14/hr=12/states-*.json" | head -5
```

---

## Configuration Reference

| Variable | Default | Description |
|---|---|---|
| `FLUME_HTTP_URL` | `http://flume-producer:44444` | Flume Producer's HTTP source endpoint |
| `POLL_INTERVAL_SECONDS` | `10` | How often to poll adsb.lol |
| `MAX_AIRCRAFT_PER_BATCH` | `300` | Cap on aircraft sent per poll (avoid overload) |
| `ADSBLOL_MODE` | `point` | Query mode: `point`, `mil`, `ladd`, or `pia` |
| `ADSBLOL_LAT` | `30.0444` (Cairo) | Center latitude — used only when `ADSBLOL_MODE=point` |
| `ADSBLOL_LON` | `31.2357` (Cairo) | Center longitude — used only when `ADSBLOL_MODE=point` |
| `ADSBLOL_RADIUS` | `250` | Search radius in nautical miles (max 250) — used only when `ADSBLOL_MODE=point` |
| `INFLUX_URL` | `http://influxdb:8086` | InfluxDB endpoint |
| `INFLUX_TOKEN` | `my-super-secret-token` | InfluxDB auth token |
| `INFLUX_ORG` | `pipeline-org` | InfluxDB organisation |
| `INFLUX_BUCKET` | `aircraft-states` | InfluxDB bucket |

---

## Data Model

`data-source/producer.py` flattens each aircraft object from the adsb.lol
`"ac"` array into a flat JSON event. Each Kafka message / HDFS record / Spark
row looks like this:

```json
{
  "hex": "4ca87c",
  "flight": "MSR785",
  "registration": "SU-GEM",
  "aircraft_type": "A320",
  "category": "A3",
  "latitude": 30.123,
  "longitude": 31.456,
  "alt_baro": 35000.0,
  "alt_geom": 35750.0,
  "ground_speed": 420.5,
  "track": 271.3,
  "baro_rate": 0.0,
  "geom_rate": 0.0,
  "squawk": "2200",
  "emergency": "none",
  "on_ground": false,
  "spi": false,
  "messages": 18421,
  "seen": 0.1,
  "seen_pos": 0.3,
  "rssi": -18.4,
  "ingested_at": "2026-06-14T10:15:00.123456+00:00"
}
```

| Field | Source field | Notes |
|---|---|---|
| `hex` | `hex` | ICAO24 transponder address |
| `flight` | `flight` | Callsign, whitespace-trimmed |
| `registration` | `r` | Tail number |
| `aircraft_type` | `t` | ICAO aircraft type designator (e.g. `A320`) |
| `category` | `category` | ADS-B emitter category (e.g. `A3`) |
| `latitude`, `longitude` | `lat`, `lon` | Position; events without both are dropped |
| `alt_baro` | `alt_baro` | Barometric altitude (ft). `0.0` when aircraft is on the ground (`alt_baro == "ground"`) |
| `alt_geom` | `alt_geom` | Geometric altitude (ft) |
| `ground_speed` | `gs` | Ground speed (knots) |
| `track` | `track` | True track (degrees) |
| `baro_rate`, `geom_rate` | `baro_rate`, `geom_rate` | Vertical rate (ft/min) |
| `squawk`, `emergency`, `spi`, `messages`, `seen`, `seen_pos`, `rssi` | same | Transponder/telemetry metadata |
| `on_ground` | derived | `true` if `alt_baro == "ground"` |
| `ingested_at` | — | UTC timestamp added at ingest time |

Spark writes two InfluxDB measurements:

- **`aircraft_states_raw`** — one point per aircraft per 10s batch
  (lat/lon/altitude/ground speed/track, tagged by `hex`, `aircraft_type`,
  `on_ground`) → feeds the Grafana Geomap
- **`aircraft_states_agg`** — 1-minute sliding-window stats (count, avg/max
  ground speed, avg baro altitude) grouped by `aircraft_type`

---

## Startup Order

```
Zookeeper → Kafka → kafka-init (creates "aircraft-states" topic)
                  → flume-producer
                  → flume-consumer (also waits for namenode)
                  → spark-streaming (also waits for influxdb)

namenode → datanode
         → flume-consumer

influxdb → grafana
         → spark-streaming
```

`data-source` waits 15 s after container start before its first poll.

---

## Architecture Notes

**Why two Flume agents?**
- **Flume Producer**: buffers between the HTTP poller and Kafka.
- **Flume Consumer**: durable file-channel buffering from Kafka → HDFS.

**Why drop entries with no position?**
adsb.lol omits `lat`/`lon` for aircraft whose last position report is stale
at query time. These can't be plotted or meaningfully aggregated, so they're
filtered out in the data source before reaching Kafka.

**Why cap `MAX_AIRCRAFT_PER_BATCH`?**
Global modes (`mil`, `ladd`, `pia`) and large-radius `point` queries can
return hundreds of aircraft in a single response. Sending all of them every
poll would overwhelm Flume's HTTP source and Kafka for a demo setup. Narrow
`ADSBLOL_RADIUS` (point mode) to focus on a smaller region instead of relying
solely on the cap.

---

## Attribution

Aircraft data courtesy of [adsb.lol](https://adsb.lol), licensed under the
[Open Data Commons Open Database License (ODbL) v1.0](https://opendatacommons.org/licenses/odbl/1-0/).