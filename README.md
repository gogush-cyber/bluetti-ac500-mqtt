# Bluetti AC500 → MQTT Bridge

A Bluetooth-to-MQTT bridge for the **Bluetti AC500** power station, with full **Home Assistant auto-discovery**. Runs inside Docker on any Linux host with a USB Bluetooth adapter.

## Features

- **BLE polling** — reads all AC500 sensors on a configurable interval (default 30 s)
- **MQTT auto-discovery** — all 19 entities self-register in Home Assistant on startup
- **Bidirectional control** — write to MQTT topics to toggle outputs, set charge mode, etc.
- **Official Bluetti encryption** — supports the `bluetti_crypt` library for encrypted BLE sessions (optional; falls back to unencrypted on older firmware)
- **Availability tracking** — LWT publishes `offline` on unexpected disconnect; `online` on connect
- **Auto-reconnect** — automatically re-scans and reconnects if BLE drops

### Entities created in Home Assistant

| Type    | Count | Examples |
|---------|-------|----------|
| Sensor  | 12    | Battery %, DC/AC power, voltage, frequency, pack info |
| Switch  | 4     | AC output, DC output, Grid charge, Time control |
| Select  | 2     | UPS mode, Sleep mode |
| Number  | 2     | Battery range start / end |

---

## Prerequisites

1. **Linux host** with a USB Bluetooth adapter (the container needs `/dev/bus/usb` passthrough)
2. **Docker** and **Docker Compose** installed
3. **(Optional)** Official Bluetti encryption library from
   [github.com/bluetti-official/bluetti-bluetooth-lib](https://github.com/bluetti-official/bluetti-bluetooth-lib/releases)
   Required only if your AC500 uses encrypted BLE (most units with recent firmware do).

---

## Installation

### Using Docker Compose (recommended)

**1. Clone the repository**

```bash
git clone https://github.com/gogush-cyber/bluetti-ac500-mqtt.git
cd bluetti-ac500-mqtt
```

**2. Create your config directory and config file**

```bash
mkdir -p /opt/bluetti/config
cp config/config.yaml.example /opt/bluetti/config/config.yaml
```

Edit `/opt/bluetti/config/config.yaml` with your values — see [Configuration](#configuration) below.

**3. (Optional) Install the Bluetti encryption library**

Download `bluetti_crypt` from the [official releases page](https://github.com/bluetti-official/bluetti-bluetooth-lib/releases), then copy it into the container image at build time:

```bash
unzip bluetti_crypt.zip -d bluetti_crypt
# Place bluetti_crypt.py and _bluetti_crypt.so next to the Dockerfile before building
```

Also place your **device licence file** (`bluetti_device_licence.csv`) in `/opt/bluetti/config/`. The licence uses your Communication Board SN:
*Bluetti App → Settings → About Device → Communication Board SN*

**4. Update `docker-compose.yml`**

Edit the volume mount path to match your host config directory:

```yaml
volumes:
  - /opt/bluetti/config:/config:ro   # adjust to your actual path
```

**5. Build and start**

```bash
docker compose up -d --build
docker compose logs -f
```

### Running directly with Python

```bash
pip install bleak paho-mqtt crcmod pyyaml

# Find your device first:
python bluetti_ac500_mqtt.py --scan

# Then run the bridge:
python bluetti_ac500_mqtt.py \
    --address AA:BB:CC:DD:EE:FF \
    --broker 192.168.1.100 \
    --port 1883 \
    --interval 30
```

---

## Configuration

Copy `config/config.yaml.example` to `config/config.yaml` (or to `/opt/bluetti/config/config.yaml` for Docker) and edit:

```yaml
ble:
  address:  "AA:BB:CC:DD:EE:FF"   # BLE MAC of your AC500 — run --scan to find it
  interval: 30                    # Polling interval in seconds (min 10)

mqtt:
  broker:   "192.168.1.100"       # Your MQTT broker IP / hostname
  port:     1883
  username: ""                    # MQTT username (leave empty if none)
  password: ""                    # MQTT password (leave empty if none)
  topic:    "bluetti/ac500"       # Base MQTT topic prefix

logging:
  level: "INFO"                   # DEBUG | INFO | WARNING | ERROR
```

All settings can also be overridden via CLI arguments (CLI takes priority over config file).

---

## Finding your device BLE address

```bash
python bluetti_ac500_mqtt.py --scan
# or inside Docker:
docker compose run --rm bluetti-mqtt --scan
```

---

## MQTT Topics

### State (read-only)

| Topic | Description |
|-------|-------------|
| `bluetti/ac500/state` | Full JSON snapshot of all values |
| `bluetti/ac500/dc_input_power` | DC input power (W) |
| `bluetti/ac500/ac_input_power` | AC input power (W) |
| `bluetti/ac500/ac_output_power` | AC output power (W) |
| `bluetti/ac500/dc_output_power` | DC output power (W) |
| `bluetti/ac500/power_generation` | Power generated today (kWh) |
| `bluetti/ac500/total_battery_percent` | Battery level (%) |
| `bluetti/ac500/total_battery_voltage` | Battery voltage (V) |
| `bluetti/ac500/ac_input_voltage` | AC input voltage (V) |
| `bluetti/ac500/ac_input_frequency` | AC input frequency (Hz) |
| `bluetti/ac500/pack_num` | Battery pack number |
| `bluetti/ac500/pack_voltage` | Pack voltage (V) |
| `bluetti/ac500/pack_battery_percent` | Pack battery level (%) |
| `bluetti/ac500/availability` | `online` / `offline` |

### Control (write)

Publish to these topics to control the device:

| Topic | Values |
|-------|--------|
| `bluetti/ac500/set/ac_output_on` | `1` / `0` |
| `bluetti/ac500/set/dc_output_on` | `1` / `0` |
| `bluetti/ac500/set/grid_charge_on` | `1` / `0` |
| `bluetti/ac500/set/time_control_on` | `1` / `0` |
| `bluetti/ac500/set/ups_mode` | `customized` \| `pv_priority` \| `standard` \| `time_control` |
| `bluetti/ac500/set/auto_sleep_mode` | `30s` \| `1min` \| `5min` \| `never` |
| `bluetti/ac500/set/battery_range_start` | `0`–`100` |
| `bluetti/ac500/set/battery_range_end` | `0`–`100` |

---

## Home Assistant Auto-Discovery

No manual YAML configuration required. On startup the bridge publishes retained discovery payloads to `homeassistant/<domain>/bluetti_ac500_<name>/config`. Home Assistant (with MQTT integration enabled) automatically creates all entities under a single **Bluetti AC500** device.

---

## Web Status Page (v4)

An optional dark web dashboard that shows live sensor values, connection status, control buttons, and a historical chart — no Home Assistant required.

### Enable

**Via config file** (`config/config.yaml`):

```yaml
status_page:
  enabled:  true
  port:     8273         # HTTP listen port
  host:     "0.0.0.0"   # Bind address
  refresh:  10           # Browser auto-refresh in seconds
  history:  100          # Max chart data points in memory
```

**Via CLI flag**:

```bash
python bluetti_ac500_mqtt.py --address AA:BB:CC:DD:EE:FF --broker 192.168.1.100 \
    --status-page --status-port 8273
```

Then open `http://<host-ip>:8273` in your browser.

### What it shows

| Section | Content |
|---------|---------|
| Status pills | BLE connected, MQTT online, last update time |
| Sensor grid | 12 live sensor cards (power, battery, voltage, frequency) |
| Controls | 4 toggle buttons, 2 mode selects, 2 battery-range inputs |
| History chart | Line chart: battery % + 4 power readings over last N polls |

Control buttons (AC/DC output, Grid Charge, Time Control) write directly to the same BLE command queue used by MQTT — no extra configuration needed.

### Docker note

The compose file uses `network_mode: host` by default (required for Bluetooth), so port 8273 is automatically accessible on the host. If you switch to bridge networking, uncomment the `ports:` entry in `docker-compose.yml`.

---

## Diagnostics

Use `diagnose.py` to inspect raw register values from the AC500 over BLE:

```bash
python diagnose.py --address AA:BB:CC:DD:EE:FF
```

This prints raw register hex + parsed values and is useful for debugging communication issues.

---

## Docker Compose Reference

```yaml
version: "3.8"

services:
  bluetti-mqtt:
    image: bluetti_ac500_mqtt:v1
    build: .
    container_name: bluetti_ac500_mqtt
    restart: unless-stopped
    network_mode: host
    privileged: true
    volumes:
      - /opt/bluetti/config:/config:ro
      - /sys/class/bluetooth:/sys/class/bluetooth
      - /dev/bus/usb:/dev/bus/usb
```

`network_mode: host` and `privileged: true` are required for Bluetooth hardware access.

---

## License

MIT License — see [LICENSE](LICENSE) for details.
