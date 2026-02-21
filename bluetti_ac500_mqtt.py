"""
Bluetti AC500 -> MQTT Bridge  (v5)
====================================
v5 adds full multi-pack B300 support and fixes per-pack HA discovery templates.

v4 features retained:
  - Optional dark web status dashboard (aiohttp, port 8273)
  - Live sensor grid, control buttons, Chart.js history chart

v5 new features:
  - Per-pack polling via register 3006 selector (all 6 B300 slots)
  - Pack voltage + SOC published individually (pack_1 … pack_6)
  - Packs connected count derived from actual live data (not raw reg 96)
  - Fixed HA MQTT discovery value_template for per-pack sensors

v3 features retained:
  - MQTT auto-discovery for all entities (sensors, switches, selects, numbers)
  - Device grouping in HA (all entities under one "Bluetti AC500" device)
  - Availability topic (online/offline with LWT)

v2 features retained:
  - Bidirectional control via bluetti/ac500/set/<param>
  - 8 writable parameters (AC/DC output, grid charge, UPS mode, etc.)

Prerequisites
-------------
1. Download the official library from:
   https://github.com/bluetti-official/bluetti-bluetooth-lib/releases

2. Install the encryption module into your Python site-packages:
   unzip bluetti_crypt.zip -C bluetti_crypt
   cp -rf bluetti_crypt.py _bluetti_crypt.so $(python3 -c "import site; print(site.getsitepackages()[0])")/

3. Place your device licence file (bluetti_device_licence.csv) in the
   same directory as this script.

4. Install Python dependencies:
   pip install bleak paho-mqtt crcmod pyyaml

Usage
-----
   python bluetti_ac500_mqtt.py --scan
   python bluetti_ac500_mqtt.py --address AA:BB:CC:DD:EE:FF --broker 192.168.1.100

MQTT Auto-Discovery
-------------------
On startup the bridge publishes retained configs to:
  homeassistant/<domain>/bluetti_ac500_<name>/config

Entities created automatically in HA:
  Sensors (12):  DC/AC input power, AC/DC output power, power generation,
                 battery %, battery voltage, AC voltage, AC frequency,
                 pack voltage, pack %, pack number
  Switches (4):  AC output, DC output, Grid charge, Time control
  Selects  (2):  UPS mode, Sleep mode
  Numbers  (2):  Battery range start/end

MQTT Control Topics
-------------------
  bluetti/ac500/set/ac_output_on        1 / 0
  bluetti/ac500/set/dc_output_on        1 / 0
  bluetti/ac500/set/grid_charge_on      1 / 0
  bluetti/ac500/set/time_control_on     1 / 0
  bluetti/ac500/set/ups_mode            customized | pv_priority | standard | time_control
  bluetti/ac500/set/battery_range_start 0-100
  bluetti/ac500/set/battery_range_end   0-100
  bluetti/ac500/set/auto_sleep_mode     30s | 1min | 5min | never
"""

import asyncio
import argparse
import collections
import json
import logging
import os
import queue as stdlib_queue
import struct
import sys
import time
from dataclasses import dataclass, asdict
from typing import Optional

import crcmod
import yaml
from bleak import BleakClient, BleakScanner
import paho.mqtt.client as mqtt
from aiohttp import web

# -- Logging -------------------------------------------------------------------
_LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bluetti.log")
_log_fmt   = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")

_file_handler = logging.FileHandler(_LOG_FILE, encoding="utf-8")
_file_handler.setFormatter(_log_fmt)
_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_log_fmt)

logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _console_handler])
logger = logging.getLogger(__name__)

# -- Official Bluetti encryption library ---------------------------------------
try:
    import bluetti_crypt
    ENCRYPTION_AVAILABLE = True
except ImportError:
    ENCRYPTION_AVAILABLE = False
    logger.warning(
        "bluetti_crypt not found -- encryption skipped. "
        "Install from https://github.com/bluetti-official/bluetti-bluetooth-lib"
    )

# -- BLE constants -------------------------------------------------------------
NOTIFY_UUID = "0000ff01-0000-1000-8000-00805f9b34fb"
WRITE_UUID  = "0000ff02-0000-1000-8000-00805f9b34fb"

crc16_modbus = crcmod.predefined.mkCrcFun("modbus")

# -- MODBUS register map (readable) --------------------------------------------
AC500_REGISTERS = {
    36: ("dc_input_power",         1,     "W"),
    37: ("ac_input_power",         1,     "W"),
    38: ("ac_output_power",        1,     "W"),
    39: ("dc_output_power",        1,     "W"),
    41: ("power_generation",       0.1,   "kWh"),
    43: ("total_battery_percent",  1,     "%"),
    48: ("ac_output_on",           1,     "bool"),
    49: ("dc_output_on",           1,     "bool"),
    77: ("ac_input_voltage",       0.1,   "V"),
    80: ("ac_input_frequency",     0.01,  "Hz"),
    92: ("total_battery_voltage",  0.1,   "V"),
    96: ("pack_num",               1,     ""),    # number of connected B300 packs
    # Registers 98 (pack_voltage) and 99 (pack_battery_percent) report data for
    # whichever pack is currently selected via register 3006 (pack_num selector).
    # Per-pack data is populated in _connect_and_poll() by iterating packs.
    # Settings (writable + readable for state feedback)
    3001: ("ups_mode",            1,  ""),
    3011: ("grid_charge_on",      1,  "bool"),
    3013: ("time_control_on",     1,  "bool"),
    3015: ("battery_range_start", 1,  ""),
    3016: ("battery_range_end",   1,  ""),
    3061: ("auto_sleep_mode",     1,  ""),
}

# Main poll ranges — does NOT include per-pack data (handled separately via pack selector)
QUERY_RANGES = [
    (10,   40, "main"),
    (70,   21, "details"),
    (91,    9, "battery"),    # regs 91-99: pack_num_max(91), total_battery_voltage(92), pack_num(96)
    (3001, 16, "settings"),
    (3061,  1, "sleep"),
]

# Per-pack poll: registers read after selecting each pack via reg 3006.
# AC500 supports up to 6 B300 packs. Register 96 is the currently-selected pack number
# (not the total connected count), so we always iterate all slots and filter by voltage.
PACK_QUERY_START  = 91
PACK_QUERY_COUNT  = 37   # covers 91-127 (matches warhammerkid reference impl)
PACK_SWITCH_DELAY = 3    # seconds to wait after writing reg 3006 before reading pack data
PACK_NUM_MAX      = 6    # AC500 maximum B300 slots

# -- Writable register map (FC=06) ---------------------------------------------
AC500_WRITABLE = {
    3001: ("ups_mode",            "enum", {"customized": 1, "pv_priority": 2,
                                           "standard": 3,  "time_control": 4}),
    3007: ("ac_output_on",        "bool", None),
    3008: ("dc_output_on",        "bool", None),
    3011: ("grid_charge_on",      "bool", None),
    3013: ("time_control_on",     "bool", None),
    3015: ("battery_range_start", "uint", None),
    3016: ("battery_range_end",   "uint", None),
    3061: ("auto_sleep_mode",     "enum", {"30s": 2, "1min": 3, "5min": 4, "never": 5}),
}
_WRITABLE_BY_NAME = {v[0]: (k, v[1], v[2]) for k, v in AC500_WRITABLE.items()}

# -- MQTT auto-discovery entity definitions ------------------------------------
# "{base}" is replaced at publish time with the configured base topic.
# Each dict maps 1:1 to the HA MQTT discovery payload fields;
# "domain" selects the entity type and is removed before publishing.
DISCOVERY_ENTITIES = [
    # ── Sensors ───────────────────────────────────────────────────────────────
    {"domain": "sensor", "unique_id": "bluetti_ac500_dc_input_power",
     "name": "AC500 DC Input Power",
     "state_topic": "{base}/state", "value_template": "{{ value_json.dc_input_power }}",
     "unit_of_measurement": "W", "device_class": "power", "state_class": "measurement"},

    {"domain": "sensor", "unique_id": "bluetti_ac500_ac_input_power",
     "name": "AC500 AC Input Power",
     "state_topic": "{base}/state", "value_template": "{{ value_json.ac_input_power }}",
     "unit_of_measurement": "W", "device_class": "power", "state_class": "measurement"},

    {"domain": "sensor", "unique_id": "bluetti_ac500_ac_output_power",
     "name": "AC500 AC Output Power",
     "state_topic": "{base}/state", "value_template": "{{ value_json.ac_output_power }}",
     "unit_of_measurement": "W", "device_class": "power", "state_class": "measurement"},

    {"domain": "sensor", "unique_id": "bluetti_ac500_dc_output_power",
     "name": "AC500 DC Output Power",
     "state_topic": "{base}/state", "value_template": "{{ value_json.dc_output_power }}",
     "unit_of_measurement": "W", "device_class": "power", "state_class": "measurement"},

    {"domain": "sensor", "unique_id": "bluetti_ac500_power_generation",
     "name": "AC500 Power Generation",
     "state_topic": "{base}/state", "value_template": "{{ value_json.power_generation }}",
     "unit_of_measurement": "kWh", "device_class": "energy",
     "state_class": "total_increasing"},

    {"domain": "sensor", "unique_id": "bluetti_ac500_total_battery_percent",
     "name": "AC500 Battery Percent",
     "state_topic": "{base}/state",
     "value_template": "{{ value_json.total_battery_percent }}",
     "unit_of_measurement": "%", "device_class": "battery", "state_class": "measurement"},

    {"domain": "sensor", "unique_id": "bluetti_ac500_total_battery_voltage",
     "name": "AC500 Battery Voltage",
     "state_topic": "{base}/state",
     "value_template": "{{ value_json.total_battery_voltage }}",
     "unit_of_measurement": "V", "device_class": "voltage", "state_class": "measurement"},

    {"domain": "sensor", "unique_id": "bluetti_ac500_ac_input_voltage",
     "name": "AC500 AC Input Voltage",
     "state_topic": "{base}/state", "value_template": "{{ value_json.ac_input_voltage }}",
     "unit_of_measurement": "V", "device_class": "voltage", "state_class": "measurement"},

    {"domain": "sensor", "unique_id": "bluetti_ac500_ac_input_frequency",
     "name": "AC500 AC Input Frequency",
     "state_topic": "{base}/state",
     "value_template": "{{ value_json.ac_input_frequency }}",
     "unit_of_measurement": "Hz", "device_class": "frequency", "state_class": "measurement"},

    {"domain": "sensor", "unique_id": "bluetti_ac500_pack_num",
     "name": "AC500 Pack Count",
     "state_topic": "{base}/state", "value_template": "{{ value_json.pack_num }}"},

    # ── Per-pack sensors (B300 packs 1–6) ─────────────────────────────────────
    *[entity for n in range(1, 7) for entity in (
        {"domain": "sensor",
         "unique_id": f"bluetti_ac500_pack_{n}_voltage",
         "name": f"AC500 Pack {n} Voltage",
         "state_topic": "{base}/state",
         "value_template": f"{{{{ value_json.pack_{n}_voltage }}}}",
         "unit_of_measurement": "V", "device_class": "voltage",
         "state_class": "measurement"},
        {"domain": "sensor",
         "unique_id": f"bluetti_ac500_pack_{n}_percent",
         "name": f"AC500 Pack {n} Battery %",
         "state_topic": "{base}/state",
         "value_template": f"{{{{ value_json.pack_{n}_percent }}}}",
         "unit_of_measurement": "%", "device_class": "battery",
         "state_class": "measurement"},
    )],

    # ── Switches ──────────────────────────────────────────────────────────────
    {"domain": "switch", "unique_id": "bluetti_ac500_ac_output_sw",
     "name": "AC500 AC Output",
     "state_topic": "{base}/ac_output_on",
     "command_topic": "{base}/set/ac_output_on",
     "payload_on": "1", "payload_off": "0",
     "state_on": "true", "state_off": "false",
     "device_class": "outlet"},

    {"domain": "switch", "unique_id": "bluetti_ac500_dc_output_sw",
     "name": "AC500 DC Output",
     "state_topic": "{base}/dc_output_on",
     "command_topic": "{base}/set/dc_output_on",
     "payload_on": "1", "payload_off": "0",
     "state_on": "true", "state_off": "false",
     "device_class": "outlet"},

    {"domain": "switch", "unique_id": "bluetti_ac500_grid_charge_sw",
     "name": "AC500 Grid Charge",
     "state_topic": "{base}/grid_charge_on",
     "command_topic": "{base}/set/grid_charge_on",
     "payload_on": "1", "payload_off": "0",
     "state_on": "true", "state_off": "false"},

    {"domain": "switch", "unique_id": "bluetti_ac500_time_control_sw",
     "name": "AC500 Time Control",
     "state_topic": "{base}/time_control_on",
     "command_topic": "{base}/set/time_control_on",
     "payload_on": "1", "payload_off": "0",
     "state_on": "true", "state_off": "false"},

    # ── Selects ───────────────────────────────────────────────────────────────
    {"domain": "select", "unique_id": "bluetti_ac500_ups_mode_sel",
     "name": "AC500 UPS Mode",
     "state_topic": "{base}/ups_mode",
     "command_topic": "{base}/set/ups_mode",
     "options": ["customized", "pv_priority", "standard", "time_control"],
     "value_template": (
         "{% set m={1:'customized',2:'pv_priority',3:'standard',4:'time_control'} %}"
         "{{ m.get(value|int,'unknown') }}"
     ),
     "command_template": (
         "{% set m={'customized':'1','pv_priority':'2','standard':'3','time_control':'4'} %}"
         "{{ m.get(value,value) }}"
     )},

    {"domain": "select", "unique_id": "bluetti_ac500_sleep_mode_sel",
     "name": "AC500 Sleep Mode",
     "state_topic": "{base}/auto_sleep_mode",
     "command_topic": "{base}/set/auto_sleep_mode",
     "options": ["30s", "1min", "5min", "never"],
     "value_template": (
         "{% set m={2:'30s',3:'1min',4:'5min',5:'never'} %}"
         "{{ m.get(value|int,'unknown') }}"
     ),
     "command_template": (
         "{% set m={'30s':'2','1min':'3','5min':'4','never':'5'} %}"
         "{{ m.get(value,value) }}"
     )},

    # ── Numbers ───────────────────────────────────────────────────────────────
    {"domain": "number", "unique_id": "bluetti_ac500_batt_range_start",
     "name": "AC500 Battery Range Start",
     "state_topic": "{base}/battery_range_start",
     "command_topic": "{base}/set/battery_range_start",
     "min": 0, "max": 100, "step": 1,
     "unit_of_measurement": "%"},

    {"domain": "number", "unique_id": "bluetti_ac500_batt_range_end",
     "name": "AC500 Battery Range End",
     "state_topic": "{base}/battery_range_end",
     "command_topic": "{base}/set/battery_range_end",
     "min": 0, "max": 100, "step": 1,
     "unit_of_measurement": "%"},
]


# -- Data model ----------------------------------------------------------------
@dataclass
class DeviceState:
    dc_input_power:        Optional[float] = None
    ac_input_power:        Optional[float] = None
    ac_output_power:       Optional[float] = None
    dc_output_power:       Optional[float] = None
    power_generation:      Optional[float] = None
    total_battery_percent: Optional[float] = None
    ac_output_on:          Optional[bool]  = None
    dc_output_on:          Optional[bool]  = None
    ac_input_voltage:      Optional[float] = None
    ac_input_frequency:    Optional[float] = None
    total_battery_voltage: Optional[float] = None
    pack_num:       Optional[int]   = None    # connected B300 pack count
    pack_1_voltage: Optional[float] = None
    pack_1_percent: Optional[int]   = None
    pack_2_voltage: Optional[float] = None
    pack_2_percent: Optional[int]   = None
    pack_3_voltage: Optional[float] = None
    pack_3_percent: Optional[int]   = None
    pack_4_voltage: Optional[float] = None
    pack_4_percent: Optional[int]   = None
    pack_5_voltage: Optional[float] = None
    pack_5_percent: Optional[int]   = None
    pack_6_voltage: Optional[float] = None
    pack_6_percent: Optional[int]   = None
    ups_mode:              Optional[int]   = None
    grid_charge_on:        Optional[bool]  = None
    time_control_on:       Optional[bool]  = None
    battery_range_start:   Optional[int]   = None
    battery_range_end:     Optional[int]   = None
    auto_sleep_mode:       Optional[int]   = None


@dataclass
class StatusState:
    device_state:  Optional[DeviceState] = None
    ble_connected: bool = False
    mqtt_online:   bool = False
    last_update:   Optional[float] = None
    history:       object = None   # collections.deque

    def __post_init__(self):
        if self.history is None:
            self.history = collections.deque(maxlen=100)

    def update(self, state: DeviceState):
        self.device_state  = state
        self.ble_connected = True
        self.last_update   = time.time()
        self.history.append({
            "ts":                    self.last_update,
            "total_battery_percent": state.total_battery_percent,
            "ac_output_power":       state.ac_output_power,
            "dc_output_power":       state.dc_output_power,
            "ac_input_power":        state.ac_input_power,
            "dc_input_power":        state.dc_input_power,
        })

    def as_api_dict(self) -> dict:
        from dataclasses import asdict as _asdict
        return {
            "state":         _asdict(self.device_state) if self.device_state else {},
            "ble_connected": self.ble_connected,
            "mqtt_online":   self.mqtt_online,
            "last_update":   self.last_update,
            "history":       list(self.history),
        }


# -- Packet helpers ------------------------------------------------------------
def build_modbus_query(start_address: int, register_count: int) -> bytes:
    payload = struct.pack(">BBHH", 0x01, 0x03, start_address, register_count)
    return payload + struct.pack("<H", crc16_modbus(payload))


def build_modbus_write(register: int, value: int) -> bytes:
    payload = struct.pack(">BBHH", 0x01, 0x06, register, value)
    return payload + struct.pack("<H", crc16_modbus(payload))


def parse_modbus_response(data: bytes, base_address: int) -> dict:
    if len(data) < 5:
        return {}
    body     = data[:-2]
    crc_recv = struct.unpack("<H", data[-2:])[0]
    if crc_recv != crc16_modbus(body):
        logger.warning("CRC mismatch in response")
        return {}
    byte_count = data[2]
    return {
        base_address + i: struct.unpack(">H", data[3 + i*2: 5 + i*2])[0]
        for i in range(byte_count // 2)
    }


def registers_to_state(registers: dict) -> DeviceState:
    state = DeviceState()
    for offset, (attr, scale, unit) in AC500_REGISTERS.items():
        if offset in registers:
            raw = registers[offset]
            if unit == "bool":
                setattr(state, attr, bool(raw))
            elif scale != 1:
                setattr(state, attr, round(raw * scale, 2))
            else:
                setattr(state, attr, raw)
    return state


def _sub_base(obj, base: str):
    """Recursively replace '{base}' placeholder with the actual base topic."""
    if isinstance(obj, str):
        return obj.replace("{base}", base)
    if isinstance(obj, dict):
        return {k: _sub_base(v, base) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sub_base(v, base) for v in obj]
    return obj


# -- Web status page HTML ------------------------------------------------------
# Regular string — NOT an f-string. CSS/JS braces are literal.
# Two tokens replaced at serve time:
#   {refresh_ms}  → refresh interval in milliseconds
#   {device_name} → BLE device address
_STATUS_PAGE_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Bluetti AC500 &#8212; {device_name}</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    :root {
      --bg:     #0f1117;
      --card:   #1a1d27;
      --border: #2a2d3d;
      --accent: #4fc3f7;
      --ok:     #66bb6a;
      --warn:   #ffa726;
      --danger: #ef5350;
      --text:   #e0e0e0;
      --muted:  #8888a0;
    }
    body { background: var(--bg); color: var(--text); font-family: system-ui, sans-serif; padding: 1rem 1.5rem; }
    h1   { font-size: 1.4rem; font-weight: 600; color: var(--accent); }
    h2   { font-size: .85rem; font-weight: 600; color: var(--muted); text-transform: uppercase;
           letter-spacing: .06em; margin: 1.5rem 0 .6rem; }
    header { display: flex; align-items: center; gap: 1rem; flex-wrap: wrap; margin-bottom: 1rem; }
    .pills  { display: flex; gap: .5rem; flex-wrap: wrap; margin-left: auto; }
    .pill   { display: inline-flex; align-items: center; gap: .35rem; padding: .25rem .7rem;
              border-radius: 999px; font-size: .78rem; font-weight: 500;
              background: var(--card); border: 1px solid var(--border); }
    .pill .dot        { width: 8px; height: 8px; border-radius: 50%; background: var(--muted); }
    .pill.ok .dot     { background: var(--ok); }
    .pill.danger .dot { background: var(--danger); }
    .sensor-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px,1fr)); gap: .75rem; }
    .card       { background: var(--card); border: 1px solid var(--border); border-radius: .6rem;
                  padding: .85rem 1rem; }
    .card-label { font-size: .72rem; color: var(--muted); text-transform: uppercase;
                  letter-spacing: .05em; margin-bottom: .3rem; }
    .card-value { font-size: 1.6rem; font-weight: 700; line-height: 1.1; }
    .card-unit  { font-size: .75rem; color: var(--muted); margin-left: .2rem; }
    .controls-row { display: flex; flex-wrap: wrap; gap: .6rem; align-items: center; }
    .btn { padding: .45rem 1.1rem; border-radius: .4rem; border: 1px solid var(--border);
           background: var(--card); color: var(--text); cursor: pointer; font-size: .85rem;
           transition: background .15s; }
    .btn:hover    { background: #252836; }
    .btn.active   { background: #1a3a22; border-color: var(--ok);     color: var(--ok); }
    .btn.inactive { background: #3a1a1a; border-color: var(--danger); color: var(--danger); }
    select, input[type=number] { background: var(--card); color: var(--text);
      border: 1px solid var(--border); border-radius: .4rem; padding: .4rem .7rem; font-size: .85rem; }
    input[type=number] { width: 80px; }
    .set-btn { padding: .4rem .9rem; border-radius: .4rem; border: 1px solid var(--accent);
               background: transparent; color: var(--accent); cursor: pointer; font-size: .85rem; }
    .set-btn:hover { background: rgba(79,195,247,.1); }
    .ctrl-group { display: flex; align-items: center; gap: .4rem; }
    .ctrl-label { font-size: .78rem; color: var(--muted); min-width: 60px; }
    .chart-wrap { background: var(--card); border: 1px solid var(--border); border-radius: .6rem;
                  padding: 1rem; margin-top: .5rem; position: relative; height: 280px; }
  </style>
</head>
<body>

<header>
  <h1>&#9889; Bluetti AC500</h1>
  <span style="color:var(--muted);font-size:.85rem">{device_name}</span>
  <div class="pills">
    <span class="pill" id="pill-ble"><span class="dot"></span>BLE</span>
    <span class="pill" id="pill-mqtt"><span class="dot"></span>MQTT</span>
    <span class="pill" id="pill-ts"><span class="dot"></span>&#8212;</span>
  </div>
</header>

<h2>Sensors</h2>
<div class="sensor-grid">
  <div class="card"><div class="card-label">AC Output Power</div>
    <div class="card-value" id="v-ac_output_power">&#8212;<span class="card-unit">W</span></div></div>
  <div class="card"><div class="card-label">DC Output Power</div>
    <div class="card-value" id="v-dc_output_power">&#8212;<span class="card-unit">W</span></div></div>
  <div class="card"><div class="card-label">AC Input Power</div>
    <div class="card-value" id="v-ac_input_power">&#8212;<span class="card-unit">W</span></div></div>
  <div class="card"><div class="card-label">DC Input Power</div>
    <div class="card-value" id="v-dc_input_power">&#8212;<span class="card-unit">W</span></div></div>
  <div class="card"><div class="card-label">Battery</div>
    <div class="card-value" id="v-total_battery_percent">&#8212;<span class="card-unit">%</span></div></div>
  <div class="card"><div class="card-label">Battery Voltage</div>
    <div class="card-value" id="v-total_battery_voltage">&#8212;<span class="card-unit">V</span></div></div>
  <div class="card"><div class="card-label">AC Input Voltage</div>
    <div class="card-value" id="v-ac_input_voltage">&#8212;<span class="card-unit">V</span></div></div>
  <div class="card"><div class="card-label">AC Frequency</div>
    <div class="card-value" id="v-ac_input_frequency">&#8212;<span class="card-unit">Hz</span></div></div>
  <div class="card"><div class="card-label">Packs Connected</div>
    <div class="card-value" id="v-pack_num">&#8212;</div></div>
  <div class="card"><div class="card-label">Power Generation</div>
    <div class="card-value" id="v-power_generation">&#8212;<span class="card-unit">kWh</span></div></div>
</div>

<h2>Battery Packs</h2>
<div class="sensor-grid" id="pack-grid">
  <div class="card pack-card" id="pack-card-1" style="display:none">
    <div class="card-label">Pack 1</div>
    <div style="font-size:1.2rem;font-weight:700" id="pack-1-pct">&#8212;<span class="card-unit">%</span></div>
    <div style="font-size:.85rem;color:var(--muted);margin-top:.2rem" id="pack-1-v">&#8212; V</div>
  </div>
  <div class="card pack-card" id="pack-card-2" style="display:none">
    <div class="card-label">Pack 2</div>
    <div style="font-size:1.2rem;font-weight:700" id="pack-2-pct">&#8212;<span class="card-unit">%</span></div>
    <div style="font-size:.85rem;color:var(--muted);margin-top:.2rem" id="pack-2-v">&#8212; V</div>
  </div>
  <div class="card pack-card" id="pack-card-3" style="display:none">
    <div class="card-label">Pack 3</div>
    <div style="font-size:1.2rem;font-weight:700" id="pack-3-pct">&#8212;<span class="card-unit">%</span></div>
    <div style="font-size:.85rem;color:var(--muted);margin-top:.2rem" id="pack-3-v">&#8212; V</div>
  </div>
  <div class="card pack-card" id="pack-card-4" style="display:none">
    <div class="card-label">Pack 4</div>
    <div style="font-size:1.2rem;font-weight:700" id="pack-4-pct">&#8212;<span class="card-unit">%</span></div>
    <div style="font-size:.85rem;color:var(--muted);margin-top:.2rem" id="pack-4-v">&#8212; V</div>
  </div>
  <div class="card pack-card" id="pack-card-5" style="display:none">
    <div class="card-label">Pack 5</div>
    <div style="font-size:1.2rem;font-weight:700" id="pack-5-pct">&#8212;<span class="card-unit">%</span></div>
    <div style="font-size:.85rem;color:var(--muted);margin-top:.2rem" id="pack-5-v">&#8212; V</div>
  </div>
  <div class="card pack-card" id="pack-card-6" style="display:none">
    <div class="card-label">Pack 6</div>
    <div style="font-size:1.2rem;font-weight:700" id="pack-6-pct">&#8212;<span class="card-unit">%</span></div>
    <div style="font-size:.85rem;color:var(--muted);margin-top:.2rem" id="pack-6-v">&#8212; V</div>
  </div>
</div>

<h2>Controls</h2>
<div class="controls-row">
  <button class="btn" id="btn-ac_output_on"    onclick="toggle('ac_output_on',this)">AC Output</button>
  <button class="btn" id="btn-dc_output_on"    onclick="toggle('dc_output_on',this)">DC Output</button>
  <button class="btn" id="btn-grid_charge_on"  onclick="toggle('grid_charge_on',this)">Grid Charge</button>
  <button class="btn" id="btn-time_control_on" onclick="toggle('time_control_on',this)">Time Control</button>
  <div class="ctrl-group">
    <span class="ctrl-label">UPS Mode</span>
    <select id="sel-ups_mode" onchange="sendSet('ups_mode',this.value)">
      <option value="customized">Customized</option>
      <option value="pv_priority">PV Priority</option>
      <option value="standard">Standard</option>
      <option value="time_control">Time Control</option>
    </select>
  </div>
  <div class="ctrl-group">
    <span class="ctrl-label">Sleep Mode</span>
    <select id="sel-auto_sleep_mode" onchange="sendSet('auto_sleep_mode',this.value)">
      <option value="30s">30 s</option>
      <option value="1min">1 min</option>
      <option value="5min">5 min</option>
      <option value="never">Never</option>
    </select>
  </div>
  <div class="ctrl-group">
    <span class="ctrl-label">Batt Start</span>
    <input type="number" id="num-battery_range_start" min="0" max="100" value="20">
    <button class="set-btn"
      onclick="sendSet('battery_range_start',document.getElementById('num-battery_range_start').value)">Set</button>
  </div>
  <div class="ctrl-group">
    <span class="ctrl-label">Batt End</span>
    <input type="number" id="num-battery_range_end" min="0" max="100" value="80">
    <button class="set-btn"
      onclick="sendSet('battery_range_end',document.getElementById('num-battery_range_end').value)">Set</button>
  </div>
</div>

<h2>History</h2>
<div class="chart-wrap">
  <canvas id="histChart"></canvas>
</div>

<script>
const REFRESH_MS = {refresh_ms};

// ── Chart setup ──────────────────────────────────────────────────────────────
const ctx = document.getElementById('histChart').getContext('2d');
const chart = new Chart(ctx, {
  type: 'line',
  data: {
    datasets: [
      { label: 'Battery %',  data: [], borderColor: '#66bb6a', backgroundColor: 'transparent',
        yAxisID: 'pct', tension: 0.3, pointRadius: 2 },
      { label: 'AC Out (W)', data: [], borderColor: '#4fc3f7', backgroundColor: 'transparent',
        yAxisID: 'pwr', tension: 0.3, pointRadius: 2 },
      { label: 'DC Out (W)', data: [], borderColor: '#29b6f6', backgroundColor: 'transparent',
        yAxisID: 'pwr', tension: 0.3, pointRadius: 2 },
      { label: 'AC In (W)',  data: [], borderColor: '#ffa726', backgroundColor: 'transparent',
        yAxisID: 'pwr', tension: 0.3, pointRadius: 2 },
      { label: 'DC In (W)',  data: [], borderColor: '#ef5350', backgroundColor: 'transparent',
        yAxisID: 'pwr', tension: 0.3, pointRadius: 2 },
    ]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    interaction: { mode: 'index', intersect: false },
    scales: {
      x: {
        type: 'time',
        time: { tooltipFormat: 'HH:mm:ss', displayFormats: { second: 'HH:mm:ss', minute: 'HH:mm' } },
        ticks: { color: '#8888a0' }, grid: { color: '#2a2d3d' }
      },
      pct: {
        position: 'left', min: 0, max: 100,
        ticks: { color: '#66bb6a' }, grid: { color: '#2a2d3d' },
        title: { display: true, text: '%', color: '#66bb6a' }
      },
      pwr: {
        position: 'right', min: 0,
        ticks: { color: '#4fc3f7' }, grid: { drawOnChartArea: false },
        title: { display: true, text: 'W', color: '#4fc3f7' }
      }
    },
    plugins: {
      legend: { labels: { color: '#e0e0e0', boxWidth: 14, font: { size: 11 } } }
    }
  }
});

// ── State maps ────────────────────────────────────────────────────────────────
const UPS_MAP   = { 1: 'customized', 2: 'pv_priority', 3: 'standard', 4: 'time_control' };
const SLEEP_MAP = { 2: '30s', 3: '1min', 4: '5min', 5: 'never' };
let _state = {};

// ── Helpers ───────────────────────────────────────────────────────────────────
function setPill(id, ok) {
  const el = document.getElementById(id);
  el.className = 'pill ' + (ok ? 'ok' : 'danger');
}

function fmtTime(ts) {
  if (!ts) return '&#8212;';
  return new Date(ts * 1000).toLocaleTimeString();
}

function updateCards(s) {
  const FIELDS = [
    'ac_output_power','dc_output_power','ac_input_power','dc_input_power',
    'total_battery_percent','total_battery_voltage','ac_input_voltage',
    'ac_input_frequency','pack_num','power_generation'
  ];
  FIELDS.forEach(f => {
    const el = document.getElementById('v-' + f);
    if (!el) return;
    const v = s[f];
    if (v === undefined || v === null) return;
    const unit = el.querySelector('.card-unit');
    el.innerHTML = v + (unit ? unit.outerHTML : '');
  });
}

function updateButtons(s) {
  ['ac_output_on','dc_output_on','grid_charge_on','time_control_on'].forEach(k => {
    const btn = document.getElementById('btn-' + k);
    if (!btn || s[k] === undefined || s[k] === null) return;
    btn.className = 'btn ' + (s[k] ? 'active' : 'inactive');
  });
  const ups = UPS_MAP[s.ups_mode];
  if (ups) document.getElementById('sel-ups_mode').value = ups;
  const slp = SLEEP_MAP[s.auto_sleep_mode];
  if (slp) document.getElementById('sel-auto_sleep_mode').value = slp;
  if (s.battery_range_start != null)
    document.getElementById('num-battery_range_start').value = s.battery_range_start;
  if (s.battery_range_end != null)
    document.getElementById('num-battery_range_end').value = s.battery_range_end;
}

function updatePacks(s) {
  for (let n = 1; n <= 6; n++) {
    const v   = s['pack_' + n + '_voltage'];
    const pct = s['pack_' + n + '_percent'];
    const card = document.getElementById('pack-card-' + n);
    if (!card) continue;
    if (v == null || v === 0) {
      card.style.display = 'none';
    } else {
      card.style.display = '';
      document.getElementById('pack-' + n + '-pct').innerHTML =
        pct + '<span class="card-unit">%</span>';
      document.getElementById('pack-' + n + '-v').textContent = v + ' V';
    }
  }
}

function updateChart(history) {
  if (!history || !history.length) return;
  chart.data.datasets[0].data = history.map(h => ({ x: h.ts * 1000, y: h.total_battery_percent }));
  chart.data.datasets[1].data = history.map(h => ({ x: h.ts * 1000, y: h.ac_output_power }));
  chart.data.datasets[2].data = history.map(h => ({ x: h.ts * 1000, y: h.dc_output_power }));
  chart.data.datasets[3].data = history.map(h => ({ x: h.ts * 1000, y: h.ac_input_power }));
  chart.data.datasets[4].data = history.map(h => ({ x: h.ts * 1000, y: h.dc_input_power }));
  chart.update('none');
}

// ── Polling ───────────────────────────────────────────────────────────────────
async function refresh() {
  try {
    const resp = await fetch('/api/state');
    if (!resp.ok) return;
    const data = await resp.json();
    _state = data.state || {};
    setPill('pill-ble',  data.ble_connected);
    setPill('pill-mqtt', data.mqtt_online);
    const tsEl = document.getElementById('pill-ts');
    tsEl.className = 'pill' + (data.last_update ? ' ok' : '');
    tsEl.innerHTML = '<span class="dot"></span>' + fmtTime(data.last_update);
    updateCards(_state);
    updatePacks(_state);
    updateButtons(_state);
    updateChart(data.history);
  } catch (e) {
    setPill('pill-ble', false);
  }
}

// ── Control actions ───────────────────────────────────────────────────────────
async function sendSet(param, value) {
  try {
    await fetch('/api/set', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ param, value: String(value) })
    });
    setTimeout(refresh, 800);
  } catch (e) { console.error('sendSet error', e); }
}

function toggle(param, btn) {
  sendSet(param, btn.classList.contains('active') ? '0' : '1');
}

refresh();
setInterval(refresh, REFRESH_MS);
</script>
</body>
</html>'''


# -- Web status HTTP server ----------------------------------------------------
class StatusServer:
    def __init__(self, status_state: "StatusState", command_queue: stdlib_queue.Queue,
                 host: str, port: int, refresh: int, device_name: str):
        self._status_state  = status_state
        self._command_queue = command_queue
        self._host          = host
        self._port          = port
        self._refresh       = refresh
        self._device_name   = device_name
        self._runner        = None

    async def start(self):
        app = web.Application()
        app.router.add_get("/",          self._handle_index)
        app.router.add_get("/api/state", self._handle_api_state)
        app.router.add_post("/api/set",  self._handle_api_set)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()
        logger.info("Web status page: http://%s:%d", self._host, self._port)

    async def stop(self):
        if self._runner:
            await self._runner.cleanup()

    async def _handle_index(self, request):
        html = (_STATUS_PAGE_HTML
                .replace("{refresh_ms}",  str(self._refresh * 1000))
                .replace("{device_name}", self._device_name))
        return web.Response(text=html, content_type="text/html")

    async def _handle_api_state(self, request):
        return web.json_response(self._status_state.as_api_dict())

    async def _handle_api_set(self, request):
        try:
            body  = await request.json()
            param = str(body.get("param", "")).strip()
            value = str(body.get("value", "")).strip()
        except Exception:
            raise web.HTTPBadRequest(reason="Invalid JSON body")
        if param not in _WRITABLE_BY_NAME:
            raise web.HTTPBadRequest(reason=f"Unknown parameter: {param}")
        self._command_queue.put((param, value))
        logger.info("Web command queued: %s = %s", param, value)
        return web.json_response({"ok": True, "param": param, "value": value})


# -- MQTT publisher ------------------------------------------------------------
class MQTTPublisher:
    def __init__(self, broker: str, port: int, username: str, password: str,
                 base_topic: str = "bluetti/ac500",
                 command_queue: Optional[stdlib_queue.Queue] = None,
                 device_address: str = "",
                 status_state: Optional["StatusState"] = None):
        self.base_topic       = base_topic
        self._command_queue   = command_queue
        self._device_address  = device_address
        self._status_state    = status_state
        self.client           = mqtt.Client(client_id="bluetti_ac500_bridge")

        if username:
            self.client.username_pw_set(username, password)

        # Last Will: mark device offline on unexpected disconnect
        self.client.will_set(
            f"{self.base_topic}/availability",
            payload="offline",
            retain=True,
            qos=1,
        )

        self.client.on_connect    = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message    = self._on_message

        try:
            self.client.connect(broker, port, keepalive=60)
            self.client.loop_start()
            logger.info("MQTT connecting to %s:%d", broker, port)
        except Exception as exc:
            logger.error("MQTT connection failed: %s", exc)
            raise

    def _on_connect(self, client, userdata, flags, rc):
        codes = {0: "OK", 1: "Bad protocol", 2: "Client ID rejected",
                 3: "Server unavailable", 4: "Bad credentials", 5: "Not authorised"}
        logger.info("MQTT connected: %s", codes.get(rc, f"code {rc}"))
        if rc == 0:
            if self._status_state:
                self._status_state.mqtt_online = True
            client.subscribe(f"{self.base_topic}/set/#")
            logger.info("Subscribed to %s/set/#", self.base_topic)
            self.publish_discovery()

    def _on_disconnect(self, client, userdata, rc):
        if self._status_state:
            self._status_state.mqtt_online = False
        if rc != 0:
            logger.warning("MQTT unexpected disconnect (code %d), will auto-reconnect", rc)

    def _on_message(self, client, userdata, msg):
        try:
            parts = msg.topic.split("/")
            if len(parts) >= 2 and parts[-2] == "set":
                param     = parts[-1]
                value_str = msg.payload.decode("utf-8", errors="replace").strip()
                if self._command_queue is not None:
                    self._command_queue.put((param, value_str))
                    logger.info("Queued command: %s = %s", param, value_str)
        except Exception as exc:
            logger.error("Error handling MQTT message on %s: %s", msg.topic, exc)

    def publish_discovery(self):
        """Publish retained MQTT auto-discovery configs for all entities."""
        addr_clean = self._device_address.replace(":", "").upper() if self._device_address else "UNKNOWN"
        device = {
            "identifiers":  [f"bluetti_ac500_{addr_clean}"],
            "name":         "Bluetti AC500",
            "model":        "AC500",
            "manufacturer": "Bluetti",
        }
        if self._device_address:
            device["connections"] = [["bluetooth", self._device_address.upper()]]

        avail = {
            "topic":               f"{self.base_topic}/availability",
            "payload_available":   "online",
            "payload_not_available": "offline",
        }

        count = 0
        for ent in DISCOVERY_ENTITIES:
            domain    = ent["domain"]
            unique_id = ent["unique_id"]

            payload = {k: v for k, v in ent.items() if k != "domain"}
            payload = _sub_base(payload, self.base_topic)
            payload["device"]       = device
            payload["availability"] = [avail]

            disc_topic = f"homeassistant/{domain}/{unique_id}/config"
            self.client.publish(disc_topic, json.dumps(payload), retain=True, qos=1)
            count += 1

        self.client.publish(f"{self.base_topic}/availability", "online", retain=True, qos=1)
        logger.info("Published %d MQTT auto-discovery configs + availability=online", count)

    def publish_state(self, state: DeviceState):
        payload = {k: v for k, v in asdict(state).items() if v is not None}

        self.client.publish(f"{self.base_topic}/state", json.dumps(payload), retain=True)

        for key, value in payload.items():
            self.client.publish(
                f"{self.base_topic}/{key}",
                str(value).lower() if isinstance(value, bool) else str(value),
                retain=True,
            )

        logger.info("Published %d fields -> %s/#", len(payload), self.base_topic)

    def stop(self):
        self.client.publish(f"{self.base_topic}/availability", "offline", retain=True, qos=1)
        self.client.loop_stop()
        self.client.disconnect()


# -- BLE client ----------------------------------------------------------------
class BluettiBLEClient:
    BLE_LINK_STATUS_INIT     = 0
    BLE_LINK_STATUS_AUTH_1   = 1
    BLE_LINK_STATUS_AUTH_2   = 2
    BLE_LINK_STATUS_CHECK_SN = 3
    BLE_LINK_STATUS_COMPLETE = 4

    def __init__(self, address: str, mqtt_pub: MQTTPublisher, poll_interval: int = 30,
                 status_state: Optional[StatusState] = None):
        self.address         = address
        self.mqtt            = mqtt_pub
        self.poll_interval   = poll_interval
        self._status_state   = status_state
        self._command_queue  = stdlib_queue.Queue()

        self._response_buf   = bytearray()
        self._response_event = asyncio.Event()
        self._link_status    = self.BLE_LINK_STATUS_INIT
        self._auth_event     = asyncio.Event()

        self._crypt = bluetti_crypt.BluettiCrypt() if ENCRYPTION_AVAILABLE else None

    def _on_notify(self, sender, raw: bytearray):
        if not ENCRYPTION_AVAILABLE:
            self._accumulate_data(raw)
            return

        status_ref = [self._link_status]
        encrypted_response = self._crypt.ble_crypt_link_handler(bytes(raw), status_ref)
        new_status = status_ref[0]

        if new_status != self._link_status:
            logger.info("Encryption handshake: %d -> %d", self._link_status, new_status)
            self._link_status = new_status

        if new_status == self.BLE_LINK_STATUS_COMPLETE:
            if encrypted_response:
                asyncio.ensure_future(self._send_raw(encrypted_response))
            self._auth_event.set()
        elif new_status < self.BLE_LINK_STATUS_COMPLETE:
            if encrypted_response:
                asyncio.ensure_future(self._send_raw(encrypted_response))
        else:
            decrypted = self._crypt.decrypt_data(bytes(raw))
            if decrypted:
                self._accumulate_data(bytearray(decrypted))

    def _accumulate_data(self, data: bytearray):
        self._response_buf.extend(data)
        if len(self._response_buf) < 2:
            return
        fc = self._response_buf[1]
        if fc == 0x03:
            if len(self._response_buf) >= 3:
                expected = self._response_buf[2] + 5
                if len(self._response_buf) >= expected:
                    self._response_event.set()
        elif fc in (0x06, 0x10):
            if len(self._response_buf) >= 8:
                self._response_event.set()
        else:
            if len(self._response_buf) >= 8:
                self._response_event.set()

    async def _send_raw(self, data: bytes):
        if hasattr(self, "_ble_client") and self._ble_client.is_connected:
            await self._ble_client.write_gatt_char(WRITE_UUID, data, response=False)

    async def _send_query(self, data: bytes) -> bytes:
        self._response_buf.clear()
        self._response_event.clear()
        to_send = self._crypt.encrypt_data(data) if self._crypt else data
        await self._send_raw(to_send)
        try:
            await asyncio.wait_for(self._response_event.wait(), timeout=8.0)
            return bytes(self._response_buf)
        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for device response")
            return b""

    async def _do_auth_handshake(self) -> bool:
        if not ENCRYPTION_AVAILABLE:
            logger.info("Skipping encryption handshake (library not installed)")
            return True
        logger.info("Starting Bluetti encryption handshake...")
        try:
            await asyncio.wait_for(self._auth_event.wait(), timeout=15.0)
            logger.info("Encryption handshake complete")
            return True
        except asyncio.TimeoutError:
            logger.error("Encryption handshake timed out. Check bluetti_device_licence.csv.")
            return False

    async def _execute_write(self, param: str, value_str: str):
        if param not in _WRITABLE_BY_NAME:
            logger.warning("Unknown writable parameter: '%s'", param)
            return
        register, dtype, value_map = _WRITABLE_BY_NAME[param]
        v_lower = value_str.strip().lower()
        try:
            if dtype == "bool":
                val = 1 if v_lower in ("1", "on", "true") else 0
            elif dtype == "enum" and value_map:
                val = value_map[v_lower] if v_lower in value_map else int(value_str)
            else:
                val = max(0, min(65535, int(value_str)))
        except (ValueError, KeyError) as exc:
            logger.error("Invalid value '%s' for %s: %s", value_str, param, exc)
            return
        cmd     = build_modbus_write(register, val)
        to_send = self._crypt.encrypt_data(cmd) if self._crypt else cmd
        logger.info("BLE WRITE: %s = %s  (reg %d, val %d)", param, value_str, register, val)
        await self._send_raw(to_send)
        await asyncio.sleep(0.6)

    async def run(self):
        while True:
            try:
                await self._connect_and_poll()
            except Exception as exc:
                logger.error("BLE error: %s -- retrying in 30s", exc, exc_info=True)
                if self._status_state:
                    self._status_state.ble_connected = False
                await asyncio.sleep(30)

    async def _connect_and_poll(self):
        logger.info("Scanning for %s ...", self.address)
        device = await BleakScanner.find_device_by_address(self.address, timeout=30.0)
        if device is None:
            raise Exception(f"Device {self.address} not found during scan — is AC500 on and in range?")
        logger.info("Found device: %s (%s)", device.name, device.address)
        async with BleakClient(device, timeout=20.0) as client:
            self._ble_client  = client
            self._link_status = self.BLE_LINK_STATUS_INIT
            self._auth_event.clear()
            if self._status_state:
                self._status_state.ble_connected = True

            logger.info("Connected. Subscribing to notifications ...")
            await client.start_notify(NOTIFY_UUID, self._on_notify)

            if not await self._do_auth_handshake():
                return

            logger.info("Starting data polling every %ds", self.poll_interval)
            while client.is_connected:
                # Drain any pending write commands first
                while not self._command_queue.empty():
                    try:
                        param, value_str = self._command_queue.get_nowait()
                        await self._execute_write(param, value_str)
                    except stdlib_queue.Empty:
                        break

                # Poll all main register pages
                all_registers = {}
                for (start_addr, count, label) in QUERY_RANGES:
                    raw_query = build_modbus_query(start_addr, count)
                    raw_resp  = await self._send_query(raw_query)
                    if raw_resp:
                        parsed = parse_modbus_response(raw_resp, start_addr)
                        logger.debug("Page '%s': %d registers", label, len(parsed))
                        all_registers.update(parsed)
                    await asyncio.sleep(0.3)

                if all_registers:
                    state = registers_to_state(all_registers)

                    # Per-pack polling: write reg 3006 to select pack, then read 91-127.
                    # Registers 98 (voltage) and 99 (percent) hold the selected pack's data.
                    # Always iterate all PACK_NUM_MAX slots — reg 96 is the currently-selected
                    # pack number, not the connected count; empty slots return 0 V.
                    for n in range(1, PACK_NUM_MAX + 1):
                        # Select pack n
                        sel_cmd = build_modbus_write(3006, n)
                        to_send = self._crypt.encrypt_data(sel_cmd) if self._crypt else sel_cmd
                        await self._send_raw(to_send)
                        await asyncio.sleep(PACK_SWITCH_DELAY)

                        # Read pack registers
                        pack_resp = await self._send_query(
                            build_modbus_query(PACK_QUERY_START, PACK_QUERY_COUNT)
                        )
                        if pack_resp:
                            pr = parse_modbus_response(pack_resp, PACK_QUERY_START)
                            voltage = round(pr[98] * 0.01, 2) if 98 in pr else None
                            percent = pr.get(99)
                            # Treat < 5 V as empty slot (noise reads ~3.3 V on disconnected packs)
                            if voltage is not None and voltage < 5.0:
                                voltage = None
                                percent = None
                            setattr(state, f"pack_{n}_voltage", voltage)
                            setattr(state, f"pack_{n}_percent", percent)
                            logger.info("Pack %d: %s V  %s%%", n,
                                        f"{voltage:.2f}" if voltage else "---", percent or "---")

                    # Override pack_num with the actual count of connected packs
                    # (reg 96 reflects the last-selected pack, not the connected count)
                    state.pack_num = sum(
                        1 for n in range(1, PACK_NUM_MAX + 1)
                        if getattr(state, f"pack_{n}_voltage", None) is not None
                    )

                    self.mqtt.publish_state(state)
                    if self._status_state:
                        self._status_state.update(state)

                await asyncio.sleep(self.poll_interval)


# -- Scan helper ---------------------------------------------------------------
async def scan_for_bluetti():
    logger.info("Scanning for Bluetti devices (10 s) ...")
    devices = await BleakScanner.discover(timeout=10.0)
    found = [d for d in devices if d.name and
             ("AC500" in d.name or "BLUETTI" in d.name.upper())]
    if not found:
        logger.info("No Bluetti devices found. All discovered:")
        for d in devices:
            logger.info("  %s -- %s", d.address, d.name or "<no name>")
    else:
        logger.info("Found %d Bluetti device(s):", len(found))
        for d in found:
            logger.info("  %-22s  %s", d.address, d.name)


# -- Config file ---------------------------------------------------------------
_CONFIG_PATHS = [
    "/config/config.yaml",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "config.yaml"),
]

def load_config() -> dict:
    for path in _CONFIG_PATHS:
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            logger.info("Loaded config from %s", path)
            return cfg
    return {}


# -- CLI -----------------------------------------------------------------------
def parse_args(cfg: dict):
    ble        = cfg.get("ble",         {})
    mqtt_cfg   = cfg.get("mqtt",        {})
    log        = cfg.get("logging",     {})
    status_cfg = cfg.get("status_page", {})

    p = argparse.ArgumentParser(
        description="Bluetti AC500 BLE -> MQTT bridge (v5 - multi-pack B300 support)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--scan",     action="store_true")
    p.add_argument("--address",  default=ble.get("address"))
    p.add_argument("--broker",   default=mqtt_cfg.get("broker", "localhost"))
    p.add_argument("--port",     type=int, default=mqtt_cfg.get("port", 1883))
    p.add_argument("--username", default=mqtt_cfg.get("username", ""))
    p.add_argument("--password", default=mqtt_cfg.get("password", ""))
    p.add_argument("--topic",    default=mqtt_cfg.get("topic", "bluetti/ac500"))
    p.add_argument("--interval", type=int, default=ble.get("interval", 30))
    p.add_argument("--debug",    action="store_true",
                   default=(log.get("level", "INFO").upper() == "DEBUG"))
    # Web status page (v4)
    p.add_argument("--status-page",    action="store_true",
                   default=status_cfg.get("enabled", False),
                   help="Enable web status page")
    p.add_argument("--status-host",    default=status_cfg.get("host", "0.0.0.0"),
                   help="Status page bind address")
    p.add_argument("--status-port",    type=int, default=status_cfg.get("port", 8273),
                   help="Status page HTTP port")
    p.add_argument("--status-refresh", type=int, default=status_cfg.get("refresh", 10),
                   help="Browser auto-refresh interval in seconds")
    p.add_argument("--status-history", type=int, default=status_cfg.get("history", 100),
                   help="Max chart data points kept in memory")
    return p.parse_args()


def main():
    cfg  = load_config()
    args = parse_args(cfg)

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.scan:
        asyncio.run(scan_for_bluetti())
        sys.exit(0)

    if not args.address:
        logger.error("No BLE address. Set 'ble.address' in config.yaml or use --address.")
        sys.exit(1)

    if args.interval < 10:
        logger.warning("Polling interval < 10s may cause BLE issues. Using --interval 10+.")

    # Shared live-state object (read by web server, written by BLE + MQTT callbacks)
    status_state = StatusState()
    status_state.history = collections.deque(maxlen=args.status_history)

    # BLE client owns the command queue
    ble_client = BluettiBLEClient(
        address=args.address,
        mqtt_pub=None,   # set below
        poll_interval=args.interval,
        status_state=status_state,
    )

    # MQTT publisher receives the command queue and device address for discovery
    publisher = MQTTPublisher(
        broker=args.broker,
        port=args.port,
        username=args.username,
        password=args.password,
        base_topic=args.topic,
        command_queue=ble_client._command_queue,
        device_address=args.address,
        status_state=status_state,
    )
    ble_client.mqtt = publisher

    async def _amain():
        status_server = None
        if args.status_page:
            status_server = StatusServer(
                status_state  = status_state,
                command_queue = ble_client._command_queue,
                host          = args.status_host,
                port          = args.status_port,
                refresh       = args.status_refresh,
                device_name   = args.address,
            )
            await status_server.start()
        try:
            await ble_client.run()
        finally:
            if status_server:
                await status_server.stop()

    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        logger.info("Interrupted -- shutting down.")
    finally:
        publisher.stop()


if __name__ == "__main__":
    main()
