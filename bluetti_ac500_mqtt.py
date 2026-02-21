"""
Bluetti AC500 -> MQTT Bridge  (v3)
====================================
v3 adds MQTT auto-discovery: the bridge self-registers all entities with
Home Assistant via homeassistant/<domain>/<unique_id>/config topics.
No manual yaml configuration in HA is needed.

v2 features retained:
  - Bidirectional control via bluetti/ac500/set/<param>
  - 8 writable parameters (AC/DC output, grid charge, UPS mode, etc.)

v3 new features:
  - MQTT auto-discovery for all 19 entities (sensors, switches, selects, numbers)
  - Device grouping in HA (all entities under one "Bluetti AC500" device)
  - Availability topic (online/offline with LWT)

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
import json
import logging
import os
import queue as stdlib_queue
import struct
import sys
from dataclasses import dataclass, asdict
from typing import Optional

import crcmod
import yaml
from bleak import BleakClient, BleakScanner
import paho.mqtt.client as mqtt

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
    96: ("pack_num",               1,     ""),
    98: ("pack_voltage",           0.01,  "V"),
    99: ("pack_battery_percent",   1,     "%"),
    # Settings (writable + readable for state feedback)
    3001: ("ups_mode",            1,  ""),
    3011: ("grid_charge_on",      1,  "bool"),
    3013: ("time_control_on",     1,  "bool"),
    3015: ("battery_range_start", 1,  ""),
    3016: ("battery_range_end",   1,  ""),
    3061: ("auto_sleep_mode",     1,  ""),
}

QUERY_RANGES = [
    (10,   40, "main"),
    (70,   21, "details"),
    (91,   37, "battery_pack"),
    (3001, 16, "settings"),
    (3061,  1, "sleep"),
]

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

    {"domain": "sensor", "unique_id": "bluetti_ac500_pack_voltage",
     "name": "AC500 Pack Voltage",
     "state_topic": "{base}/state", "value_template": "{{ value_json.pack_voltage }}",
     "unit_of_measurement": "V", "device_class": "voltage", "state_class": "measurement"},

    {"domain": "sensor", "unique_id": "bluetti_ac500_pack_battery_percent",
     "name": "AC500 Pack Battery Percent",
     "state_topic": "{base}/state",
     "value_template": "{{ value_json.pack_battery_percent }}",
     "unit_of_measurement": "%", "device_class": "battery", "state_class": "measurement"},

    {"domain": "sensor", "unique_id": "bluetti_ac500_pack_num",
     "name": "AC500 Pack Number",
     "state_topic": "{base}/state", "value_template": "{{ value_json.pack_num }}"},

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
    pack_num:              Optional[int]   = None
    pack_voltage:          Optional[float] = None
    pack_battery_percent:  Optional[float] = None
    ups_mode:              Optional[int]   = None
    grid_charge_on:        Optional[bool]  = None
    time_control_on:       Optional[bool]  = None
    battery_range_start:   Optional[int]   = None
    battery_range_end:     Optional[int]   = None
    auto_sleep_mode:       Optional[int]   = None


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


# -- MQTT publisher ------------------------------------------------------------
class MQTTPublisher:
    def __init__(self, broker: str, port: int, username: str, password: str,
                 base_topic: str = "bluetti/ac500",
                 command_queue: Optional[stdlib_queue.Queue] = None,
                 device_address: str = ""):
        self.base_topic       = base_topic
        self._command_queue   = command_queue
        self._device_address  = device_address
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
            client.subscribe(f"{self.base_topic}/set/#")
            logger.info("Subscribed to %s/set/#", self.base_topic)
            self.publish_discovery()

    def _on_disconnect(self, client, userdata, rc):
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

    def __init__(self, address: str, mqtt_pub: MQTTPublisher, poll_interval: int = 30):
        self.address         = address
        self.mqtt            = mqtt_pub
        self.poll_interval   = poll_interval
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

                # Poll all register pages
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
                    self.mqtt.publish_state(state)

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
    ble      = cfg.get("ble",     {})
    mqtt_cfg = cfg.get("mqtt",    {})
    log      = cfg.get("logging", {})

    p = argparse.ArgumentParser(
        description="Bluetti AC500 BLE -> MQTT bridge (v3 - auto-discovery)",
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

    # BLE client owns the command queue
    ble_client = BluettiBLEClient(
        address=args.address,
        mqtt_pub=None,   # set below
        poll_interval=args.interval,
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
    )
    ble_client.mqtt = publisher

    try:
        asyncio.run(ble_client.run())
    except KeyboardInterrupt:
        logger.info("Interrupted -- shutting down.")
    finally:
        publisher.stop()


if __name__ == "__main__":
    main()
