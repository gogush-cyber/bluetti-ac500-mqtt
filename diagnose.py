"""
Diagnostic script — prints raw register hex + parsed values from AC500.
Run: python diagnose.py --address AA:BB:CC:DD:EE:FF
"""
import asyncio
import argparse
import struct
import crcmod
from bleak import BleakClient, BleakScanner

NOTIFY_UUID = "0000ff01-0000-1000-8000-00805f9b34fb"
WRITE_UUID  = "0000ff02-0000-1000-8000-00805f9b34fb"
crc16_modbus = crcmod.predefined.mkCrcFun("modbus")

AC500_REGISTERS = {
    0x00: ("dc_input_power",         1,    "W"),
    0x01: ("ac_input_power",         1,    "W"),
    0x02: ("ac_output_power",        1,    "W"),
    0x03: ("dc_output_power",        1,    "W"),
    0x04: ("power_generation_today", 0.1,  "kWh"),
    0x08: ("total_battery_percent",  1,    "%"),
    0x0A: ("ac_output_on",           1,    "bool"),
    0x0B: ("dc_output_on",           1,    "bool"),
    0x44: ("ac_input_voltage",       0.1,  "V"),
    0x45: ("ac_input_frequency",     0.1,  "Hz"),
    0x46: ("pack_num",               1,    ""),
    0x47: ("pack_battery_percent",   1,    "%"),
    0x48: ("pack_voltage",           0.1,  "V"),
    0x49: ("pack_temperature",       0.1,  "°C"),
}

QUERY_RANGES = [
    (0x00, 0x46, "main"),
    (0x46, 0x15, "battery_pack"),
]

def build_query(start, count):
    payload = struct.pack(">BBHH", 0x01, 0x03, start, count)
    return payload + struct.pack("<H", crc16_modbus(payload))

response_buf   = bytearray()
response_event = asyncio.Event()

def on_notify(sender, raw: bytearray):
    global response_buf
    response_buf.extend(raw)
    print(f"  [RAW notify {len(raw)} bytes] {raw.hex()}")
    if len(response_buf) >= 3:
        expected = response_buf[2] + 5
        if len(response_buf) >= expected:
            response_event.set()

async def send_query(client, data):
    global response_buf
    response_buf = bytearray()
    response_event.clear()
    await client.write_gatt_char(WRITE_UUID, data, response=False)
    try:
        await asyncio.wait_for(response_event.wait(), timeout=8.0)
        return bytes(response_buf)
    except asyncio.TimeoutError:
        print("  [TIMEOUT] No response received")
        return b""

def parse_response(data, base):
    if len(data) < 5:
        return {}
    body = data[:-2]
    crc_recv = struct.unpack("<H", data[-2:])[0]
    crc_calc = crc16_modbus(body)
    if crc_recv != crc_calc:
        print(f"  [CRC FAIL] recv={crc_recv:04X} calc={crc_calc:04X} — data is likely encrypted")
        return {}
    byte_count = data[2]
    regs = {}
    for i in range(byte_count // 2):
        regs[base + i] = struct.unpack(">H", data[3 + i*2: 5 + i*2])[0]
    return regs

async def run(address):
    print(f"\nConnecting to {address} ...")
    async with BleakClient(address, timeout=20.0) as client:
        print("Connected. Listing GATT services:")
        for svc in client.services:
            print(f"  Service: {svc.uuid}")
            for ch in svc.characteristics:
                print(f"    Char: {ch.uuid}  props={ch.properties}")

        await client.start_notify(NOTIFY_UUID, on_notify)
        print("\nSubscribed to notifications.\n")

        all_regs = {}
        for start, count, label in QUERY_RANGES:
            query = build_query(start, count)
            print(f"--- Querying page '{label}' (start=0x{start:02X}, count={count}) ---")
            print(f"  [QUERY] {query.hex()}")
            resp = await send_query(client, query)
            if resp:
                print(f"  [RESP ] {resp.hex()}")
                parsed = parse_response(resp, start)
                all_regs.update(parsed)
                print(f"  Parsed {len(parsed)} registers")
            await asyncio.sleep(0.5)

        print("\n=== RAW REGISTER VALUES ===")
        for offset in sorted(all_regs):
            val = all_regs[offset]
            if offset in AC500_REGISTERS:
                name, scale, unit = AC500_REGISTERS[offset]
                cooked = bool(val) if unit == "bool" else round(val * scale, 2)
                print(f"  0x{offset:02X}  {name:<30} raw={val:5d}  value={cooked} {unit}")
            else:
                print(f"  0x{offset:02X}  {'<unknown>':<30} raw={val:5d}")

        print("\nDone.")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--address", required=True)
    args = p.parse_args()
    asyncio.run(run(args.address))
