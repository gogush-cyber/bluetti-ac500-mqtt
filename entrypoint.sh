#!/bin/bash
set -e

echo "[entrypoint] Starting D-Bus system daemon..."
mkdir -p /run/dbus
rm -f /run/dbus/pid          # remove stale PID from previous container run
dbus-daemon --system --fork
sleep 1

echo "[entrypoint] Resetting USB Bluetooth adapter..."
# usbreset resets the adapter firmware — prevents frozen-but-UP state after container restart
if command -v usbreset &>/dev/null; then
    usbreset "$(lsusb | grep -i bluetooth | awk '{print $2"/"$4}' | tr -d ':')" 2>/dev/null || true
else
    # Fallback: toggle hci0 down/up to flush firmware state
    hciconfig hci0 down 2>/dev/null || true
    sleep 1
fi

echo "[entrypoint] Unblocking Bluetooth via rfkill..."
rfkill unblock bluetooth 2>/dev/null || true

echo "[entrypoint] Starting BlueZ daemon..."
bluetoothd --nodetach &
BLUEZ_PID=$!
sleep 3

echo "[entrypoint] Bluetooth adapter status:"
hciconfig 2>/dev/null || echo "  hciconfig not ready yet"

echo "[entrypoint] Bringing up hci0..."
hciconfig hci0 up 2>/dev/null && echo "  hci0 UP" || echo "  hci0 not found — is USB adapter plugged in?"

echo "[entrypoint] Starting Bluetti MQTT bridge..."
exec python -u /app/bluetti_ac500_mqtt.py "$@"
