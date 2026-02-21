FROM python:3.11-slim

# Install BlueZ, D-Bus and rfkill
RUN apt-get update && apt-get install -y --no-install-recommends \
        bluetooth \
        bluez \
        dbus \
        libdbus-1-dev \
        rfkill \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bluetti_ac500_mqtt.py .
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# /config is the mount point for the host config volume
VOLUME /config

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/entrypoint.sh"]
