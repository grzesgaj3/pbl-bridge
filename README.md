# UART <-> pbl-api bridge

Python bridge for a Raspberry Pi 4 that forwards rainfall measurements read
from a serial port to the [pbl-api](https://github.com/grzesgaj3/pbl-api),
and polls the API for sensor-cleaning commands, forwarding their payload over
the serial port.

## Files

- `bridge.py` — the bridge
- `.env` — configuration (copy from `.env.example`)
- `measurements_queue.jsonl`, `pending_acks.json` — offline buffers (created at runtime)

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
# edit .env: API_URL, API_KEY (bridge key), SERIAL_PORT, ...
```

## Run

```bash
.venv/bin/python bridge.py
```

Stop with Ctrl+C. Logs go to stdout.

## Configuration (.env)

| Variable | Default | Description |
|----------|---------|-------------|
| `API_URL` | (required) | pbl-api base URL |
| `API_KEY` | (required) | bridge API key |
| `SERIAL_PORT` | `/dev/serial0` | serial device |
| `SERIAL_BAUD` | `9600` | baud rate |
| `POLL_INTERVAL_SEC` | `30` | queue flush + command poll interval |
| `MEASUREMENTS_QUEUE_FILE` | `measurements_queue.jsonl` | offline measurement buffer |
| `PENDING_ACKS_FILE` | `pending_acks.json` | commands awaiting ack retry |
| `HTTP_TIMEOUT_SEC` | `10` | HTTP timeout |
| `LOG_LEVEL` | `info` | logging level |

## UART protocol

Input (one line per reading):

```
sensor1;20;123456789
```

`sensor_id;value_mm (0..9999.99);epoch_seconds`. Invalid lines are logged and
ignored.

Output (cleaning command, sent by the API's payload):

```
sensor1;clean;
```
