#!/usr/bin/env python3
"""UART <-> pbl-api bridge.

Reads rainfall measurements from a serial port, forwards them to the pbl-api
REST API, and polls the API for sensor-cleaning commands, forwarding their
payload over the serial port.

Configuration is read from environment variables (or a .env file).
"""

import contextlib
import json
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import requests
import serial
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("uart-bridge")
storage_lock = threading.Lock()


@dataclass(frozen=True)
class ApiResult:
    """Outcome of an API call: ok flag + whether it is worth retrying."""

    ok: bool
    retryable: bool = False
    detail: str = ""


class ApiClient:
    def __init__(self, base_url: str, api_key: str, timeout: float) -> None:
        self.base = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"X-API-Key": api_key, "Content-Type": "application/json"})
        self.timeout = timeout

    def _result(self, resp: requests.Response, ok_codes: tuple[int, ...]) -> ApiResult:
        if resp.status_code in ok_codes:
            return ApiResult(True)
        if resp.status_code == 429 or resp.status_code >= 500:
            return ApiResult(False, True, f"HTTP {resp.status_code}")
        return ApiResult(False, False, f"HTTP {resp.status_code}: {resp.text[:200]}")

    def post_measurement(self, sensor_id: str, value_mm: float, sent_at: int) -> ApiResult:
        url = f"{self.base}/api/v1/measurements"
        body = {"sensor_id": sensor_id, "value_mm": value_mm, "sent_at": sent_at}
        try:
            resp = self.session.post(url, json=body, timeout=self.timeout)
        except requests.RequestException as exc:
            return ApiResult(False, True, str(exc))
        # 409 means the API already has this measurement.
        return self._result(resp, (200, 201, 409))

    def get_pending_commands(self) -> list[dict[str, Any]]:
        url = f"{self.base}/api/v1/commands"
        try:
            resp = self.session.get(
                url, params={"status": "PENDING", "limit": 100}, timeout=self.timeout
            )
        except requests.RequestException as exc:
            log.warning("poll commands failed: %s", exc)
            return []
        if resp.status_code != 200:
            log.warning("poll commands HTTP %s", resp.status_code)
            return []
        data = resp.json()
        items = data.get("items", []) if isinstance(data, dict) else []
        return [c for c in items if isinstance(c, dict)]

    def ack_command(self, command_id: int) -> ApiResult:
        url = f"{self.base}/api/v1/commands/{command_id}/ack"
        try:
            resp = self.session.post(url, timeout=self.timeout)
        except requests.RequestException as exc:
            return ApiResult(False, True, str(exc))
        return self._result(resp, (200, 409))

    def reject_command(self, command_id: int, note: str) -> None:
        url = f"{self.base}/api/v1/commands/{command_id}/reject"
        try:
            self.session.post(url, json={"note": note}, timeout=self.timeout)
        except requests.RequestException as exc:
            log.warning("reject command %s failed: %s", command_id, exc)


def append_measurement(path: Path, entry: dict[str, Any]) -> None:
    with storage_lock, path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")


def pop_measurements(path: Path) -> list[dict[str, Any]]:
    """Read and truncate the queue."""
    with storage_lock:
        if not path.exists():
            return []
        lines = path.read_text(encoding="utf-8").splitlines()
        path.write_text("", encoding="utf-8")

    entries: list[dict[str, Any]] = []
    for line in lines:
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(entry, dict):
            entries.append(entry)
    return entries


def load_acks(path: Path) -> list[int]:
    with storage_lock:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []
    if not isinstance(data, list):
        return []
    return [int(x) for x in data if isinstance(x, int) or str(x).isdigit()]


def save_acks(path: Path, ids: list[int]) -> None:
    with storage_lock:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(sorted(set(ids))), encoding="utf-8")
        tmp.replace(path)


def parse_measurement_line(line: str) -> dict[str, Any] | None:
    """Parse 'sensor: value_mm;epoch[;]'. Return None on any validation failure."""
    sensor_id, sep, rest = line.partition(":")
    sensor_id = sensor_id.strip()
    if not sep or not sensor_id or len(sensor_id) > 64:
        return None

    parts = [p.strip() for p in rest.strip().rstrip(";").split(";")]
    if len(parts) != 2:
        return None
    value_str, epoch_str = parts

    try:
        value = float(value_str)
        sent_at = int(epoch_str)
    except ValueError:
        return None
    if value < 0 or value > 9999.99 or sent_at < 0:
        return None
    if "." in value_str and len(value_str.split(".", 1)[1]) > 2:
        return None
    return {"sensor_id": sensor_id, "value_mm": value, "sent_at": sent_at}


def env(name: str, default: str | None = None, cast=str):
    raw = os.getenv(name, default)
    if raw is None or not raw.strip():
        if default is None:
            raise SystemExit(f"Missing required environment variable: {name}")
        raw = default
    try:
        return cast(raw.strip())
    except ValueError as exc:
        raise SystemExit(f"Invalid {name}: {raw}") from exc


def load_config() -> SimpleNamespace:
    poll_interval = env("POLL_INTERVAL_SEC", "30", float)
    if poll_interval <= 0:
        raise SystemExit("POLL_INTERVAL_SEC must be > 0")

    return SimpleNamespace(
        api_url=env("API_URL"),
        api_key=env("API_KEY"),
        serial_port=env("SERIAL_PORT", "/dev/serial0"),
        serial_baud=env("SERIAL_BAUD", "9600", int),
        poll_interval=poll_interval,
        measurements_queue=Path(env("MEASUREMENTS_QUEUE_FILE", "measurements_queue.jsonl")),
        pending_acks=Path(env("PENDING_ACKS_FILE", "pending_acks.json")),
        http_timeout=env("HTTP_TIMEOUT_SEC", "10", float),
    )


def uart_reader(
    ser: serial.Serial,
    serial_lock: threading.Lock,
    api: ApiClient,
    queue_path: Path,
    stop_event: threading.Event,
) -> None:
    while not stop_event.is_set():
        try:
            with serial_lock:
                raw = ser.readline()
        except serial.SerialException as exc:
            log.error("serial read error: %s", exc)
            time.sleep(2)
            continue

        line = raw.decode("utf-8", errors="replace").strip()
        if not line:
            continue
        entry = parse_measurement_line(line)
        if entry is None:
            log.warning("invalid measurement line ignored: %r", line)
            continue

        result = api.post_measurement(entry["sensor_id"], entry["value_mm"], entry["sent_at"])
        if result.ok:
            log.info(
                "measurement sent: %s %.2f @ %s",
                entry["sensor_id"],
                entry["value_mm"],
                entry["sent_at"],
            )
        elif result.retryable:
            log.warning("API unavailable, queued: %s (%s)", entry, result.detail)
            append_measurement(queue_path, entry)
        else:
            log.error("measurement rejected by API, dropped: %s (%s)", entry, result.detail)


def flush_measurements(api: ApiClient, queue_path: Path) -> None:
    entries = pop_measurements(queue_path)
    if not entries:
        return
    remaining: list[dict[str, Any]] = []
    sent = 0
    for entry in entries:
        result = api.post_measurement(
            entry.get("sensor_id", ""),
            float(entry.get("value_mm", 0)),
            int(entry.get("sent_at", 0)),
        )
        if result.ok:
            sent += 1
        elif result.retryable:
            remaining.append(entry)
        else:
            log.error("dropping queued measurement (permanent): %s (%s)", entry, result.detail)
    for entry in remaining:
        append_measurement(queue_path, entry)
    if sent:
        log.info("flushed %d queued measurement(s); %d remain", sent, len(remaining))


def flush_pending_acks(api: ApiClient, acks_path: Path) -> None:
    ids = load_acks(acks_path)
    if not ids:
        return
    remaining = []
    for cid in ids:
        result = api.ack_command(cid)
        if result.ok:
            continue
        if result.retryable:
            remaining.append(cid)
        else:
            log.error("dropping pending ack %s (%s)", cid, result.detail)
    if remaining != ids:
        save_acks(acks_path, remaining)


def process_commands(
    api: ApiClient,
    ser: serial.Serial,
    serial_lock: threading.Lock,
    acks_path: Path,
) -> None:
    for cmd in api.get_pending_commands():
        cid = cmd.get("id")
        if not isinstance(cid, int):
            continue
        payload = cmd.get("payload")
        if not isinstance(payload, str) or not payload:
            log.warning("command %s has no payload, skipping", cid)
            continue
        text = payload if payload.endswith("\n") else payload + "\n"
        try:
            with serial_lock:
                ser.write(text.encode("utf-8"))
                ser.flush()
        except serial.SerialException as exc:
            log.error("serial write failed for command %s: %s", cid, exc)
            api.reject_command(cid, "serial write failed")
            continue
        result = api.ack_command(cid)
        if result.ok:
            log.info("command %s acknowledged (payload=%r)", cid, payload)
        elif result.retryable:
            log.warning("ack failed for command %s, queued", cid)
            ids = load_acks(acks_path)
            ids.append(cid)
            save_acks(acks_path, ids)
        else:
            log.error("ack rejected for command %s: %s", cid, result.detail)


def main() -> None:
    config = load_config()
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "info").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log.info(
        "starting uart-bridge: port=%s baud=%s interval=%ss",
        config.serial_port, config.serial_baud, config.poll_interval,
    )

    try:
        ser = serial.Serial(config.serial_port, config.serial_baud, timeout=1)
    except serial.SerialException as exc:
        log.error("cannot open serial port %s: %s", config.serial_port, exc)
        raise SystemExit(1) from exc

    serial_lock = threading.Lock()
    api = ApiClient(config.api_url, config.api_key, config.http_timeout)

    stop_event = threading.Event()

    def handle_signal(signum: int, _frame: Any) -> None:
        log.info("received signal %s, shutting down", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    reader = threading.Thread(
        target=uart_reader, args=(ser, serial_lock, api, config.measurements_queue, stop_event),
        name="uart-reader", daemon=True,
    )
    reader.start()

    try:
        while not stop_event.is_set():
            flush_measurements(api, config.measurements_queue)
            flush_pending_acks(api, config.pending_acks)
            process_commands(api, ser, serial_lock, config.pending_acks)
            stop_event.wait(config.poll_interval)
    finally:
        stop_event.set()
        reader.join(timeout=5)
        with contextlib.suppress(serial.SerialException):
            ser.close()
        log.info("uart-bridge stopped")


if __name__ == "__main__":
    main()
