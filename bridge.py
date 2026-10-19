#!/usr/bin/env python3
"""UART <-> pbl-api bridge.

Reads rainfall measurements from a serial port, forwards them to the pbl-api
REST API, and polls the API for sensor-cleaning commands, forwarding their
payload over the serial port.

Configuration is read from environment variables (or a .env file).
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
import serial
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("uart-bridge")


@dataclass(frozen=True)
class Config:
    api_url: str
    api_key: str
    serial_port: str
    serial_baud: int
    poll_interval: float
    measurements_queue: Path
    pending_acks: Path
    http_timeout: float


class ApiResult:
    """Outcome of an API call: ok flag + whether it is worth retrying."""

    __slots__ = ("ok", "retryable", "detail")

    def __init__(self, ok: bool, retryable: bool, detail: str = "") -> None:
        self.ok = ok
        self.retryable = retryable
        self.detail = detail


class ApiClient:
    def __init__(self, base_url: str, api_key: str, timeout: float) -> None:
        self.base = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"X-API-Key": api_key, "Content-Type": "application/json"})
        self.timeout = timeout

    def post_measurement(self, sensor_id: str, value_mm: float, sent_at: int) -> ApiResult:
        url = f"{self.base}/api/v1/measurements"
        body = {"sensor_id": sensor_id, "value_mm": value_mm, "sent_at": sent_at}
        try:
            resp = self.session.post(url, json=body, timeout=self.timeout)
        except requests.RequestException as exc:
            return ApiResult(False, True, str(exc))
        if resp.status_code in (200, 201):
            return ApiResult(True, False)
        if resp.status_code == 409:
            # Conflict: treat as handled so we don't poison the queue forever.
            return ApiResult(True, False, "conflict")
        if resp.status_code == 429 or resp.status_code >= 500:
            return ApiResult(False, True, f"HTTP {resp.status_code}")
        # Other 4xx -> permanent (bad data); drop from queue.
        return ApiResult(False, False, f"HTTP {resp.status_code}: {resp.text[:200]}")

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
        if resp.status_code == 200 or resp.status_code == 409:
            return ApiResult(True, False)
        if resp.status_code == 429 or resp.status_code >= 500:
            return ApiResult(False, True, f"HTTP {resp.status_code}")
        return ApiResult(False, False, f"HTTP {resp.status_code}: {resp.text[:200]}")

    def reject_command(self, command_id: int, note: str) -> None:
        url = f"{self.base}/api/v1/commands/{command_id}/reject"
        try:
            self.session.post(url, json={"note": note}, timeout=self.timeout)
        except requests.RequestException as exc:
            log.warning("reject command %s failed: %s", command_id, exc)


class MeasurementQueue:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()

    def append(self, entry: dict[str, Any]) -> None:
        with self._lock, self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")

    def load(self) -> list[dict[str, Any]]:
        with self._lock:
            return self._read_unlocked()

    def _read_unlocked(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        out: list[dict[str, Any]] = []
        with self.path.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(obj, dict):
                    out.append(obj)
        return out

    def pop_all(self) -> list[dict[str, Any]]:
        """Atomically read and truncate the queue (used by the flush loop)."""
        with self._lock:
            entries = self._read_unlocked()
            with self.path.open("w", encoding="utf-8"):
                pass
            return entries

    def save(self, entries: list[dict[str, Any]]) -> None:
        with self._lock:
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            with tmp.open("w", encoding="utf-8") as fh:
                for entry in entries:
                    fh.write(json.dumps(entry) + "\n")
            tmp.replace(self.path)


class PendingAcks:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()

    def load(self) -> list[int]:
        with self._lock:
            if not self.path.exists():
                return []
            try:
                data = json.loads(self.path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                return []
            if not isinstance(data, list):
                return []
            return [int(x) for x in data if isinstance(x, int) or str(x).isdigit()]

    def save(self, ids: list[int]) -> None:
        with self._lock:
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text(json.dumps(sorted(set(ids))), encoding="utf-8")
            tmp.replace(self.path)


def parse_measurement_line(line: str) -> dict[str, Any] | None:
    """Parse 'sensor;value_mm;epoch'. Return None on any validation failure."""
    parts = [p.strip() for p in line.strip().split(";")]
    if len(parts) != 3:
        return None
    sensor_id, value_str, epoch_str = parts
    if not sensor_id or len(sensor_id) > 64:
        return None
    try:
        value = float(value_str)
    except ValueError:
        return None
    if value < 0 or value > 9999.99:
        return None
    if "." in value_str:
        decimals = value_str.split(".", 1)[1]
        if len(decimals) > 2:
            return None
    try:
        sent_at = int(epoch_str)
    except ValueError:
        return None
    if sent_at < 0:
        return None
    return {"sensor_id": sensor_id, "value_mm": value, "sent_at": sent_at}


def load_config() -> Config:
    def req(name: str) -> str:
        value = os.getenv(name, "").strip()
        if not value:
            raise SystemExit(f"Missing required environment variable: {name}")
        return value

    api_url = req("API_URL")
    api_key = req("API_KEY")
    serial_port = os.getenv("SERIAL_PORT", "/dev/serial0").strip()
    baud_raw = os.getenv("SERIAL_BAUD", "9600").strip()
    try:
        serial_baud = int(baud_raw)
    except ValueError as exc:
        raise SystemExit(f"Invalid SERIAL_BAUD: {baud_raw}") from exc
    interval_raw = os.getenv("POLL_INTERVAL_SEC", "30").strip()
    try:
        poll_interval = float(interval_raw)
    except ValueError as exc:
        raise SystemExit(f"Invalid POLL_INTERVAL_SEC: {interval_raw}") from exc
    if poll_interval <= 0:
        raise SystemExit("POLL_INTERVAL_SEC must be > 0")
    measurements_queue = Path(
        os.getenv("MEASUREMENTS_QUEUE_FILE", "measurements_queue.jsonl").strip()
    )
    pending_acks = Path(os.getenv("PENDING_ACKS_FILE", "pending_acks.json").strip())
    http_timeout_raw = os.getenv("HTTP_TIMEOUT_SEC", "10").strip()
    try:
        http_timeout = float(http_timeout_raw)
    except ValueError as exc:
        raise SystemExit(f"Invalid HTTP_TIMEOUT_SEC: {http_timeout_raw}") from exc

    return Config(
        api_url=api_url,
        api_key=api_key,
        serial_port=serial_port,
        serial_baud=serial_baud,
        poll_interval=poll_interval,
        measurements_queue=measurements_queue,
        pending_acks=pending_acks,
        http_timeout=http_timeout,
    )


def setup_logging(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def uart_reader(
    ser: serial.Serial,
    serial_lock: threading.Lock,
    api: ApiClient,
    queue: MeasurementQueue,
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
        if not raw:
            continue
        try:
            line = raw.decode("utf-8", errors="replace").strip()
        except UnicodeDecodeError:
            continue
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
            continue
        if result.retryable:
            log.warning("API unavailable, queued: %s (%s)", entry, result.detail)
            queue.append(entry)
        else:
            log.error("measurement rejected by API, dropped: %s (%s)", entry, result.detail)


def flush_measurements(api: ApiClient, queue: MeasurementQueue) -> None:
    entries = queue.pop_all()
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
            continue
        if result.retryable:
            remaining.append(entry)
        else:
            log.error("dropping queued measurement (permanent): %s (%s)", entry, result.detail)
    for entry in remaining:
        queue.append(entry)
    if sent:
        log.info("flushed %d queued measurement(s); %d remain", sent, len(remaining))


def flush_pending_acks(api: ApiClient, pending: PendingAcks) -> None:
    ids = pending.load()
    if not ids:
        return
    remaining: list[int] = []
    for cid in ids:
        result = api.ack_command(cid)
        if not result.ok and result.retryable:
            remaining.append(cid)
        elif not result.ok:
            log.error("dropping pending ack %s (%s)", cid, result.detail)
    if remaining != ids:
        pending.save(remaining)


def process_commands(
    api: ApiClient,
    ser: serial.Serial,
    serial_lock: threading.Lock,
    pending: PendingAcks,
) -> None:
    commands = api.get_pending_commands()
    if not commands:
        return
    for cmd in commands:
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
            ids = pending.load()
            ids.append(cid)
            pending.save(ids)
        else:
            log.error("ack rejected for command %s: %s", cid, result.detail)


def main() -> None:
    config = load_config()
    setup_logging(os.getenv("LOG_LEVEL", "info"))
    log.info(
        "starting uart-bridge: port=%s baud=%s interval=%ss",
        config.serial_port,
        config.serial_baud,
        config.poll_interval,
    )

    try:
        ser = serial.Serial(config.serial_port, config.serial_baud, timeout=1)
    except serial.SerialException as exc:
        log.error("cannot open serial port %s: %s", config.serial_port, exc)
        raise SystemExit(1) from exc

    serial_lock = threading.Lock()
    queue = MeasurementQueue(config.measurements_queue)
    pending = PendingAcks(config.pending_acks)
    api = ApiClient(config.api_url, config.api_key, config.http_timeout)

    stop_event = threading.Event()

    def handle_signal(signum: int, _frame: Any) -> None:
        log.info("received signal %s, shutting down", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    reader = threading.Thread(
        target=uart_reader,
        args=(ser, serial_lock, api, queue, stop_event),
        name="uart-reader",
        daemon=True,
    )
    reader.start()

    try:
        while not stop_event.is_set():
            flush_measurements(api, queue)
            flush_pending_acks(api, pending)
            process_commands(api, ser, serial_lock, pending)
            stop_event.wait(config.poll_interval)
    finally:
        stop_event.set()
        reader.join(timeout=5)
        with contextlib.suppress(serial.SerialException):
            ser.close()
        log.info("uart-bridge stopped")


if __name__ == "__main__":
    main()
