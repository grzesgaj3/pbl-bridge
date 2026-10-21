import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests
import serial

from bridge import (
    ApiClient,
    ApiResult,
    append_measurement,
    flush_measurements,
    flush_pending_acks,
    load_acks,
    load_config,
    parse_measurement_line,
    pop_measurements,
    process_commands,
    save_acks,
    uart_reader,
)


def test_parse_valid_line():
    assert parse_measurement_line("sensor1: 20;123456789;") == {
        "sensor_id": "sensor1",
        "value_mm": 20.0,
        "sent_at": 123456789,
    }


def test_parse_accepts_line_without_trailing_semicolon():
    assert parse_measurement_line("sensor1: 20;123456789") == {
        "sensor_id": "sensor1",
        "value_mm": 20.0,
        "sent_at": 123456789,
    }


def test_parse_trims_fields():
    assert parse_measurement_line("  sensor1 : 3.4 ; 1729260000  ") == {
        "sensor_id": "sensor1",
        "value_mm": 3.4,
        "sent_at": 1729260000,
    }


def test_parse_allows_spaces_in_sensor_name():
    assert parse_measurement_line("sensor one: 2;1;") == {
        "sensor_id": "sensor one",
        "value_mm": 2.0,
        "sent_at": 1,
    }


def test_parse_rejects_old_semicolon_format():
    assert parse_measurement_line("sensor1;20;123456789") is None


def test_parse_rejects_wrong_field_count():
    assert parse_measurement_line("sensor1: 20") is None
    assert parse_measurement_line("sensor1: a;b;c") is None


def test_parse_rejects_empty_sensor():
    assert parse_measurement_line(": 20;1") is None


def test_parse_rejects_too_long_sensor():
    assert parse_measurement_line("x" * 65 + ": 1;1") is None


def test_parse_rejects_bad_value():
    assert parse_measurement_line("s: abc;1") is None
    assert parse_measurement_line("s: -1;1") is None
    assert parse_measurement_line("s: 10000;1") is None
    assert parse_measurement_line("s: 1.234;1") is None


def test_parse_rejects_non_plain_number_formats():
    for value in ("nan", "NaN", "-nan", "inf", "-inf", "1e3", "1_0", "+5", "1."):
        assert parse_measurement_line(f"s: {value};1") is None


def test_parse_rejects_bad_epoch():
    assert parse_measurement_line("s: 1;abc") is None
    assert parse_measurement_line("s: 1;-5") is None


def test_measurement_queue_roundtrip(tmp_path: Path):
    path = tmp_path / "q.jsonl"
    assert pop_measurements(path) == []
    append_measurement(path, {"sensor_id": "s", "value_mm": 1.0, "sent_at": 10})
    assert pop_measurements(path) == [{"sensor_id": "s", "value_mm": 1.0, "sent_at": 10}]
    assert pop_measurements(path) == []


def test_measurement_queue_skips_corrupt_lines(tmp_path: Path):
    path = tmp_path / "q.jsonl"
    path.write_text('{"a":1}\nnot json\n{"b":2}\n', encoding="utf-8")
    assert pop_measurements(path) == [{"a": 1}, {"b": 2}]
    assert path.read_text(encoding="utf-8") == ""


def test_pending_acks_roundtrip(tmp_path: Path):
    path = tmp_path / "acks.json"
    assert load_acks(path) == []
    save_acks(path, [3, 1, 3])
    assert load_acks(path) == [1, 3]


def test_pending_acks_handles_corrupt(tmp_path: Path):
    path = tmp_path / "acks.json"
    path.write_text("garbage", encoding="utf-8")
    assert load_acks(path) == []


def test_load_config_requires_api_url(monkeypatch):
    monkeypatch.delenv("API_URL", raising=False)
    monkeypatch.setenv("API_KEY", "key")
    with pytest.raises(SystemExit):
        load_config()


def test_load_config_defaults(monkeypatch):
    monkeypatch.setenv("API_URL", "http://example.test")
    monkeypatch.setenv("API_KEY", "key")
    for name in (
        "SERIAL_PORT",
        "SERIAL_BAUD",
        "POLL_INTERVAL_SEC",
        "MEASUREMENTS_QUEUE_FILE",
        "PENDING_ACKS_FILE",
        "HTTP_TIMEOUT_SEC",
    ):
        monkeypatch.delenv(name, raising=False)
    config = load_config()
    assert config.serial_port == "/dev/serial0"
    assert config.serial_baud == 9600
    assert config.poll_interval == 30.0
    assert config.measurements_queue == Path("measurements_queue.jsonl")
    assert config.pending_acks == Path("pending_acks.json")
    assert config.http_timeout == 10.0


def test_load_config_rejects_invalid_interval(monkeypatch):
    monkeypatch.setenv("API_URL", "http://example.test")
    monkeypatch.setenv("API_KEY", "key")
    monkeypatch.setenv("POLL_INTERVAL_SEC", "0")
    with pytest.raises(SystemExit):
        load_config()


class FakeApi:
    def __init__(self, post_results=(), ack_results=(), commands=()):
        self.post_results = list(post_results)
        self.ack_results = list(ack_results)
        self.commands = list(commands)
        self.posted = []
        self.acked = []
        self.rejected = []

    def post_measurement(self, sensor_id, value_mm, sent_at):
        self.posted.append((sensor_id, value_mm, sent_at))
        return self.post_results.pop(0)

    def ack_command(self, command_id):
        self.acked.append(command_id)
        return self.ack_results.pop(0)

    def get_pending_commands(self):
        return self.commands

    def reject_command(self, command_id, note):
        self.rejected.append((command_id, note))


class OneShotStop:
    def __init__(self):
        self.calls = 0

    def is_set(self):
        self.calls += 1
        return self.calls > 1


def test_flush_measurements_keeps_only_retryable_entries(tmp_path: Path):
    path = tmp_path / "q.jsonl"
    entries = [
        {"sensor_id": "a", "value_mm": 1.0, "sent_at": 10},
        {"sensor_id": "b", "value_mm": 2.0, "sent_at": 20},
        {"sensor_id": "c", "value_mm": 3.0, "sent_at": 30},
    ]
    for entry in entries:
        append_measurement(path, entry)
    api = FakeApi(
        post_results=[
            ApiResult(True),
            ApiResult(False, True, "down"),
            ApiResult(False, False, "bad"),
        ]
    )

    flush_measurements(api, path)

    assert api.posted == [("a", 1.0, 10), ("b", 2.0, 20), ("c", 3.0, 30)]
    assert pop_measurements(path) == [entries[1]]


def test_flush_measurements_drops_non_finite_queued_value(tmp_path: Path):
    path = tmp_path / "q.jsonl"
    path.write_text('{"sensor_id": "s", "value_mm": NaN, "sent_at": 1}\n', encoding="utf-8")
    api = FakeApi()

    flush_measurements(api, path)

    assert api.posted == []
    assert pop_measurements(path) == []


def test_flush_pending_acks_keeps_only_retryable_ids(tmp_path: Path):
    path = tmp_path / "acks.json"
    save_acks(path, [1, 2, 3])
    api = FakeApi(
        ack_results=[
            ApiResult(True),
            ApiResult(False, True, "down"),
            ApiResult(False, False, "bad"),
        ]
    )

    flush_pending_acks(api, path)

    assert api.acked == [1, 2, 3]
    assert load_acks(path) == [2]


def test_process_commands_writes_payload_and_queues_retryable_ack(tmp_path: Path):
    path = tmp_path / "acks.json"
    api = FakeApi(
        commands=[{"id": 7, "payload": "s1;clean;"}],
        ack_results=[ApiResult(False, True, "down")],
    )
    ser = MagicMock()

    process_commands(api, ser, threading.Lock(), path)

    ser.write.assert_called_once_with(b"s1;clean;\n")
    assert load_acks(path) == [7]


def test_process_commands_rejects_when_serial_write_fails(tmp_path: Path):
    api = FakeApi(commands=[{"id": 8, "payload": "s1;clean;"}])
    ser = MagicMock()
    ser.write.side_effect = serial.SerialException("boom")

    process_commands(api, ser, threading.Lock(), tmp_path / "acks.json")

    assert api.rejected == [(8, "serial write failed")]
    assert api.acked == []


def test_uart_reader_queues_retryable_measurement(tmp_path: Path):
    path = tmp_path / "q.jsonl"
    api = FakeApi(post_results=[ApiResult(False, True, "down")])
    ser = MagicMock()
    ser.readline.return_value = b"sensor1: 20;123456789;\n"

    uart_reader(ser, threading.Lock(), api, path, OneShotStop())

    assert pop_measurements(path) == [
        {"sensor_id": "sensor1", "value_mm": 20.0, "sent_at": 123456789}
    ]


def _make_client() -> ApiClient:
    client = ApiClient("http://example.test", "key", 5.0)
    return client


def test_post_measurement_success():
    client = _make_client()
    resp = MagicMock()
    resp.status_code = 201
    client.session.post = MagicMock(return_value=resp)
    result = client.post_measurement("s1", 2.5, 100)
    assert result.ok is True
    assert result.retryable is False


def test_post_measurement_conflict_treated_ok():
    client = _make_client()
    resp = MagicMock()
    resp.status_code = 409
    client.session.post = MagicMock(return_value=resp)
    result = client.post_measurement("s1", 2.5, 100)
    assert result.ok is True


def test_post_measurement_server_error_retryable():
    client = _make_client()
    resp = MagicMock()
    resp.status_code = 503
    client.session.post = MagicMock(return_value=resp)
    result = client.post_measurement("s1", 2.5, 100)
    assert result.ok is False
    assert result.retryable is True


def test_post_measurement_client_error_permanent():
    client = _make_client()
    resp = MagicMock()
    resp.status_code = 422
    resp.text = "bad"
    client.session.post = MagicMock(return_value=resp)
    result = client.post_measurement("s1", 2.5, 100)
    assert result.ok is False
    assert result.retryable is False


def test_post_measurement_network_error_retryable():
    client = _make_client()
    client.session.post = MagicMock(side_effect=requests.RequestException("boom"))
    result = client.post_measurement("s1", 2.5, 100)
    assert result.ok is False
    assert result.retryable is True


def test_get_pending_commands_parses_items():
    client = _make_client()
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"items": [{"id": 1, "payload": "s1;clean;"}], "count": 1}
    client.session.get = MagicMock(return_value=resp)
    items = client.get_pending_commands()
    assert items == [{"id": 1, "payload": "s1;clean;"}]


def test_ack_command_already_answered_ok():
    client = _make_client()
    resp = MagicMock()
    resp.status_code = 409
    client.session.post = MagicMock(return_value=resp)
    result = client.ack_command(7)
    assert result.ok is True
