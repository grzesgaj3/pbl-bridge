from pathlib import Path
from unittest.mock import MagicMock

import requests

from bridge import (
    ApiClient,
    MeasurementQueue,
    PendingAcks,
    parse_measurement_line,
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


def test_parse_rejects_bad_epoch():
    assert parse_measurement_line("s: 1;abc") is None
    assert parse_measurement_line("s: 1;-5") is None


def test_measurement_queue_roundtrip(tmp_path: Path):
    q = MeasurementQueue(tmp_path / "q.jsonl")
    assert q.load() == []
    q.append({"sensor_id": "s", "value_mm": 1.0, "sent_at": 10})
    assert q.load() == [{"sensor_id": "s", "value_mm": 1.0, "sent_at": 10}]
    q.save([{"sensor_id": "s2", "value_mm": 2.0, "sent_at": 20}])
    assert q.load() == [{"sensor_id": "s2", "value_mm": 2.0, "sent_at": 20}]


def test_measurement_queue_skips_corrupt_lines(tmp_path: Path):
    path = tmp_path / "q.jsonl"
    path.write_text('{"a":1}\nnot json\n{"b":2}\n', encoding="utf-8")
    q = MeasurementQueue(path)
    assert q.load() == [{"a": 1}, {"b": 2}]


def test_pending_acks_roundtrip(tmp_path: Path):
    p = PendingAcks(tmp_path / "acks.json")
    assert p.load() == []
    p.save([3, 1, 3])
    assert p.load() == [1, 3]


def test_pending_acks_handles_corrupt(tmp_path: Path):
    path = tmp_path / "acks.json"
    path.write_text("garbage", encoding="utf-8")
    assert PendingAcks(path).load() == []


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
