"""Alerts: posted to a webhook when one is set, always recorded, never raised (sentinel/alerts.py).

The webhook here is a local HTTP server, so nothing leaves the machine.
"""
import json
import threading
from datetime import date
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import SimpleNamespace

import pytest

from generator.source_db import conninfo as source_conninfo
from sentinel import alerts


@pytest.fixture
def webhook():
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}/hook", received
    server.shutdown()


def test_without_a_webhook_nothing_is_sent(monkeypatch):
    monkeypatch.delenv("CHRONOS_SLACK_WEBHOOK", raising=False)
    assert alerts.post_slack("hello") == "not_configured"


def test_a_webhook_gets_the_text(monkeypatch, webhook):
    url, received = webhook
    monkeypatch.setenv("CHRONOS_SLACK_WEBHOOK", url)
    assert alerts.post_slack("2017-05-07 blocked") == "slack 200"
    assert received == [{"text": "2017-05-07 blocked"}]


def test_an_unreachable_webhook_is_recorded_not_raised(monkeypatch):
    monkeypatch.setenv("CHRONOS_SLACK_WEBHOOK", "http://127.0.0.1:9/hook")  # discard port
    assert alerts.post_slack("hello").startswith("failed:")


@pytest.mark.postgres
def test_a_failed_task_is_recorded_with_its_reason(monkeypatch, source_db, webhook):
    url, received = webhook
    monkeypatch.setenv("CHRONOS_SLACK_WEBHOOK", url)
    conninfo = source_conninfo(source_db.info.dbname)
    monkeypatch.setattr("jobs.run_day.default_context",
                        lambda: SimpleNamespace(source_conninfo=conninfo, namespace_prefix=""))

    class SentinelBlocked(Exception):  # matched by name, as Airflow hands over the instance
        pass

    ti = SimpleNamespace(dag_id="chronos_daily", task_id="sentinel_audit", try_number=1)
    alerts.task_failed({"task_instance": ti, "logical_date": SimpleNamespace(
        date=lambda: date(2017, 5, 7)), "run_id": "backfill__2017-05-07",
        "exception": SentinelBlocked("2017-05-07: silver.events volume")})

    kind, summary, delivered = source_db.execute(
        "SELECT kind, summary, delivered FROM metrics.notifications").fetchone()
    assert kind == "blocked" and delivered == "slack 200"
    assert "sentinel_audit blocked for 2017-05-07" in summary and "volume" in summary
    assert "Console: http://localhost:8502" in received[0]["text"]
