"""Regression tests for restoring the #62549 dashboard-identity transfer.

The 08-15 upstream refactor collapsed ``_ws_auth_reason`` back to a boolean
outcome and dropped the ``consume_ticket`` identity dict, so the gateway
WebSocket stopped carrying the authenticated user into session records. These
tests pin the restored contract:

- ``_ws_auth_with_info`` returns ``(reason, credential, auth_info)`` with the
  identity dict for ticket / internal credentials and ``None`` otherwise.
- ``_ws_auth_reason`` keeps its legacy two-tuple shape (backward compatible).
- ``tui_gateway.ws._bind_connection_identity`` rewrites session-create params
  with the server-verified ``pty_user_id`` / ``pty_provider`` (and strips them
  for ``server-internal``).
"""

from __future__ import annotations

from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pytest

from hermes_cli import web_server
from hermes_cli.dashboard_auth.ws_tickets import (
    INTERNAL_USER_ID,
    TicketInvalid,
    _reset_for_tests,
    internal_ws_credential,
    mint_ticket,
)


@pytest.fixture(autouse=True)
def reset_tickets():
    _reset_for_tests()
    yield
    _reset_for_tests()


def fake_ws(query: str = "") -> SimpleNamespace:
    values = parse_qs(query, keep_blank_values=True)
    params = {key: items[0] for key, items in values.items()}
    return SimpleNamespace(
        query_params=SimpleNamespace(get=lambda key, default="": params.get(key, default)),
        client=SimpleNamespace(host="127.0.0.1"),
        url=SimpleNamespace(path="/api/ws"),
    )


def set_auth_required(monkeypatch, value: bool) -> None:
    monkeypatch.setattr(web_server.app.state, "auth_required", value, raising=False)


# ── _ws_auth_with_info ────────────────────────────────────────────────


def test_ticket_auth_returns_identity(monkeypatch):
    set_auth_required(monkeypatch, True)
    ticket = mint_ticket(user_id="alice", provider="oauth")

    reason, credential, info = web_server._ws_auth_with_info(fake_ws(f"ticket={ticket}"))
    assert (reason, credential) == (None, "ticket")
    assert info["user_id"] == "alice"
    assert info["provider"] == "oauth"
    assert isinstance(info["minted_at"], int)


def test_internal_auth_returns_server_internal_identity(monkeypatch):
    set_auth_required(monkeypatch, True)
    reason, credential, info = web_server._ws_auth_with_info(
        fake_ws(f"internal={internal_ws_credential()}")
    )
    assert (reason, credential) == (None, "internal")
    assert info["user_id"] == INTERNAL_USER_ID


def test_rejected_credentials_have_no_identity(monkeypatch):
    set_auth_required(monkeypatch, True)
    assert web_server._ws_auth_with_info(fake_ws("ticket=bad")) == (
        "ticket_invalid",
        "ticket",
        None,
    )

    set_auth_required(monkeypatch, False)
    assert web_server._ws_auth_with_info(fake_ws("token=bad")) == (
        "token_mismatch",
        "token",
        None,
    )


def test_loopback_token_has_no_identity(monkeypatch):
    set_auth_required(monkeypatch, False)
    reason, credential, info = web_server._ws_auth_with_info(
        fake_ws(f"token={web_server._SESSION_TOKEN}")
    )
    assert (reason, credential) == (None, "token")
    assert info is None


def test_ws_auth_reason_stays_binary_backward_compatible(monkeypatch):
    """Legacy callers unpack two values and must not break."""
    set_auth_required(monkeypatch, True)
    ticket = mint_ticket(user_id="alice", provider="oauth")
    reason, credential = web_server._ws_auth_reason(fake_ws(f"ticket={ticket}"))
    assert (reason, credential) == (None, "ticket")


# ── _bind_connection_identity ─────────────────────────────────────────


def test_bind_injects_pty_identity_into_params():
    from tui_gateway.ws import _bind_connection_identity

    request = {"method": "session.create", "params": {"title": "hello"}}
    bound = _bind_connection_identity(request, {"user_id": "alice", "provider": "oauth"})
    assert bound["params"]["pty_user_id"] == "alice"
    assert bound["params"]["pty_provider"] == "oauth"
    # Original request is not mutated.
    assert request["params"] == {"title": "hello"}


def test_bind_strips_pty_identity_for_server_internal():
    from tui_gateway.ws import _bind_connection_identity

    request = {
        "method": "session.create",
        "params": {"pty_user_id": "forged", "pty_provider": "forged"},
    }
    bound = _bind_connection_identity(
        request, {"user_id": "server-internal", "provider": "server-internal"}
    )
    assert "pty_user_id" not in bound["params"]
    assert "pty_provider" not in bound["params"]


def test_bind_no_info_is_passthrough():
    from tui_gateway.ws import _bind_connection_identity

    request = {"method": "session.create", "params": {"title": "hello"}}
    bound = _bind_connection_identity(request, None)
    assert bound is request
