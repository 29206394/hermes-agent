"""Feishu topic/thread delivery routing — regression coverage for #375.

``metadata["thread_id"]`` was handed to the create-message API as
``receive_id_type`` when no reply anchor was available, but that API's enum has no
``thread_id`` member (``chat_id``/``open_id``/``user_id``/``union_id``/``email``
only). Every such send died with ``[99992402] field validation failed``, and a
cron turn (no inbound message to reply into) *always* took that branch — so any
``deliver=origin`` job stamped with a topic lost its deliveries entirely.

Contract asserted here:

* a real topic (``omt_…``) with no anchor is reached through the reply API,
  anchored on the topic's last message, ``reply_in_thread=True``;
* a topic that cannot be addressed (unknown/withdrawn, or a value that is not a
  topic id at all — ``root_id`` message ids have historically been recorded in
  the ``thread_id`` slot) falls back to a plain top-level chat send;
* ``receive_id_type`` is never emitted with a value outside the API enum;
* the pre-existing reply-anchor and no-thread paths are unchanged.
"""
import asyncio
import json
from types import SimpleNamespace

import pytest

# A message id (``om_``) that the adapter records as ``thread_id`` when an inbound
# event carries no topic (``getattr(message, "thread_id", None) or root_id``).
POLLUTED_THREAD_ID = "om_x100b67a1e00b5070b326b10883ed5ca"
REAL_TOPIC_ID = "omt_19c12c3e528f9c8e"
CHAT_ID = "oc_3b855fe3345cf42296c9e3c7ee9ba177"


class _FakeMessageAPI:
    """Records every create/reply/list request; list is configurable per test."""

    def __init__(self, *, list_items=(), list_ok=True, reply_ok=True, reply_code=0):
        self.create_requests = []
        self.reply_requests = []
        self.list_requests = []
        self._list_items = list(list_items)
        self._list_ok = list_ok
        self._reply_ok = reply_ok
        self._reply_code = reply_code

    def create(self, request):
        self.create_requests.append(request)
        return SimpleNamespace(
            success=lambda: True, data=SimpleNamespace(message_id=f"om_created_{len(self.create_requests)}"))

    def reply(self, request):
        self.reply_requests.append(request)
        return SimpleNamespace(
            success=lambda: self._reply_ok, code=self._reply_code,
            data=SimpleNamespace(message_id=f"om_replied_{len(self.reply_requests)}"))

    def list(self, request):
        self.list_requests.append(request)
        return SimpleNamespace(
            success=lambda: self._list_ok,
            data=SimpleNamespace(items=[SimpleNamespace(message_id=m) for m in self._list_items]),
        )


def _make_adapter(api):
    from gateway.config import PlatformConfig
    from plugins.platforms.feishu.adapter import FeishuAdapter

    adapter = FeishuAdapter(PlatformConfig())
    adapter._client = SimpleNamespace(im=SimpleNamespace(v1=SimpleNamespace(message=api)))
    return adapter


def _send_metadata(api, metadata, *, chat_id=CHAT_ID, reply_to=None):
    adapter = _make_adapter(api)
    result = asyncio.run(adapter.send(chat_id, "hello", reply_to=reply_to, metadata=metadata))
    return adapter, result


def _created(api):
    assert len(api.create_requests) == 1
    request = api.create_requests[0]
    return request.receive_id_type, request.request_body.receive_id


def test_polluted_thread_id_never_becomes_receive_id_type():
    """The #375 shape: a root message id in the thread slot must not be sent as a thread."""
    api = _FakeMessageAPI(list_ok=False)
    _, result = _send_metadata(api, {"thread_id": POLLUTED_THREAD_ID})

    assert result.success, result.error
    receive_id_type, receive_id = _created(api)
    assert receive_id_type == "chat_id"
    assert receive_id == CHAT_ID
    assert not api.reply_requests


def test_real_topic_without_anchor_replies_into_the_topic():
    """A genuine ``omt_`` topic with no anchor is reached via the reply API."""
    api = _FakeMessageAPI(list_items=["om_last_in_topic"])
    _, result = _send_metadata(api, {"thread_id": REAL_TOPIC_ID})

    assert result.success, result.error
    assert not api.create_requests
    assert api.list_requests[0].container_id == REAL_TOPIC_ID
    assert len(api.reply_requests) == 1
    reply = api.reply_requests[0]
    assert reply.message_id == "om_last_in_topic"
    assert reply.request_body.reply_in_thread is True


def test_unaddressable_topic_falls_back_to_top_level_chat_send():
    """Topic listed but empty (withdrawn/not addressable) → plain chat send, never a bad enum."""
    api = _FakeMessageAPI(list_items=[])
    _, result = _send_metadata(api, {"thread_id": REAL_TOPIC_ID})

    assert result.success, result.error
    assert api.list_requests and not api.reply_requests
    receive_id_type, receive_id = _created(api)
    assert receive_id_type == "chat_id"
    assert receive_id == CHAT_ID


def test_anchor_withdrawn_before_reply_still_delivers_top_level():
    """The topic's last message can vanish between lookup and reply — post top-level, don't drop."""
    api = _FakeMessageAPI(list_items=["om_last_in_topic"], reply_code=230011, reply_ok=False)
    _, result = _send_metadata(api, {"thread_id": REAL_TOPIC_ID})

    assert result.success, result.error
    assert len(api.reply_requests) == 1
    receive_id_type, receive_id = _created(api)
    assert receive_id_type == "chat_id"
    assert receive_id == CHAT_ID


def test_reply_anchor_still_wins_and_replies_in_thread():
    """Regression: an explicit anchor keeps routing through the reply API."""
    api = _FakeMessageAPI()
    _, result = _send_metadata(
        api, {"thread_id": REAL_TOPIC_ID, "reply_to_message_id": "om_anchor"}, reply_to="om_explicit")

    assert result.success, result.error
    assert not api.create_requests
    reply = api.reply_requests[0]
    assert reply.message_id == "om_explicit"
    assert reply.request_body.reply_in_thread is True


def test_metadata_reply_anchor_used_without_a_real_topic():
    """reply_to_message_id alone (no thread) still replies rather than posting top-level."""
    api = _FakeMessageAPI()
    _, result = _send_metadata(api, {"reply_to_message_id": "om_anchor"})

    assert result.success, result.error
    assert not api.create_requests
    assert api.reply_requests[0].message_id == "om_anchor"
    assert api.reply_requests[0].request_body.reply_in_thread is False


def test_plain_send_without_thread_unchanged():
    api = _FakeMessageAPI()
    _, result = _send_metadata(api, None)

    assert result.success, result.error
    assert _created(api) == ("chat_id", CHAT_ID)


def test_create_request_guard_coerces_unknown_receive_id_type():
    """Layer-3 guard: an unknown enum value must never reach the API."""
    from plugins.platforms.feishu.adapter import FeishuAdapter

    body = FeishuAdapter._build_create_message_body(
        receive_id="om_whatever", msg_type="text", content=json.dumps({"text": "hi"}), uuid_value="u")
    request = FeishuAdapter._build_create_message_request("thread_id", body)

    assert request.receive_id_type == "chat_id"
