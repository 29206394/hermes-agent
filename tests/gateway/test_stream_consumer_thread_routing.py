"""Regression tests for stream consumer thread/topic routing fix.

Verifies that GatewayStreamConsumer correctly passes reply_to on the first
message send, ensuring messages land in the correct topic/thread instead of
the main group chat.

Covers: #6969, #9916, #7355
"""
from unittest.mock import AsyncMock, MagicMock
from types import SimpleNamespace

import pytest

from gateway.stream_consumer import (
    GatewayStreamConsumer,
)


def _make_adapter(send_result=None, edit_result=None, max_length=4096):
    adapter = MagicMock()
    adapter.send = AsyncMock(
        return_value=send_result or SimpleNamespace(success=True, message_id="msg_1")
    )
    adapter.edit_message = AsyncMock(
        return_value=edit_result or SimpleNamespace(success=True)
    )
    adapter.MAX_MESSAGE_LENGTH = max_length
    return adapter


class TestInitialReplyToId:
    """Verify initial_reply_to_id is passed as reply_to on first send."""

    @pytest.mark.asyncio
    async def test_first_send_uses_initial_reply_to_id(self):
        """When initial_reply_to_id is set, first adapter.send() should
        include reply_to=initial_reply_to_id."""
        adapter = _make_adapter()
        consumer = GatewayStreamConsumer(
            adapter,
            "chat_123",
            metadata={"thread_id": "omt_topic123"},
            initial_reply_to_id="om_user_msg_456",
        )
        await consumer._send_or_edit("Hello world")

        adapter.send.assert_called_once()
        call_kwargs = adapter.send.call_args[1]
        assert call_kwargs["reply_to"] == "om_user_msg_456", (
            "First send should pass initial_reply_to_id as reply_to"
        )
        assert call_kwargs["chat_id"] == "chat_123"


    @pytest.mark.asyncio
    async def test_subsequent_edits_ignore_initial_reply_to_id(self):
        """After first send, edits should use message_id, not initial_reply_to_id."""
        adapter = _make_adapter()
        consumer = GatewayStreamConsumer(
            adapter,
            "chat_123",
            metadata={"thread_id": "omt_topic123"},
            initial_reply_to_id="om_user_msg_456",
        )

        # First send
        await consumer._send_or_edit("Hello world")
        assert adapter.send.call_count == 1

        # Second call should edit, not send
        await consumer._send_or_edit("Hello world updated")
        assert adapter.send.call_count == 1, "Should edit, not send again"
        adapter.edit_message.assert_called_once()
        edit_kwargs = adapter.edit_message.call_args[1]
        assert edit_kwargs["message_id"] == "msg_1"
        assert edit_kwargs["chat_id"] == "chat_123"


class TestOverflowFirstMessage:
    """Verify thread routing is preserved when the first message overflows."""

    @pytest.mark.asyncio
    async def test_overflow_first_send_uses_initial_reply_to_id(self):
        """When first message exceeds platform limit and is split into chunks,
        each chunk should be threaded to initial_reply_to_id, not None."""
        adapter = _make_adapter(max_length=10)
        adapter.truncate_message = MagicMock(
            return_value=["chunk_1", "chunk_2"]
        )
        consumer = GatewayStreamConsumer(
            adapter,
            "chat_123",
            metadata={"thread_id": "omt_topic123"},
            initial_reply_to_id="om_user_msg_789",
        )

        # Inject oversized accumulated text to trigger overflow path
        consumer._accumulated = "A" * 100
        consumer._current_edit_interval = 999
        await consumer._send_new_chunk("chunk_1", consumer._message_id or consumer._initial_reply_to_id)

        adapter.send.assert_called_once()
        call_kwargs = adapter.send.call_args[1]
        assert call_kwargs["reply_to"] == "om_user_msg_789", (
            "Overflow first chunk should use initial_reply_to_id"
        )


class TestFeishuFallbackThreadRouting:
    """FeishuAdapter._send_raw_message must never send ``receive_id_type='thread_id'``.

    That value is not in the send-message API's enum (chat_id/open_id/user_id/union_id/email),
    so it failed with ``[99992402] field validation failed`` on every threaded send. A topic is
    reachable only through the reply API: anchor on its last message with
    ``reply_in_thread=True``, else post top-level into the chat. Full matrix:
    ``tests/gateway/test_feishu_thread_delivery_routing.py``.
    """

    POLLUTED_THREAD_ID = "om_x100b67a1e00b5070b326b10883ed5ca"  # root_id stored in the thread slot
    REAL_TOPIC_ID = "omt_19c12c3e528f9c8e"

    @staticmethod
    def _adapter_with(message_api):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        adapter._client = SimpleNamespace(im=SimpleNamespace(v1=SimpleNamespace(message=message_api)))
        return adapter

    @pytest.mark.asyncio
    async def test_topic_without_anchor_replies_into_the_topic(self):
        import json

        calls = {"reply": [], "create": []}

        class _MessageAPI:
            def list(self, request):
                return SimpleNamespace(success=lambda: True, data=SimpleNamespace(
                    items=[SimpleNamespace(message_id="om_last_in_topic")]))

            def reply(self, request):
                calls["reply"].append(request)
                return SimpleNamespace(success=lambda: True, data=SimpleNamespace(message_id="om_reply"))

            def create(self, request):
                calls["create"].append(request)
                return SimpleNamespace(success=lambda: True, data=SimpleNamespace(message_id="om_created"))

        adapter = self._adapter_with(_MessageAPI())
        result = await adapter._send_raw_message(
            chat_id="oc_main_chat",
            msg_type="text",
            payload=json.dumps({"text": "hello"}),
            reply_to=None,
            metadata={"thread_id": self.REAL_TOPIC_ID},
        )

        assert result.success
        assert not calls["create"], "a real topic must not be addressed through message.create"
        assert calls["reply"][0].message_id == "om_last_in_topic"
        assert calls["reply"][0].request_body.reply_in_thread is True

    @pytest.mark.asyncio
    async def test_thread_slot_holding_a_message_id_falls_back_to_chat_send(self):
        import json

        calls = {"reply": [], "create": []}

        class _MessageAPI:
            def list(self, request):
                return SimpleNamespace(success=lambda: False, code=99991672, data=None)

            def reply(self, request):
                calls["reply"].append(request)
                return SimpleNamespace(success=lambda: True, data=SimpleNamespace(message_id="om_reply"))

            def create(self, request):
                calls["create"].append(request)
                return SimpleNamespace(success=lambda: True, data=SimpleNamespace(message_id="om_created"))

        adapter = self._adapter_with(_MessageAPI())
        result = await adapter._send_raw_message(
            chat_id="oc_main_chat",
            msg_type="text",
            payload=json.dumps({"text": "hello"}),
            reply_to=None,
            metadata={"thread_id": self.POLLUTED_THREAD_ID},
        )

        assert result.success
        assert not calls["reply"]
        assert calls["create"][0].receive_id_type == "chat_id"
        assert calls["create"][0].request_body.receive_id == "oc_main_chat"

