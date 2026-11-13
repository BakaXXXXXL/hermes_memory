"""Regression guard: send_slash_confirm must use format_message + MARKDOWN_V2."""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_repo = str(Path(__file__).resolve().parents[2])
if _repo not in sys.path:
    sys.path.insert(0, _repo)
from plugins.platforms.telegram.adapter import TelegramAdapter
from gateway.config import PlatformConfig


def _make_adapter():
    config = PlatformConfig(enabled=True, token="test-token", extra={})
    adapter = TelegramAdapter(config)
    adapter._bot = AsyncMock()
    adapter._app = MagicMock()
    return adapter


class TestSendSlashConfirm:

    @pytest.mark.asyncio
    async def test_uses_markdown_v2_and_escapes_special_chars(self):
        """send_slash_confirm must pass preview through format_message and use
        MARKDOWN_V2 — so commands with underscores, dots, or brackets don't
        raise BadRequest: Can't parse entities."""
        adapter = _make_adapter()
        sent = {}

        async def mock_send(**kwargs):
            sent.update(kwargs)
            return SimpleNamespace(message_id=7)

        adapter._bot.send_message = AsyncMock(side_effect=mock_send)

        result = await adapter.send_slash_confirm(
            chat_id="100",
            title="Confirm",
            message="/run script_name.sh --flag=value [option]",
            session_key="sk",
            confirm_id="cid1",
        )

        assert result.success is True
        assert "MARKDOWN_V2" in repr(sent["parse_mode"])
        # Underscores and dots must be escaped by format_message
        assert "script\\_name" in sent["text"]
        assert "\\." in sent["text"]




class TestSlashConfirmCallbackBinding:

    @staticmethod
    def _query(user_id=111):
        query = AsyncMock()
        query.from_user = MagicMock()
        query.from_user.id = user_id
        query.from_user.first_name = "Alice"
        query.answer = AsyncMock()
        query.edit_message_text = AsyncMock()
        query.message = MagicMock()
        query.message.chat_id = 100
        query.message.message_thread_id = None
        return query

    @pytest.mark.asyncio
    async def test_button_passes_the_clicker_to_the_resolver(self):
        adapter = _make_adapter()
        adapter._slash_confirm_state["cf-1"] = "sess-1"
        adapter._callback_authorized = AsyncMock(return_value=True)
        query = self._query(111)

        with patch("tools.slash_confirm.can_resolve", return_value=True), \
                patch("tools.slash_confirm.resolve", new=AsyncMock(return_value=None)) as resolve:
            await adapter._handle_slash_confirm_callback(query, "sc:once:cf-1", {})

        resolve.assert_awaited_once_with(
            "sess-1", "cf-1", "once", user_id="111")

    @pytest.mark.asyncio
    async def test_co_member_click_leaves_the_prompt_for_its_owner(self):
        adapter = _make_adapter()
        adapter._slash_confirm_state["cf-1"] = "sess-1"
        adapter._callback_authorized = AsyncMock(return_value=True)
        query = self._query(222)

        with patch("tools.slash_confirm.can_resolve", return_value=False), \
                patch("tools.slash_confirm.resolve", new=AsyncMock(return_value=None)) as resolve:
            await adapter._handle_slash_confirm_callback(query, "sc:once:cf-1", {})

        resolve.assert_not_awaited()
        assert adapter._slash_confirm_state["cf-1"] == "sess-1"
        query.answer.assert_awaited()
