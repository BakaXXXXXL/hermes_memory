"""Unit tests for relay wire-field hygiene (Phase 1 parity).

Covers _event_from_wire's consumption of the contract §3 user-identity
enrichment fields the connector has always sent but the gateway used to drop:

  - ``user_display_name`` — the human-facing display name (native parity: the
    native Discord adapter surfaces ``message.author.display_name`` as
    ``user_name``).
  - ``user_handle`` — the raw platform handle, used only as a last resort.

Precedence: user_display_name > user_name > user_handle. Session keys derive
from user_id (never user_name), so this mapping is presentation-only and must
remain key-stable — asserted here via build_session_key.

Pure unit tests: no socket, no websockets dependency.
"""

from __future__ import annotations

import asyncio

from gateway.config import Platform
from gateway.platforms.base import BasePlatformAdapter
from gateway.platforms.event import MessageEvent
from gateway.relay.ws_transport import _event_from_wire
from gateway.session import SessionSource, build_session_key


def _wire_event(*, text="hello", **src_overrides):
    src = {
        "platform": "discord",
        "chat_id": "chan-1",
        "chat_type": "group",
        "user_id": "u-1",
        "user_name": "rawusername",
        "scope_id": "guild-1",
    }
    src.update(src_overrides)
    return {"text": text, "message_type": "text", "source": src}


class TestUserIdentityEnrichment:


    def test_handle_is_last_resort(self):
        ev = _event_from_wire(
            _wire_event(user_name=None, user_handle="ben#1234")
        )
        assert ev.source.user_name == "ben#1234"


    def test_session_key_is_stable_across_name_shapes(self):
        """user_name is presentation-only: the same user_id keys the same
        session whether or not the connector sent the enrichment fields."""
        plain = _event_from_wire(_wire_event())
        enriched = _event_from_wire(
            _wire_event(user_display_name="Ben Display", user_handle="ben#1234")
        )
        assert build_session_key(plain.source) == build_session_key(enriched.source)

    def test_shared_session_passes_through_only_when_set(self):
        assert _event_from_wire(_wire_event()).source.shared_session is False
        forwarded = _event_from_wire(_wire_event(shared_session=True))
        assert forwarded.source.shared_session is True
        # Same sender identity, shared lane: the key omits the participant.
        assert build_session_key(forwarded.source) == "agent:main:discord:group:chan-1"

    def test_shared_session_requires_a_strict_boolean(self):
        """Old/loose connectors send "false"/0; neither may opt into a shared lane."""
        for loose in ("false", "0", 0, 1, "true", "yes"):
            assert _event_from_wire(_wire_event(shared_session=loose)).source.shared_session is False

    def test_shared_lane_text_prefix_is_not_reapplied(self):
        """The connector already attributes shared-lane text; the runner must not add a second one."""
        prefixed = _wire_event(shared_session=True, text="[Alice|u-1]\nhello")
        event = _event_from_wire(prefixed)
        assert event.sender_prefix_handled is True
        assert event.sender_prefix == "[Alice|u-1]\n"

        # A loose value must not claim the text is already attributed.
        loose = _event_from_wire(_wire_event(shared_session="false", text="[Alice|u-1]\nhello"))
        assert loose.sender_prefix_handled is False

        # No prefix in the text -> the runner still adds the attribution once.
        bare = _event_from_wire(_wire_event(shared_session=True))
        assert bare.sender_prefix_handled is False
        assert bare.sender_prefix == ""

    def test_same_sender_prefixed_chunks_merge_with_one_prefix(self):
        """Two client-side split chunks arrive pre-attributed; the batch keeps one prefix."""
        class _Harness:
            _pending_text_batches: dict = {}
            _pending_text_batch_tasks: dict = {}

            @staticmethod
            def _drop_unresolved(event):
                return False

            @staticmethod
            def _text_batch_key(event):
                return "batch"

            @staticmethod
            async def _flush_text_batch(key):
                await asyncio.sleep(3600)

        source = SessionSource(
            platform=Platform.DISCORD, chat_id="chan-1", chat_type="group",
            user_id="u-1", shared_session=True)
        prefix = "[Alice|u-1]\n"
        first = MessageEvent(
            text=f"{prefix}hello", source=source,
            sender_prefix_handled=True, sender_prefix=prefix)
        second = MessageEvent(
            text=f"{prefix}world", source=source,
            sender_prefix_handled=True, sender_prefix=prefix)

        async def _run():
            harness = _Harness()
            BasePlatformAdapter._enqueue_text_event(harness, first)
            BasePlatformAdapter._enqueue_text_event(harness, second)
            assert harness._pending_text_batches["batch"].text == f"{prefix}hello\nworld"
            for task in list(harness._pending_text_batch_tasks.values()):
                task.cancel()

        asyncio.run(_run())
