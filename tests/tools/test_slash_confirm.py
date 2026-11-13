"""Tests for tools/slash_confirm.py — the generic slash-command confirmation primitive.

Covers register/resolve/clear lifecycle, stale-entry behavior, confirm_id
mismatch, handler exceptions, and async resolution.
"""

import asyncio
import time

import pytest

from tools import slash_confirm


@pytest.fixture(autouse=True)
def _clean_pending():
    """Every test gets a clean primitive state."""
    slash_confirm._pending.clear()
    yield
    slash_confirm._pending.clear()


class TestRegisterAndGetPending:
    def test_register_stores_entry(self):
        async def handler(choice):
            return f"got {choice}"

        slash_confirm.register("sess1", "cid1", "reload-mcp", handler)

        pending = slash_confirm.get_pending("sess1")
        assert pending is not None
        assert pending["confirm_id"] == "cid1"
        assert pending["command"] == "reload-mcp"
        assert pending["handler"] is handler
        assert "created_at" in pending


    def test_get_pending_returns_copy_not_reference(self):
        async def h(choice):
            return "x"

        slash_confirm.register("sess1", "cid1", "cmd", h)

        p1 = slash_confirm.get_pending("sess1")
        p1["command"] = "mutated"

        p2 = slash_confirm.get_pending("sess1")
        assert p2["command"] == "cmd"


class TestResolve:
    @pytest.mark.asyncio
    async def test_resolve_runs_handler_and_pops_entry(self):
        calls = []

        async def handler(choice):
            calls.append(choice)
            return f"resolved {choice}"

        slash_confirm.register("sess1", "cid1", "reload-mcp", handler)

        result = await slash_confirm.resolve("sess1", "cid1", "once")
        assert result == "resolved once"
        assert calls == ["once"]

        # Entry should be popped.
        assert slash_confirm.get_pending("sess1") is None

    @pytest.mark.asyncio
    async def test_resolve_no_pending_returns_none(self):
        result = await slash_confirm.resolve("sess1", "cid1", "once")
        assert result is None


    @pytest.mark.asyncio
    async def test_resolve_double_click_only_runs_handler_once(self):
        calls = []

        async def handler(choice):
            calls.append(choice)
            return "ran"

        slash_confirm.register("sess1", "cid1", "cmd", handler)

        # Simulate two near-simultaneous button clicks.
        r1, r2 = await asyncio.gather(
            slash_confirm.resolve("sess1", "cid1", "once"),
            slash_confirm.resolve("sess1", "cid1", "once"),
        )
        # Exactly one should have run the handler.
        assert calls == ["once"]
        assert (r1 == "ran") ^ (r2 == "ran")


class TestSharedLaneBinding:
    @pytest.mark.asyncio
    async def test_shared_lane_only_its_initiator_can_resolve(self):
        calls = []

        async def handler(choice):
            calls.append(choice)
            return "ran"

        slash_confirm.register(
            "sess1", "cid1", "new", handler,
            user_id="alice", shared_session=True)

        assert slash_confirm.can_resolve("sess1", "cid1", "alice") is True
        assert slash_confirm.can_resolve("sess1", "cid1", "bob") is False
        # A rejected answer must not consume the prompt.
        assert await slash_confirm.resolve("sess1", "cid1", "once", user_id="bob") is None
        assert calls == []
        assert slash_confirm.get_pending("sess1") is not None

        assert await slash_confirm.resolve("sess1", "cid1", "once", user_id="alice") == "ran"
        assert calls == ["once"]

    @pytest.mark.asyncio
    async def test_shared_lane_empty_owner_is_not_resolve_any(self):
        async def handler(choice):
            return "ran"

        slash_confirm.register("sess1", "cid1", "new", handler, shared_session=True)
        assert slash_confirm.can_resolve("sess1", "cid1", "alice") is False
        assert await slash_confirm.resolve("sess1", "cid1", "once", user_id="alice") is None
        assert await slash_confirm.resolve("sess1", "cid1", "once") is None
        assert slash_confirm.get_pending("sess1") is not None

    @pytest.mark.asyncio
    async def test_per_user_ownerless_entry_keeps_legacy_behavior(self):
        async def handler(choice):
            return "ran"

        slash_confirm.register("sess1", "cid1", "new", handler)
        assert await slash_confirm.resolve("sess1", "cid1", "once", user_id="alice") == "ran"


class TestClear:
    def test_clear_removes_entry(self):
        async def h(c):
            return "x"

        slash_confirm.register("sess1", "cid1", "cmd", h)
        assert slash_confirm.get_pending("sess1") is not None

        slash_confirm.clear("sess1")
        assert slash_confirm.get_pending("sess1") is None



class TestClearIfStale:
    def test_clears_stale_entry(self):
        async def h(c):
            return "x"

        slash_confirm.register("sess1", "cid1", "cmd", h)
        slash_confirm._pending["sess1"]["created_at"] = time.time() - 10000

        cleared = slash_confirm.clear_if_stale("sess1", timeout=300)
        assert cleared is True
        assert slash_confirm.get_pending("sess1") is None


    def test_returns_false_for_missing_entry(self):
        cleared = slash_confirm.clear_if_stale("nobody")
        assert cleared is False
