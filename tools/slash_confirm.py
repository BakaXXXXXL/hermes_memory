"""Generic slash-command confirmation primitive (gateway-side).

Slash commands with an expensive side effect (currently only ``/reload-mcp``, which invalidates
the provider prompt cache) route through here. Button-UI adapters render Approve Once / Always
Approve / Cancel and call ``resolve()``; text-only adapters get a prompt and the gateway
intercepts ``/approve``, ``/always``, ``/cancel``. State is module-level (like ``tools.approval``)
so adapters can resolve without a ``GatewayRunner`` backreference. The CLI has its own
synchronous variant (``_prompt_slash_confirm`` in ``cli.py``).
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Awaitable, Callable, Dict, Optional

logger = logging.getLogger(__name__)

# session_key -> {"confirm_id", "command", "handler", "created_at"}
_pending: dict[str, dict[str, Any]] = {}
_lock = threading.RLock()

# Older pending confirms are discarded when the session's next message arrives (buttons live
# as long as the adapter keeps callback_data).
DEFAULT_TIMEOUT_SECONDS = 300


def register(session_key: str, confirm_id: str, command: str,
             handler: Callable[[str], Awaitable[Optional[str]]],
             *, user_id: Optional[str] = None,
             shared_session: bool = False) -> None:
    """Register a pending confirm, superseding any prior one for the session.

    *user_id* / *shared_session* bind the answer to the sender who opened the prompt:
    on a shared lane a co-member must not be able to confirm someone else's /new.
    """
    with _lock:
        _pending[session_key] = {
            "confirm_id": confirm_id, "command": command, "handler": handler,
            "created_at": time.time(), "user_id": str(user_id or ""),
            "shared_session": shared_session is True,
        }


def _entry_allows_user(entry: dict[str, Any], user_id: Optional[str]) -> bool:
    """Whether *user_id* may answer *entry*; shared lanes fail closed on an empty owner."""
    shared = entry.get("shared_session") is True
    owner = str(entry.get("user_id") or "")
    if shared:
        return bool(owner) and user_id is not None and owner == str(user_id)
    if not owner:
        return True
    return user_id is None or owner == str(user_id)


def can_resolve(session_key: str, confirm_id: str, user_id: Optional[str] = None) -> bool:
    """Whether this caller may answer the pending confirm (does not consume it)."""
    with _lock:
        entry = _pending.get(session_key)
        if not entry or entry.get("confirm_id") != confirm_id:
            return False
        return _entry_allows_user(entry, user_id)


def get_pending(session_key: str) -> Optional[dict[str, Any]]:
    """Return a copy of the pending confirm dict for a session, or None."""
    with _lock:
        entry = _pending.get(session_key)
        return dict(entry) if entry else None


def clear(session_key: str) -> None:
    """Drop the pending confirm for ``session_key`` without running it."""
    with _lock:
        _pending.pop(session_key, None)


def _is_stale(entry: dict[str, Any], timeout: float) -> bool:
    return time.time() - float(entry.get("created_at", 0) or 0) > timeout


def clear_if_stale(session_key: str, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> bool:
    """Drop the pending confirm if older than ``timeout`` seconds; True if dropped."""
    with _lock:
        entry = _pending.get(session_key)
        stale = bool(entry and _is_stale(entry, timeout))
        if stale:
            _pending.pop(session_key, None)
        return stale


async def resolve(session_key: str, confirm_id: str, choice: str,
                  timeout: float = DEFAULT_TIMEOUT_SECONDS,
                  user_id: Optional[str] = None) -> Optional[str]:
    """Run the pending handler with ``choice`` ("once" / "always" / "cancel").

    Returns the handler's output string, or None if the confirm was stale, already resolved,
    the confirm_id doesn't match (superseded prompt), or *user_id* is not the sender who
    opened it on a shared lane. An unauthorized answer is rejected WITHOUT consuming the
    prompt, so the real owner can still answer.
    """
    with _lock:
        entry = _pending.get(session_key)
        if not entry or entry.get("confirm_id") != confirm_id:
            return None
        if not _entry_allows_user(entry, user_id):
            logger.warning(
                "slash-confirm %s for session %s rejected: caller %r is not the initiator",
                confirm_id, session_key, user_id)
            return None
        # Pop before running so duplicate callbacks (button double-click) cannot run it twice.
        _pending.pop(session_key, None)
        if _is_stale(entry, timeout):
            return None
        handler = entry.get("handler")
        command = entry.get("command", "?")

    if not handler:
        return None
    try:
        result = await handler(choice)
    except Exception as exc:
        logger.error("Slash-confirm handler for /%s raised: %s", command, exc, exc_info=True)
        return f"❌ Error handling confirmation: {exc}"
    return result if isinstance(result, str) else None
