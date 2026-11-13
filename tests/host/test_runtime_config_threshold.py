from __future__ import annotations

import json
import sqlite3
from dataclasses import replace

import pytest
from scope_recall.adapters.runtime_wiring import attach_trusted_host_runtime
from scope_recall.contracts import ContractError, InstanceBinding
from scope_recall.core.recall_policy import EMBEDDING_SPACE, SPACE_ID
from scope_recall.runtime.auxiliary import AuxiliaryRuntimeConfig
from scope_recall.runtime.instance import build_runtime_instance, default_vector_factory
from scope_recall.runtime.instance_config import RuntimeInstanceConfig
from scope_recall.runtime.lance_port import LanceIndexWriter, LanceVectorRecord
from v11_support import source_event


def test_runtime_mapping_wires_frozen_vector_threshold(tmp_path):
    binding = InstanceBinding(
        "TEST-threshold-agent",
        "TEST-threshold-installation",
        tmp_path / "data",
        frozenset({"TEST-threshold-scope"}),
        True,
    )
    config = RuntimeInstanceConfig.from_mapping(
        {
            "binding": {
                "agent_id": binding.agent_id,
                "installation_id": binding.installation_id,
                "data_directory": str(binding.data_directory),
                "scope_ids": sorted(binding.scope_ids),
                "test_mode": True,
            },
            "session_id": "TEST-threshold-session",
            "allowed_scope_ids": sorted(binding.scope_ids),
            "auxiliary": {"external_embedding": False, "external_consolidation": False},
            "vector_threshold": 0.653189984350642,
        }
    )
    instance = build_runtime_instance(config)
    try:
        assert instance.core.recall_pipeline.policy.vector_threshold == 0.653189984350642
    finally:
        instance.close()


class _QueryEmbedding:
    """Deterministic stand-in for the route's HTTP embedder; no model is called."""

    def __init__(self, dimensions: int) -> None:
        self.vector = (1.0,) + (0.0,) * (dimensions - 1)

    def embed_query(self, text: str, *, remaining_seconds: float):
        assert text and remaining_seconds > 0
        return self.vector


_NAMED_ROUTES = {
    # Stating the shipped values is still a named route: its request encoding
    # differs from the frozen descriptor's, so its space digest does too.
    "stated-gemini-defaults": {
        "model": EMBEDDING_SPACE["model"],
        "endpoint": EMBEDDING_SPACE["endpoint"],
        "dimensions": EMBEDDING_SPACE["dimensions"],
        "dialect": "gemini",
    },
    "openai-dialect-provider": {
        "model": "TEST-embedding",
        "endpoint": "https://example.invalid/v1/embeddings",
        "dimensions": 8,
        "dialect": "openai",
    },
}


@pytest.mark.parametrize("route_name", sorted(_NAMED_ROUTES))
def test_named_embedding_route_admits_its_own_vector_hits(tmp_path, route_name):
    """A host attaching a runtime config that names an embedding route recalls through its vectors.

    ``attach_trusted_host_runtime`` is the wiring Hermes, the Codex hooks and the
    Codex MCP server share.  While admission compared against the shipped space,
    every hit of such a route was refused as ``vector_old_or_mismatched_space``.
    """
    route = _NAMED_ROUTES[route_name]
    data = tmp_path / "d"
    data.mkdir()
    binding = InstanceBinding("TEST-agent", "TEST-installation", data, frozenset({"TEST-scope"}), True)
    raw = {
        "binding": {
            "agent_id": binding.agent_id,
            "installation_id": binding.installation_id,
            "data_directory": str(data),
            "scope_ids": ["TEST-scope"],
            "test_mode": True,
        },
        "session_id": "TEST-session",
        "allowed_scope_ids": ["TEST-scope"],
        "auxiliary": {
            "external_embedding": False,
            "external_consolidation": False,
            "embedding": {"credential_env": "TEST_EMBED_KEY", **route},
        },
        "vector_threshold": 0.5,
    }
    space_id = RuntimeInstanceConfig.from_mapping(raw).embedding_space_id()
    assert space_id != SPACE_ID
    raw["vector"] = {
        "backend": "sqlite-bruteforce",
        "storage_dir": str(data / "vectors" / space_id),
        "table_name": "TEST_vectors",
        "dimensions": route["dimensions"],
    }
    (data / "runtime-config.json").write_text(json.dumps(raw), encoding="utf-8")

    host = attach_trusted_host_runtime(
        config_path=None, expected_binding=binding, session_id="TEST-session", allowed_scope_ids=binding.scope_ids
    )
    try:
        assert host.configured, host.capability_gaps
        runtime = host.runtime
        assert host.core.recall_pipeline.policy.embedding_space_id == space_id
        context = runtime.config.context()
        host.core.initialize()
        event = host.core.record_event(
            context,
            source_event(source_event_key="TEST-named-route/source", content="海报四周压低明度，核心图案保留高光。"),
            scope_id="TEST-scope",
            remaining_seconds=10,
        ).event_refs[0]
        query = _QueryEmbedding(route["dimensions"])
        # The projection a drain writes: the configured space's partition, nothing else.
        store = default_vector_factory(runtime.config.vector)
        store.open()
        try:
            LanceIndexWriter(store).upsert_records(
                [
                    LanceVectorRecord(
                        "event",
                        event.ref,
                        event.revision,
                        "TEST-named-route-vector",
                        space_id,
                        query.vector,
                        "TEST-scope",
                        binding.agent_id,
                        binding.installation_id,
                    )
                ]
            )
        finally:
            store.close()
        runtime.auxiliary = replace(runtime.auxiliary, query_embedding=query)

        # No lexical overlap with the source: only the vector channel can admit it.
        request = {
            "protocol_version": "1.1",
            "request_id": "TEST-named-route",
            "query": "zzz unrelated words",
            "mode": "current",
            "max_items": 6,
            "budget_tokens": 1200,
        }
        result = host.core.recall(context, request, deadline_seconds=10)
        assert [candidate.ref for candidate in result.candidates if candidate.source == "vector"] == [event.ref]
        packet = host.core.recall_packet(context, request, deadline_seconds=10)
        assert event.ref in [item["ref"] for item in packet["items"]]
        assert "vector_old_or_mismatched_space" not in packet["gaps"]
    finally:
        host.close()


def test_runtime_mapping_wires_the_owner_s_names(tmp_path):
    """``owner_names`` is a JSON list in the runtime config; recall reads it as the policy's tuple, and a value it
    could not read refuses the config instead of recalling without it."""
    raw = {
        "binding": {
            "agent_id": "TEST-owner-agent",
            "installation_id": "TEST-owner-installation",
            "data_directory": str(tmp_path / "data"),
            "scope_ids": ["TEST-owner-scope"],
            "test_mode": True,
        },
        "session_id": "TEST-owner-session",
        "allowed_scope_ids": ["TEST-owner-scope"],
        "auxiliary": {"external_embedding": False, "external_consolidation": False},
        "owner_names": ["Joy", "乔伊"],
    }
    instance = build_runtime_instance(RuntimeInstanceConfig.from_mapping(raw))
    try:
        assert instance.core.recall_pipeline.policy.owner_names == ("Joy", "乔伊")
    finally:
        instance.close()
    with pytest.raises(ContractError):
        build_runtime_instance(RuntimeInstanceConfig.from_mapping(dict(raw, owner_names="Joy")))


def test_runtime_admission_consolidation_follows_the_actual_port(tmp_path):
    binding = InstanceBinding(
        "TEST-consolidation-agent",
        "TEST-consolidation-installation",
        tmp_path / "data",
        frozenset({"TEST-scope"}),
        True,
    )
    base = RuntimeInstanceConfig(binding, "TEST-session", binding.scope_ids)
    assert base.admission_policy().consolidation_enabled is False
    assert base.admission_policy(consolidation_port=object()).consolidation_enabled is True
    disabled = replace(
        base,
        auxiliary=AuxiliaryRuntimeConfig.from_mapping({"external_embedding": False, "external_consolidation": False}),
    )
    assert disabled.admission_policy().consolidation_enabled is False
    approved_without_route = replace(
        base,
        auxiliary=AuxiliaryRuntimeConfig.from_mapping({"external_embedding": False, "external_consolidation": True}),
    )
    assert approved_without_route.admission_policy().consolidation_enabled is False
    route_without_ledger = replace(
        base,
        auxiliary=AuxiliaryRuntimeConfig.from_mapping(
            {
                "external_embedding": False,
                "external_consolidation": True,
                "consolidation": {
                    "kind": "openai",
                    "model": "TEST-model",
                    "endpoint": "https://example.invalid/v1/chat/completions",
                    "credential_env": "TEST_CONSOLIDATION_KEY",
                    "output_limit_field": "max_tokens",
                    "max_output_tokens": 256,
                },
            }
        ),
    )
    assert route_without_ledger.admission_policy().consolidation_enabled is False


def _captured_work_types(instance):
    receipt = instance.core.record_event(
        instance.config.context(),
        source_event(source_event_key="TEST-consolidation/source", content="TEST substantive source"),
        scope_id="TEST-scope",
        remaining_seconds=10,
    )
    with sqlite3.connect(instance.core.storage.path) as conn:
        return conn.execute(
            "SELECT work_type FROM work_items WHERE subject_ref=? ORDER BY work_type",
            (receipt.event_refs[0].ref,),
        ).fetchall()


def test_runtime_capture_queues_embedding_alone_without_a_consolidation_port(tmp_path):
    binding = InstanceBinding(
        "TEST-no-port-agent",
        "TEST-no-port-installation",
        tmp_path / "data",
        frozenset({"TEST-scope"}),
        True,
    )
    config = RuntimeInstanceConfig(
        binding,
        "TEST-session",
        binding.scope_ids,
        auxiliary=AuxiliaryRuntimeConfig.from_mapping({"external_embedding": False, "external_consolidation": False}),
    )
    instance = build_runtime_instance(config)
    try:
        instance.core.initialize()
        assert _captured_work_types(instance) == [("embed",)]
    finally:
        instance.close()


@pytest.mark.parametrize(
    "auxiliary",
    [
        {"external_embedding": False, "external_consolidation": True},
        {
            "external_embedding": False,
            "external_consolidation": True,
            "consolidation": {
                "kind": "openai",
                "model": "TEST-model",
                "endpoint": "https://example.invalid/v1/chat/completions",
                "credential_env": "TEST_CONSOLIDATION_KEY",
                "output_limit_field": "max_tokens",
                "max_output_tokens": 256,
            },
        },
    ],
)
def test_runtime_capture_without_a_usable_consolidation_port_queues_embedding_alone(tmp_path, auxiliary):
    binding = InstanceBinding(
        "TEST-approved-agent",
        "TEST-approved-installation",
        tmp_path / "data",
        frozenset({"TEST-scope"}),
        True,
    )
    config = RuntimeInstanceConfig(
        binding,
        "TEST-session",
        binding.scope_ids,
        auxiliary=AuxiliaryRuntimeConfig.from_mapping(auxiliary),
    )
    instance = build_runtime_instance(config)
    try:
        instance.core.initialize()
        assert _captured_work_types(instance) == [("embed",)]
    finally:
        instance.close()


def test_runtime_capture_queues_consolidation_when_a_port_is_injected(tmp_path):
    binding = InstanceBinding(
        "TEST-port-agent",
        "TEST-port-installation",
        tmp_path / "data",
        frozenset({"TEST-scope"}),
        True,
    )
    config = RuntimeInstanceConfig(binding, "TEST-session", binding.scope_ids)
    instance = build_runtime_instance(config, consolidation=object())
    try:
        instance.core.initialize()
        assert _captured_work_types(instance) == [("consolidate",), ("embed",)]
    finally:
        instance.close()
