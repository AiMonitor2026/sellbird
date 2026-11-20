"""Unit coverage for AI Monitor qualification and Hercules sync primitives."""
import asyncio
import json
import sqlite3

import httpx
import pytest

from sellbird.ai_monitor import (
    CRMConfig, HerculesClient, SyncStore, default_sequence, normalize_domain,
    priority_for_score, report_from_sellbird, score_lead, sync_record,
    validate_sequence,
)


def test_lead_score_thresholds_and_breakdown():
    assert score_lead({"industry_fit": 20, "company_size_sites": 20,
                       "recurring_revenue": 20, "solution_relevance": 20,
                       "decision_maker": 10, "verified_contact": 10})["priority"] == "high"
    assert priority_for_score(79) == "medium"
    assert priority_for_score(60) == "medium"
    assert priority_for_score(59) == "low"
    assert score_lead({})["score"] == 0
    with pytest.raises(ValueError):
        score_lead({"industry_fit": 21})


def test_default_sequence_is_day_1_5_12_20_and_limited():
    assert [s["delay_days"] for s in default_sequence()] == [1, 5, 12, 20]
    with pytest.raises(ValueError):
        validate_sequence([1, 5, 12, 20, 30])


def test_duplicate_handling_email_crm_domain_and_suppression(tmp_path):
    store = SyncStore(str(tmp_path / "shared.db"))
    first, outcome = store.save_local({"email": "JANE@ACME.CO.ZA", "domain": "https://www.acme.co.za/", "name": "Jane"})
    assert outcome == "created"
    same, outcome = store.save_local({"email": "jane@acme.co.za", "domain": "acme.co.za", "name": "Jane updated"})
    assert same == first and outcome == "matched_email_or_crm_id"
    second, outcome = store.save_local({"email": "john@acme.co.za", "domain": "acme.co.za", "name": "John"})
    assert second != first and outcome == "company_domain_review"
    assert len(store.find_domain_candidate("acme.co.za")) == 2
    store.add_suppression("jane@acme.co.za", "opt-out", "reply")
    assert store.is_suppressed("JANE@ACME.CO.ZA")
    assert normalize_domain("https://WWW.Acme.co.za/path") == "acme.co.za"


def test_crm_mapping_and_credentials_are_not_in_payload():
    cfg = CRMConfig(base_url="https://crm.example", api_token="secret", field_mapping={"email": "emailAddress"})
    client = HerculesClient(cfg)
    assert client.map_contact({"email": "x@example.com", "unknown": 1}) == {"emailAddress": "x@example.com"}
    assert "secret" not in repr(client.map_contact({"email": "x@example.com"}))


@pytest.mark.asyncio
async def test_crm_sync_matches_existing_and_does_not_create(tmp_path):
    store = SyncStore(str(tmp_path / "crm.db"))
    record_id, _ = store.save_local({"email": "lead@example.com", "domain": "example.com", "score": 80})
    calls = []
    def handler(request):
        calls.append(request.method)
        if request.method == "GET":
            return httpx.Response(200, json={"data": [{"id": "crm-1", "email": "lead@example.com", "status": "customer"}]})
        return httpx.Response(201, json={"id": "new"})
    client = HerculesClient(CRMConfig(base_url="https://crm.example", api_token="mock", retries=0), transport=httpx.MockTransport(handler))
    assert await sync_record(store, client, record_id) == "matched_existing"
    assert calls == ["GET"]
    assert store.record(record_id)["crm_id"] == "crm-1"
    assert store.audit_events()[0]["status"] == "matched_existing"


@pytest.mark.asyncio
async def test_crm_sync_creates_new_and_never_patches(tmp_path):
    store = SyncStore(str(tmp_path / "crm.db"))
    record_id, _ = store.save_local({"email": "new@example.com", "domain": "example.com"})
    methods = []
    def handler(request):
        methods.append(request.method)
        if request.method == "GET":
            return httpx.Response(200, json=[])
        return httpx.Response(201, json={"id": "crm-new"})
    client = HerculesClient(CRMConfig(base_url="https://crm.example", api_token="mock", retries=0), transport=httpx.MockTransport(handler))
    assert await sync_record(store, client, record_id) == "created"
    assert methods == ["GET", "POST"]
    assert "crm-new" == store.record(record_id)["crm_id"]


@pytest.mark.asyncio
async def test_sync_suppression_and_retry_fail_closed(tmp_path):
    store = SyncStore(str(tmp_path / "retry.db"))
    record_id, _ = store.save_local({"email": "blocked@example.com"})
    store.add_suppression("blocked@example.com", "bounce")
    client = HerculesClient(CRMConfig(base_url="https://crm.example", api_token="x", retries=0))
    assert await sync_record(store, client, record_id) == "suppressed"
    record_id2, _ = store.save_local({"email": "retry@example.com"})
    bad = HerculesClient(CRMConfig(base_url="https://crm.example", api_token="x", retries=0), transport=httpx.MockTransport(lambda req: httpx.Response(503)))
    assert await sync_record(store, bad, record_id2) == "retry_pending"
    assert store.audit_events()[0]["action"] == "crm_sync"


def test_reporting_keeps_revenue_crm_authoritative():
    report = report_from_sellbird({"prospects": {"new": 2}, "qualified_leads": 1},
                                  [{"leads_count": 2, "reply_count": 1}],
                                  {"meetings": 1, "opportunities": 2, "pipeline_value": 1000, "revenue": 250})
    assert report["replies"] == 1 and report["revenue"] == 250
    assert report["revenue_source"] == "Hercules CRM"
