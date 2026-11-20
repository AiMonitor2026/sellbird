"""Workflow and API tests for the AI Monitor extension."""
import sqlite3

import httpx
import pytest
from fastapi.testclient import TestClient

from sellbird.ai_monitor import SyncStore, default_sequence, score_lead
from sellbird.hercules import HerculesAPI, HerculesAPIConfig, sync_to_hercules
from sellbird import ai_monitor_plugin


def test_day_schedule_uses_absolute_campaign_days_and_not_cumulative_gaps():
    sequence = default_sequence()
    assert [s["campaign_day"] for s in sequence] == [1, 5, 12, 20]
    assert [s["delay_days"] for s in sequence] == [0, 4, 7, 8]


@pytest.mark.asyncio
async def test_paginated_contacts_and_same_host_next_link():
    calls = []
    def handler(request):
        calls.append(str(request.url))
        if request.url.params.get("page") == "2":
            return httpx.Response(200, json={"results": [{"id": "2"}]})
        return httpx.Response(200, json={"results": [{"id": "1"}], "next": "?page=2"})
    api = HerculesAPI(HerculesAPIConfig(base_url="https://crm.example", api_token="mock", retries=0),
                      transport=httpx.MockTransport(handler))
    assert [x["id"] for x in await api.contacts()] == ["1", "2"]
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_pagination_refuses_external_link():
    api = HerculesAPI(HerculesAPIConfig(base_url="https://crm.example", api_token="mock", retries=0),
                      transport=httpx.MockTransport(lambda req: httpx.Response(200, json={"results": [], "next": "https://evil.example/leak"})))
    with pytest.raises(Exception, match="configured HTTPS host"):
        await api.contacts()


@pytest.mark.asyncio
async def test_post_has_deterministic_idempotency_key_and_no_duplicate_on_sync_retry(tmp_path):
    store = SyncStore(str(tmp_path / "integration.db"))
    rid, _ = store.save_local({"email": "x@business.co.za", "score": 85, "domain": "business.co.za"})
    posted, keys = [], set()
    def handler(request):
        if request.method == "GET":
            return httpx.Response(200, json={"results": [], "next": None})
        key = request.headers["Idempotency-Key"]
        keys.add(key)
        posted.append(key)
        return httpx.Response(201, json={"id": "crm-contact"})
    api = HerculesAPI(HerculesAPIConfig(base_url="https://crm.example", api_token="mock", retries=0),
                      transport=httpx.MockTransport(handler))
    assert await sync_to_hercules(store, api, rid) == "created"
    # A subsequent run reads the CRM record and links it rather than POSTing again.
    def after(request):
        if request.method == "GET":
            return httpx.Response(200, json={"results": [{"id": "crm-contact", "email": "x@business.co.za"}]})
        return httpx.Response(500)
    api.transport = httpx.MockTransport(after)
    assert await sync_to_hercules(store, api, rid) == "matched_existing"
    assert len(posted) == 1 and len(keys) == 1


def test_plugin_installs_dashboard_and_suppression_hooks():
    class Registry:
        def __init__(self): self.routers, self.rules = [], {}
        def add_router(self, router): self.routers.append(router)
        def add_send_rule(self, name, rule): self.rules[name] = rule
    reg = Registry()
    ai_monitor_plugin.install(reg)
    assert reg.routers and "ai-monitor.suppression" in reg.rules
    paths = {route.path for route in reg.routers[0].routes}
    assert "/api/ai-monitor/overview" in paths
    assert "/api/ai-monitor/records/{record_id}/sync" in paths


def test_dashboard_assigns_owner_and_reports_local_metrics(tmp_path, monkeypatch):
    # Exercise route methods with a test Home and no live CRM config.
    from sellbird.workspace_home import Home
    home = Home.for_root(tmp_path)
    monkeypatch.setattr("sellbird.ai_monitor_plugin._store", lambda: SyncStore(str(home.db_path)))
    store = SyncStore(str(home.db_path))
    rid, _ = store.save_local({"email": "owner@example.com", "score": 81, "source": "company website"})
    router = ai_monitor_plugin.router
    from fastapi import FastAPI
    app = FastAPI()
    app.include_router(router)
    from sellbird.workspace_home import use_home
    with use_home(home):
        client = TestClient(app)
        response = client.post(f"/api/ai-monitor/records/{rid}/assign", json={"owner_id": "rep-1"})
        assert response.status_code == 200 and response.json()["owner_id"] == "rep-1"
        assert client.get("/api/ai-monitor/records").json()[0]["owner_id"] == "rep-1"
        overview = client.get("/api/ai-monitor/overview").json()
        assert overview["prospects"] == 1 and overview["qualified_leads"] == 1
        assert overview["revenue"] is None


@pytest.mark.asyncio
async def test_discovery_qualification_sync_opportunity_reporting_workflow(tmp_path):
    """Mock end-to-end: discovered company/contact -> scored -> CRM lead -> CRM metrics."""
    store = SyncStore(str(tmp_path / "e2e.db"))
    facts = {"industry_fit": 20, "company_size_sites": 18, "recurring_revenue": 18,
             "solution_relevance": 19, "decision_maker": 10, "verified_contact": 10}
    qualification = score_lead(facts)
    assert qualification["priority"] == "high"
    rid, _ = store.save_local({"email": "ops@fleet.co.za", "domain": "fleet.co.za",
                               "score": qualification["score"], "priority": qualification["priority"],
                               "source": "public company website", "source_url": "https://fleet.co.za/team"})
    def handler(request):
        if request.method == "GET" and request.url.path.endswith("contacts"):
            return httpx.Response(200, json={"results": []})
        if request.method == "POST":
            return httpx.Response(201, json={"id": "lead-42"})
        if request.url.path.endswith("opportunities"):
            return httpx.Response(200, json={"results": [{"id": "opp-8", "amount": 25000,
                                                           "revenue": 0, "meetingStatus": "scheduled"}]})
        return httpx.Response(404)
    api = HerculesAPI(HerculesAPIConfig(base_url="https://crm.example", api_token="mock", retries=0),
                      transport=httpx.MockTransport(handler))
    assert await sync_to_hercules(store, api, rid) == "created"
    assert store.record(rid)["crm_id"] == "lead-42"
    assert store.reporting()["qualified_leads"] == 1
    assert default_sequence()[-1]["campaign_day"] == 20
