"""Sellbird extension: AI Monitor CRM and management APIs plus send guard."""
from __future__ import annotations

import json
import sqlite3
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from sellbird.ai_monitor import SyncStore, score_lead
from sellbird.hercules import HerculesAPI, HerculesAPIConfig, sync_to_hercules
from sellbird.send_hooks import Cancel, Hold

router = APIRouter()


def _store() -> SyncStore:
    from sellbird.workspace_home import current_home
    store = SyncStore(str(current_home().db_path))
    with sqlite3.connect(store.db_path) as db:
        db.executescript("""
          CREATE TABLE IF NOT EXISTS ai_monitor_users(user_id TEXT PRIMARY KEY, display_name TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1);
          CREATE TABLE IF NOT EXISTS ai_monitor_ownership(record_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, assigned_at TEXT DEFAULT CURRENT_TIMESTAMP);
          CREATE TABLE IF NOT EXISTS ai_monitor_opportunity_attribution(crm_opportunity_id TEXT PRIMARY KEY, record_id TEXT NOT NULL, campaign_id TEXT DEFAULT '', attributed_at TEXT DEFAULT CURRENT_TIMESTAMP);
        """)
    return store


class AssignLead(BaseModel):
    owner_id: str = Field(min_length=1, max_length=200)


class ScoreRequest(BaseModel):
    criteria: dict[str, int | float | bool | None]


@router.get("/api/ai-monitor/overview")
async def overview():
    store = _store()
    report = store.reporting()
    try:
        api = HerculesAPI(HerculesAPIConfig.from_env())
        if api.configured:
            opps = await api.opportunities()
            report["opportunities"] = len(opps)
            # CRM values are only displayed if API response provides these numeric fields.
            report["pipeline_value"] = sum(float(x.get("pipelineValue", x.get("amount", 0)) or 0) for x in opps)
            report["revenue"] = sum(float(x.get("revenue", 0) or 0) for x in opps)
            report["meetings"] = sum(1 for x in opps if str(x.get("meetingStatus", "")).casefold() in {"scheduled", "held", "complete"})
            report["crm_metrics_source"] = "Hercules CRM"
    except Exception:
        report["crm_metrics_error"] = "Hercules metrics unavailable; outreach metrics remain available"
    return report


@router.get("/api/ai-monitor/records")
async def records(limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
    store = _store()
    with sqlite3.connect(store.db_path) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute("SELECT r.id,r.crm_id,r.email,r.domain,r.kind,r.payload_json,o.owner_id "
                          "FROM ai_monitor_records r LEFT JOIN ai_monitor_ownership o ON o.record_id=r.id "
                          "ORDER BY r.updated_at DESC LIMIT ? OFFSET ?", (limit, offset)).fetchall()
    return [{**dict(row), "payload": json.loads(row["payload_json"])} for row in rows]


@router.post("/api/ai-monitor/score")
async def score(request: ScoreRequest):
    try:
        return score_lead(request.criteria)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@router.post("/api/ai-monitor/records/{record_id}/assign")
async def assign(record_id: str, request: AssignLead):
    store = _store()
    if not store.record(record_id):
        raise HTTPException(404, "record not found")
    with sqlite3.connect(store.db_path) as db:
        db.execute("INSERT INTO ai_monitor_users(user_id,display_name) VALUES(?,?) ON CONFLICT(user_id) DO NOTHING",
                   (request.owner_id, request.owner_id))
        db.execute("INSERT INTO ai_monitor_ownership(record_id,owner_id) VALUES(?,?) ON CONFLICT(record_id) "
                   "DO UPDATE SET owner_id=excluded.owner_id,assigned_at=CURRENT_TIMESTAMP",
                   (record_id, request.owner_id)); db.commit()
    await __import__("sellbird.state", fromlist=["StateManager"]).StateManager(store.db_path).log_action(
        "ai_monitor_lead_assigned", "dashboard", {"record_id": record_id, "owner_id": request.owner_id})
    return {"record_id": record_id, "owner_id": request.owner_id}


@router.post("/api/ai-monitor/records/{record_id}/sync")
async def sync(record_id: str):
    store = _store()
    try:
        outcome = await sync_to_hercules(store, HerculesAPI(HerculesAPIConfig.from_env()), record_id)
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc
    return {"record_id": record_id, "outcome": outcome}


@router.post("/api/ai-monitor/opportunities/{opportunity_id}/attribute")
async def attribute_opportunity(opportunity_id: str, record_id: str, campaign_id: str = ""):
    store = _store()
    if not store.record(record_id):
        raise HTTPException(404, "record not found")
    # Attribution is an overlay/reference; opportunity stage and value remain CRM-owned.
    with sqlite3.connect(store.db_path) as db:
        db.execute("INSERT INTO ai_monitor_opportunity_attribution(crm_opportunity_id,record_id,campaign_id) VALUES(?,?,?) "
                   "ON CONFLICT(crm_opportunity_id) DO UPDATE SET record_id=excluded.record_id,campaign_id=excluded.campaign_id",
                   (opportunity_id, record_id, campaign_id)); db.commit()
    return {"opportunity_id": opportunity_id, "record_id": record_id, "campaign_id": campaign_id}


async def _ai_monitor_send_guard(check):
    """Fail closed for AI Monitor suppressions; built-in reply/opt-out/bounce checks still run first."""
    try:
        store = SyncStore(str(check.state.db_path))
        prospect = check.prospect
        email = getattr(prospect, "email", "") or check.item.get("to_email", "")
        domain = ""
        company_id = getattr(prospect, "company_id", "")
        if company_id:
            company = await check.state.get_company(company_id)
            domain = getattr(company, "domain", "") if company else ""
        if store.is_suppressed(email, domain):
            return Cancel("AI Monitor suppression list")
        # CRM lookup failure is a hold, never permission to send.
        cfg = HerculesAPIConfig.from_env()
        if cfg.base_url and cfg.api_token:
            if email in await HerculesAPI(cfg).suppressions():
                store.add_suppression(email, "crm_suppressed", "Hercules")
                return Cancel("Hercules CRM suppression")
    except Exception as exc:
        return Hold(f"AI Monitor suppression check unavailable ({type(exc).__name__})")
    return None


def install(registry):
    registry.add_router(router)
    registry.add_send_rule("ai-monitor.suppression", _ai_monitor_send_guard)
