"""AI Monitor qualification, sequencing, and Hercules CRM sync primitives.

Hercules is authoritative for customer/opportunity/revenue fields. This module
keeps Sellbird outreach data separate, records provenance, and only performs
writes when explicitly invoked. Credentials are read from the environment.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping
from urllib.parse import urljoin

import httpx

SCORING_WEIGHTS = {
    "industry_fit": 20,
    "company_size_sites": 20,
    "recurring_revenue": 20,
    "solution_relevance": 20,
    "decision_maker": 10,
    "verified_contact": 10,
}
DEFAULT_FOLLOW_UP_DAYS = (1, 5, 12, 20)
AI_MONITOR_INDUSTRIES = (
    "logistics and distribution", "retail", "manufacturing and industrial",
    "automotive", "security companies",
)
AI_MONITOR_GEOGRAPHY = (
    "South Africa", "Botswana", "Namibia", "Zimbabwe", "Mozambique",
    "Lesotho", "Eswatini", "Zambia",
)


def priority_for_score(score: int) -> str:
    if not 0 <= score <= 100:
        raise ValueError("score must be between 0 and 100")
    return "high" if score >= 80 else "medium" if score >= 60 else "low"


def score_lead(criteria: Mapping[str, int | float | bool | None]) -> dict[str, Any]:
    """Score weighted criteria; each input is 0..its configured weight.

    Missing evidence contributes zero. Boolean True contributes the full weight;
    False contributes zero. Returns score, priority, and transparent breakdown.
    """
    breakdown: dict[str, int] = {}
    for key, weight in SCORING_WEIGHTS.items():
        value = criteria.get(key)
        if isinstance(value, bool):
            points = weight if value else 0
        elif value is None:
            points = 0
        else:
            points = round(float(value))
        if not 0 <= points <= weight:
            raise ValueError(f"{key} must be between 0 and {weight}")
        breakdown[key] = points
    total = sum(breakdown.values())
    return {"score": total, "priority": priority_for_score(total), "breakdown": breakdown}


def normalize_email(value: str | None) -> str:
    return (value or "").strip().casefold()


def normalize_domain(value: str | None) -> str:
    value = (value or "").strip().casefold()
    value = re.sub(r"^https?://", "", value).split("/", 1)[0].split(":", 1)[0]
    return value.removeprefix("www.").strip(".")


def default_sequence() -> list[dict[str, Any]]:
    """Four-message maximum; each delay is measured from the prior message."""
    return [{"step": i + 1, "delay_days": days} for i, days in enumerate(DEFAULT_FOLLOW_UP_DAYS)]


def validate_sequence(delays: list[int] | tuple[int, ...]) -> tuple[int, ...]:
    if len(delays) > 4 or any(isinstance(d, bool) or d < 0 for d in delays):
        raise ValueError("follow-up sequence must contain at most four non-negative delays")
    return tuple(int(d) for d in delays)


@dataclass(frozen=True)
class CRMConfig:
    base_url: str = ""
    api_token: str = ""
    contacts_path: str = "/api/contacts"
    companies_path: str = "/api/companies"
    opportunities_path: str = "/api/opportunities"
    timeout_seconds: float = 15.0
    retries: int = 3
    field_mapping: Mapping[str, str] = field(default_factory=lambda: {
        "email": "email", "first_name": "firstName", "last_name": "lastName",
        "title": "jobTitle", "company": "companyName", "domain": "websiteDomain",
        "phone": "phone", "linkedin_url": "linkedinUrl", "source": "leadSource",
        "source_url": "sourceUrl", "score": "sellbirdScore", "priority": "leadPriority",
    })

    @classmethod
    def from_env(cls) -> "CRMConfig":
        mapping = json.loads(os.getenv("HERCULES_FIELD_MAPPING", "{}") or "{}")
        return cls(
            base_url=os.getenv("HERCULES_BASE_URL", "").rstrip("/"),
            api_token=os.getenv("HERCULES_API_TOKEN", ""),
            contacts_path=os.getenv("HERCULES_CONTACTS_PATH", "/api/contacts"),
            companies_path=os.getenv("HERCULES_COMPANIES_PATH", "/api/companies"),
            opportunities_path=os.getenv("HERCULES_OPPORTUNITIES_PATH", "/api/opportunities"),
            retries=max(0, min(int(os.getenv("HERCULES_RETRIES", "3")), 8)),
            field_mapping={**cls().field_mapping, **mapping},
        )


class CRMError(RuntimeError):
    pass


class HerculesClient:
    """Configurable REST adapter; API schema/endpoints are operator supplied.

    Retry only transient network failures, 429s and 5xx. Never logs tokens or
    contact payloads. Outbound writes use POST only for new records; PATCH is
    restricted to explicitly configured Sellbird-owned mapped fields.
    """
    def __init__(self, config: CRMConfig, *, transport: httpx.AsyncBaseTransport | None = None):
        self.config = config
        self._transport = transport

    @property
    def configured(self) -> bool:
        return bool(self.config.base_url and self.config.api_token)

    async def request(self, method: str, path: str, *, payload: dict | None = None,
                      params: dict | None = None) -> Any:
        if not self.config.base_url or not self.config.api_token:
            raise CRMError("Hercules CRM is not configured; set HERCULES_BASE_URL and HERCULES_API_TOKEN")
        url = urljoin(self.config.base_url + "/", path.lstrip("/"))
        headers = {"Authorization": f"Bearer {self.config.api_token}", "Accept": "application/json"}
        transient: Exception | None = None
        for attempt in range(self.config.retries + 1):
            try:
                async with httpx.AsyncClient(timeout=self.config.timeout_seconds,
                                             transport=self._transport) as client:
                    response = await client.request(method, url, headers=headers,
                                                    json=payload, params=params)
                if response.status_code == 429 or response.status_code >= 500:
                    transient = CRMError(f"Hercules transient HTTP {response.status_code}")
                    if attempt < self.config.retries:
                        retry_after = response.headers.get("Retry-After", "")
                        try:
                            delay = min(10.0, max(0.0, float(retry_after)))
                        except ValueError:
                            delay = min(2 ** attempt, 8)
                        await asyncio.sleep(delay)
                        continue
                if response.is_error:
                    raise CRMError(f"Hercules returned HTTP {response.status_code}")
                if response.status_code == 204 or not response.content:
                    return None
                try:
                    return response.json()
                except ValueError as exc:
                    raise CRMError("Hercules response was not valid JSON") from exc
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                transient = exc
                if attempt < self.config.retries:
                    await asyncio.sleep(min(2 ** attempt, 8))
                    continue
                break
        raise CRMError(f"Hercules request failed after retries: {type(transient).__name__ if transient else 'unknown error'}")

    def map_contact(self, contact: Mapping[str, Any]) -> dict[str, Any]:
        return {remote: contact.get(local) for local, remote in self.config.field_mapping.items()
                if local in contact and contact.get(local) is not None}

    async def list_contacts(self) -> list[dict]:
        result = await self.request("GET", self.config.contacts_path)
        if isinstance(result, list):
            return [x for x in result if isinstance(x, dict)]
        if isinstance(result, dict):
            rows = result.get("results", result.get("data", result.get("contacts", [])))
            if isinstance(rows, list):
                return [x for x in rows if isinstance(x, dict)]
        raise CRMError("Hercules contacts response must be a list or contain results/data/contacts")

    async def create_contact(self, contact: Mapping[str, Any]) -> Any:
        return await self.request("POST", self.config.contacts_path, payload=self.map_contact(contact))


class SyncStore:
    """Durable shared-state tables in Sellbird's SQLite database.

    Point all sales representatives to the same database path on a supported
    shared filesystem, or use a managed storage backend; local SQLite files are
    not multi-host databases. CRM sync state and audit events are idempotent.
    """
    CRM_OWNED = frozenset({"customer_status", "opportunity_stage", "pipeline_value", "revenue", "owner"})

    def __init__(self, db_path: str):
        self.db_path = db_path
        with sqlite3.connect(db_path, timeout=15) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS ai_monitor_records (
                  id TEXT PRIMARY KEY, crm_id TEXT UNIQUE, email TEXT DEFAULT '', domain TEXT DEFAULT '',
                  kind TEXT DEFAULT 'prospect', payload_json TEXT NOT NULL DEFAULT '{}',
                  synced_at TEXT DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
                CREATE UNIQUE INDEX IF NOT EXISTS uq_ai_monitor_email ON ai_monitor_records(email) WHERE email != '';
                CREATE TABLE IF NOT EXISTS ai_monitor_audit (
                  id INTEGER PRIMARY KEY AUTOINCREMENT, record_id TEXT DEFAULT '', action TEXT NOT NULL,
                  status TEXT NOT NULL, details_json TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS ai_monitor_sync_queue (
                  record_id TEXT PRIMARY KEY, attempts INTEGER NOT NULL DEFAULT 0,
                  next_retry_at REAL NOT NULL DEFAULT 0, last_error TEXT DEFAULT '');
                CREATE TABLE IF NOT EXISTS ai_monitor_suppressions (
                  value TEXT PRIMARY KEY, reason TEXT NOT NULL, source TEXT DEFAULT '', created_at TEXT NOT NULL);
            """)

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def save_local(self, record: Mapping[str, Any], *, crm_id: str = "") -> tuple[str, str]:
        """Insert or merge Sellbird-owned values, returning (id, outcome).

        Email is a hard identity. A shared business domain is a company-level
        candidate, not enough to merge two different people; it is recorded for
        review. CRM id is authoritative when supplied.
        """
        data = dict(record)
        email = normalize_email(str(data.get("email") or ""))
        domain = normalize_domain(str(data.get("domain") or data.get("website") or ""))
        now = self._now()
        with sqlite3.connect(self.db_path, timeout=15) as db:
            db.row_factory = sqlite3.Row
            row = None
            if crm_id:
                row = db.execute("SELECT * FROM ai_monitor_records WHERE crm_id=?", (crm_id,)).fetchone()
            if row is None and email:
                row = db.execute("SELECT * FROM ai_monitor_records WHERE email=?", (email,)).fetchone()
            outcome = "created"
            if row:
                existing = json.loads(row["payload_json"] or "{}")
                # Preserve fields explicitly marked CRM-owned; never overwrite them locally.
                for key, value in data.items():
                    if key not in self.CRM_OWNED and value not in (None, ""):
                        existing[key] = value
                db.execute("UPDATE ai_monitor_records SET payload_json=?, domain=COALESCE(NULLIF(?,''),domain), "
                           "crm_id=COALESCE(NULLIF(?,''),crm_id), updated_at=? WHERE id=?",
                           (json.dumps(existing), domain, crm_id, now, row["id"]))
                record_id, outcome = row["id"], "matched_email_or_crm_id"
            else:
                record_id = str(data.get("id") or __import__("uuid").uuid4().hex)
                conflict = db.execute("SELECT id FROM ai_monitor_records WHERE domain=? AND domain!='' LIMIT 1",
                                      (domain,)).fetchone() if domain else None
                outcome = "company_domain_review" if conflict else "created"
                db.execute("INSERT INTO ai_monitor_records(id,crm_id,email,domain,kind,payload_json,created_at,updated_at) "
                           "VALUES(?,?,?,?,?,?,?,?)",
                           (record_id, crm_id or None, email, domain, str(data.get("kind") or "prospect"),
                            json.dumps(data), now, now))
            db.execute("INSERT INTO ai_monitor_audit(record_id,action,status,details_json,created_at) VALUES(?,?,?,?,?)",
                       (record_id, "local_upsert", outcome,
                        json.dumps({"email_present": bool(email), "domain_present": bool(domain)}), now))
            db.commit()
            return record_id, outcome

    def enqueue(self, record_id: str, error: str = "") -> None:
        with sqlite3.connect(self.db_path, timeout=15) as db:
            db.execute("INSERT INTO ai_monitor_sync_queue(record_id,attempts,next_retry_at,last_error) VALUES(?,0,0,?) "
                       "ON CONFLICT(record_id) DO UPDATE SET last_error=excluded.last_error", (record_id, error[:300]))
            db.commit()

    def note_sync(self, record_id: str, status: str, details: Mapping[str, Any] | None = None,
                  *, error: str = "", retry_after: float = 60.0) -> None:
        now = self._now()
        with sqlite3.connect(self.db_path, timeout=15) as db:
            if status == "synced":
                db.execute("UPDATE ai_monitor_records SET synced_at=?,updated_at=? WHERE id=?", (now, now, record_id))
                db.execute("DELETE FROM ai_monitor_sync_queue WHERE record_id=?", (record_id,))
            else:
                db.execute("INSERT INTO ai_monitor_sync_queue(record_id,attempts,next_retry_at,last_error) VALUES(?,1,?,?) "
                           "ON CONFLICT(record_id) DO UPDATE SET attempts=attempts+1,next_retry_at=excluded.next_retry_at,last_error=excluded.last_error",
                           (record_id, time.time() + retry_after, error[:300]))
            db.execute("INSERT INTO ai_monitor_audit(record_id,action,status,details_json,created_at) VALUES(?,?,?,?,?)",
                       (record_id, "crm_sync", status, json.dumps(details or {}), now))
            db.commit()

    def add_suppression(self, value: str, reason: str, source: str = "") -> None:
        value = normalize_email(value) if "@" in value else normalize_domain(value)
        if not value:
            raise ValueError("suppression must be an email address or company domain")
        with sqlite3.connect(self.db_path, timeout=15) as db:
            db.execute("INSERT OR IGNORE INTO ai_monitor_suppressions(value,reason,source,created_at) VALUES(?,?,?,?)",
                       (value, reason[:100], source[:200], self._now()))
            db.commit()

    def is_suppressed(self, email: str = "", domain: str = "") -> bool:
        keys = [normalize_email(email), normalize_domain(domain)]
        keys = [key for key in keys if key]
        if not keys:
            return False
        with sqlite3.connect(self.db_path, timeout=15) as db:
            marks = ",".join("?" for _ in keys)
            return db.execute(f"SELECT 1 FROM ai_monitor_suppressions WHERE value IN ({marks}) LIMIT 1", keys).fetchone() is not None

    def audit_events(self, limit: int = 100) -> list[dict]:
        with sqlite3.connect(self.db_path, timeout=15) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute("SELECT * FROM ai_monitor_audit ORDER BY id DESC LIMIT ?", (max(1, min(limit, 1000)),)).fetchall()
        return [{**dict(row), "details": json.loads(row["details_json"])} for row in rows]

    def reporting(self) -> dict[str, Any]:
        """Outreach-side dashboard totals. Revenue/opportunities stay CRM-owned."""
        with sqlite3.connect(self.db_path, timeout=15) as db:
            total, qualified = db.execute("SELECT COUNT(*), SUM(CASE WHEN json_extract(payload_json,'$.score') >= 60 THEN 1 ELSE 0 END) FROM ai_monitor_records").fetchone()
            audits = db.execute("SELECT action,status,COUNT(*) FROM ai_monitor_audit GROUP BY action,status").fetchall()
            synced = db.execute("SELECT COUNT(*) FROM ai_monitor_records WHERE crm_id IS NOT NULL AND crm_id != ''").fetchone()[0]
        return {"prospects": total or 0, "qualified_leads": qualified or 0,
                "crm_synced_records": synced, "activity": {f"{a}:{s}": n for a, s, n in audits},
                "opportunities": None, "pipeline_value": None, "revenue": None,
                "crm_metrics_note": "Opportunity, pipeline, and revenue metrics must be read from Hercules; not inferred from outreach records."}

    def record(self, record_id: str) -> dict | None:
        with sqlite3.connect(self.db_path, timeout=15) as db:
            db.row_factory = sqlite3.Row
            row = db.execute("SELECT * FROM ai_monitor_records WHERE id=?", (record_id,)).fetchone()
        if not row:
            return None
        result = dict(row)
        result["payload"] = json.loads(result.pop("payload_json"))
        return result

    def find_domain_candidate(self, domain: str) -> list[dict]:
        domain = normalize_domain(domain)
        with sqlite3.connect(self.db_path, timeout=15) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute("SELECT id,crm_id,email,domain,kind FROM ai_monitor_records WHERE domain=?", (domain,)).fetchall()
        return [dict(row) for row in rows]


async def sync_record(store: SyncStore, client: HerculesClient, record_id: str,
                      *, retries: int = 3) -> str:
    """Create in Hercules only if no email/CRM ID match exists; audit failures.

    If domain-only possible duplicates exist, do not create or merge a person;
    leave a review audit record. Existing Hercules-owned fields are untouched.
    """
    record = store.record(record_id)
    if not record:
        raise ValueError(f"unknown local record {record_id}")
    payload = record["payload"]
    if store.is_suppressed(payload.get("email", ""), payload.get("domain", "")):
        store.note_sync(record_id, "suppressed", {"reason": "local suppression list"})
        return "suppressed"
    try:
        contacts = await client.list_contacts()
        mapping = client.config.field_mapping
        email_key = mapping.get("email", "email")
        crm_key = "id"
        email = normalize_email(str(payload.get("email") or ""))
        match = next((c for c in contacts if email and normalize_email(str(c.get(email_key, ""))) == email), None)
        if match:
            # Read-through link only. Do not send a PATCH: CRM owns its record.
            crm_id = str(match.get(crm_key) or match.get("contactId") or "")
            if crm_id:
                with sqlite3.connect(store.db_path, timeout=15) as db:
                    db.execute("UPDATE ai_monitor_records SET crm_id=?,synced_at=?,updated_at=? WHERE id=?",
                               (crm_id, store._now(), store._now(), record_id)); db.commit()
            store.note_sync(record_id, "matched_existing", {"crm_id_present": bool(crm_id)})
            return "matched_existing"
        domain_matches = store.find_domain_candidate(str(payload.get("domain") or ""))
        # Distinct people at the same organization are valid; flag instead of blocking
        # unless no person email is available, where safe identity is impossible.
        if not email and domain_matches:
            store.note_sync(record_id, "needs_review", {"reason": "domain match without contact email"})
            return "needs_review"
        result = await client.create_contact(payload)
        crm_id = ""
        if isinstance(result, dict):
            crm_id = str(result.get("id") or result.get("contactId") or "")
        with sqlite3.connect(store.db_path, timeout=15) as db:
            db.execute("UPDATE ai_monitor_records SET crm_id=?,synced_at=?,updated_at=? WHERE id=?",
                       (crm_id or None, store._now(), store._now(), record_id)); db.commit()
        store.note_sync(record_id, "created", {"crm_id_present": bool(crm_id)})
        return "created"
    except CRMError as exc:
        # Queue with capped exponential backoff; external connector can drain it later.
        store.enqueue(record_id, str(exc))
        store.note_sync(record_id, "retry_pending", error=str(exc), retry_after=min(3600, 60 * (2 ** min(retries, 5))))
        return "retry_pending"


def report_from_sellbird(state_summary: Mapping[str, Any], campaign_stats: list[Mapping[str, Any]],
                         crm_metrics: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Combine outreach measures with CRM-authoritative opportunity/revenue totals."""
    activities = {k: sum(int(c.get(k, 0) or 0) for c in campaign_stats)
                  for k in ("leads_count", "reply_count", "interested_count")}
    crm_metrics = dict(crm_metrics or {})
    return {"prospects": sum(int(v) for v in state_summary.get("prospects", {}).values()),
            "qualified_leads": int(state_summary.get("qualified_leads", 0) or 0),
            "outreach_activity": activities,
            "replies": activities["reply_count"], "meetings": crm_metrics.get("meetings"),
            "opportunities": crm_metrics.get("opportunities"),
            "pipeline_value": crm_metrics.get("pipeline_value"),
            "revenue": crm_metrics.get("revenue"),
            "revenue_source": "Hercules CRM"}
