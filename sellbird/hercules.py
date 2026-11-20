"""Hercules API adapter with bounded pagination and idempotent writes.

The API's field names and endpoint shapes are configurable because Hercules
workspaces can expose different contracts. POST retries require the server to
honour the configured Idempotency-Key header; without that guarantee writes are
not retried after ambiguous transport failures.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import urljoin, urlparse

import httpx

from sellbird.ai_monitor import CRMConfig, CRMError, SyncStore, normalize_email


@dataclass(frozen=True)
class HerculesAPIConfig(CRMConfig):
    page_size: int = 100
    max_pages: int = 100
    idempotency_header: str = "Idempotency-Key"
    idempotent_post_supported: bool = False
    suppression_status_field: str = "status"
    suppressed_statuses: tuple[str, ...] = ("unsubscribed", "do_not_contact", "suppressed", "customer")
    meeting_path: str = ""

    @classmethod
    def from_env(cls) -> "HerculesAPIConfig":
        base = CRMConfig.from_env()
        statuses = tuple(s.strip().casefold() for s in os.getenv(
            "HERCULES_SUPPRESSED_STATUSES", "unsubscribed,do_not_contact,suppressed,customer").split(",") if s.strip())
        return cls(**base.__dict__,
                   page_size=max(1, min(int(os.getenv("HERCULES_PAGE_SIZE", "100")), 500)),
                   max_pages=max(1, min(int(os.getenv("HERCULES_MAX_PAGES", "100")), 1000)),
                   idempotency_header=os.getenv("HERCULES_IDEMPOTENCY_HEADER", "Idempotency-Key"),
                   idempotent_post_supported=os.getenv("HERCULES_IDEMPOTENT_POST", "false").casefold() == "true",
                   suppression_status_field=os.getenv("HERCULES_SUPPRESSION_FIELD", "status"),
                   suppressed_statuses=statuses,
                   meeting_path=os.getenv("HERCULES_MEETINGS_PATH", ""))


class HerculesAPI:
    def __init__(self, config: HerculesAPIConfig, *, transport: httpx.AsyncBaseTransport | None = None):
        self.config, self.transport = config, transport

    def _url(self, path: str) -> str:
        url = urljoin(self.config.base_url.rstrip("/") + "/", path.lstrip("/"))
        parsed, root = urlparse(url), urlparse(self.config.base_url)
        if parsed.scheme != "https" or parsed.hostname != root.hostname or parsed.port not in (None, 443):
            raise CRMError("Hercules pagination link is not on the configured HTTPS host")
        return url

    async def request(self, method: str, url_or_path: str, *, payload: dict | None = None,
                      idempotency_key: str = "") -> Any:
        if not self.config.base_url or not self.config.api_token:
            raise CRMError("Hercules CRM is not configured")
        url = self._url(url_or_path)
        headers = {"Authorization": f"Bearer {self.config.api_token}", "Accept": "application/json"}
        if idempotency_key:
            if not re.fullmatch(r"[A-Za-z0-9._:-]{1,200}", self.config.idempotency_header):
                raise CRMError("Invalid configured idempotency header")
            headers[self.config.idempotency_header] = idempotency_key
        attempts = self.config.retries if method.upper() == "GET" or self.config.idempotent_post_supported else 0
        for attempt in range(attempts + 1):
            try:
                async with httpx.AsyncClient(timeout=self.config.timeout_seconds, transport=self.transport) as client:
                    response = await client.request(method, url, headers=headers, json=payload)
                if (response.status_code == 429 or response.status_code >= 500) and attempt < attempts:
                    retry = response.headers.get("Retry-After", "")
                    try:
                        delay = min(10, max(0, float(retry)))
                    except ValueError:
                        delay = min(8, 2 ** attempt)
                    await asyncio.sleep(delay)
                    continue
                if response.is_error:
                    raise CRMError(f"Hercules returned HTTP {response.status_code}")
                if not response.content or response.status_code == 204:
                    return None
                try:
                    return response.json()
                except ValueError as exc:
                    raise CRMError("Hercules response was not valid JSON") from exc
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt == attempts:
                    raise CRMError(f"Hercules {method} failed ({type(exc).__name__})") from exc
                await asyncio.sleep(min(8, 2 ** attempt))
        raise CRMError("Hercules request exhausted retries")

    def _page(self, data: Any) -> tuple[list[dict], str | None]:
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)], None
        if not isinstance(data, dict):
            raise CRMError("Hercules page response must be an object or list")
        rows = next((data[k] for k in ("results", "data", "contacts", "items") if isinstance(data.get(k), list)), None)
        if rows is None:
            raise CRMError("Hercules page has no results/data/contacts/items list")
        next_url = data.get("next") or data.get("next_url") or data.get("nextPage")
        if not next_url and isinstance(data.get("links"), dict):
            next_url = data["links"].get("next")
        if not next_url and isinstance(data.get("meta"), dict):
            meta = data["meta"]
            if meta.get("has_more") and meta.get("page") is not None:
                next_url = "?page=" + str(int(meta["page"]) + 1)
        return [x for x in rows if isinstance(x, dict)], str(next_url) if next_url else None

    async def list_all(self, path: str) -> list[dict]:
        rows, seen = [], set()
        url = path
        for page in range(self.config.max_pages):
            absolute = self._url(url)
            if absolute in seen:
                raise CRMError("Hercules pagination loop detected")
            seen.add(absolute)
            result = await self.request("GET", absolute)
            items, next_link = self._page(result)
            rows.extend(items)
            if not next_link:
                return rows
            url = urljoin(absolute, next_link)
        raise CRMError(f"Hercules pagination exceeded {self.config.max_pages} pages")

    def map_contact(self, contact: Mapping[str, Any]) -> dict:
        return {remote: contact[local] for local, remote in self.config.field_mapping.items()
                if local in contact and contact[local] is not None}

    async def contacts(self) -> list[dict]:
        return await self.list_all(self.config.contacts_path)

    async def create_contact(self, contact: Mapping[str, Any], *, record_id: str) -> Any:
        # Deterministic key makes safe retries across process restarts possible,
        # provided Hercules implements the documented idempotency-key contract.
        key = hashlib.sha256(f"sellbird:contact:{record_id}".encode()).hexdigest()
        return await self.request("POST", self.config.contacts_path,
                                  payload=self.map_contact(contact), idempotency_key=key)

    async def opportunities(self) -> list[dict]:
        if not self.config.opportunities_path:
            return []
        return await self.list_all(self.config.opportunities_path)

    async def suppressions(self) -> set[str]:
        rows = await self.contacts()
        status_field = self.config.suppression_status_field
        email_field = self.config.field_mapping.get("email", "email")
        return {normalize_email(str(row.get(email_field, ""))) for row in rows
                if str(row.get(status_field, "")).casefold() in self.config.suppressed_statuses
                and row.get(email_field)}


async def sync_to_hercules(store: SyncStore, api: HerculesAPI, record_id: str) -> str:
    """Dedup and sync one local record; lock per record and re-check before POST."""
    record = store.record(record_id)
    if not record:
        raise ValueError(f"Unknown AI Monitor record: {record_id}")
    payload = record["payload"]
    email = normalize_email(str(payload.get("email", "")))
    if store.is_suppressed(email, str(payload.get("domain", ""))):
        store.note_sync(record_id, "suppressed", {"source": "local suppression"})
        return "suppressed"
    db_path = store.db_path
    with sqlite3.connect(db_path, timeout=20) as db:
        db.execute("CREATE TABLE IF NOT EXISTS ai_monitor_sync_locks(record_id TEXT PRIMARY KEY, locked_at REAL NOT NULL)")
        try:
            db.execute("INSERT INTO ai_monitor_sync_locks VALUES(?,?)", (record_id, time.time()))
            db.commit()
        except sqlite3.IntegrityError:
            return "in_progress"
    try:
        suppressed = await api.suppressions()
        if email and email in suppressed:
            store.add_suppression(email, "crm_suppressed", "Hercules")
            store.note_sync(record_id, "suppressed", {"source": "Hercules CRM"})
            return "suppressed"
        contacts = await api.contacts()
        email_field = api.config.field_mapping.get("email", "email")
        match = next((c for c in contacts if email and normalize_email(str(c.get(email_field, ""))) == email), None)
        if match:
            crm_id = str(match.get("id") or match.get("contactId") or "")
            if crm_id:
                with sqlite3.connect(db_path, timeout=20) as db:
                    db.execute("UPDATE ai_monitor_records SET crm_id=?,synced_at=?,updated_at=? WHERE id=?",
                               (crm_id, store._now(), store._now(), record_id)); db.commit()
            store.note_sync(record_id, "matched_existing", {"crm_id_present": bool(crm_id)})
            return "matched_existing"
        # Without a recipient address, identity cannot be safely deduplicated.
        if not email:
            store.note_sync(record_id, "needs_review", {"reason": "contact email unavailable"})
            return "needs_review"
        result = await api.create_contact(payload, record_id=record_id)
        crm_id = str(result.get("id") or result.get("contactId") or "") if isinstance(result, dict) else ""
        with sqlite3.connect(db_path, timeout=20) as db:
            db.execute("UPDATE ai_monitor_records SET crm_id=?,synced_at=?,updated_at=? WHERE id=?",
                       (crm_id or None, store._now(), store._now(), record_id)); db.commit()
        store.note_sync(record_id, "created", {"crm_id_present": bool(crm_id)})
        return "created"
    except CRMError as exc:
        store.enqueue(record_id, str(exc))
        store.note_sync(record_id, "retry_pending", error=str(exc))
        return "retry_pending"
    finally:
        with sqlite3.connect(db_path, timeout=20) as db:
            db.execute("DELETE FROM ai_monitor_sync_locks WHERE record_id=?", (record_id,)); db.commit()
