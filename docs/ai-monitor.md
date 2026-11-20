# AI Monitor implementation notes

## Existing functionality reused

Sellbird already has company discovery, public-company-site research and decision-maker finding, email verification, personalized sequences, follow-up scheduling, inbox/reply handling, suppression controls, an approval Outbox, Microsoft Graph mail, dashboard reporting primitives, SQLite state and discovery provenance. This implementation layers AI Monitor scoring and CRM synchronization on those components rather than replacing them.

## AI Monitor additions

- Weighted 100-point lead score with transparent per-criterion points and exact priority cutoffs (High 80–100, Medium 60–79, Low 0–59).
- South African ICP and neighboring-market profile in `sellbird.yaml`; administrator-editable industries, regions and role lists.
- Default follow-up delay configuration Day 1, Day 5, Day 12, Day 20 (at most four messages); live send remains disabled by default.
- Configurable Hercules REST adapter, field mapping, API paths and bounded retry behavior; credentials are read from `HERCULES_*` environment variables only.
- Durable sync queue, audit trail, email/CRM-ID identity checks, organization-domain review candidates, and suppression checks. Existing CRM contacts are linked, not patched. New CRM contacts are only created by an explicit sync operation.
- Reporting combines Sellbird outreach activity with opportunity, pipeline, meetings and revenue values only when supplied by Hercules; those CRM measures are never inferred from outreach.

## CRM source of truth and shared storage

`SyncStore` adds AI Monitor tables to a SQLite file. For a single local instance, point it at Sellbird's central database file. Multiple users/processes on one machine can use that same SQLite database. For multi-host / multi-location use, configure a supported shared database backend or managed relational database; do not put SQLite on an unreliable network filesystem. Hercules remains authoritative for customer, opportunity, pipeline and revenue data. The connector currently supports GET contacts and POST new contact; API-specific update, pagination and webhook details require Hercules API documentation and should be added only against the documented contract. CRM-owned fields are excluded from local overwrite behavior.

## Launch checklist (not performed here)

1. Install Sellbird and this package from the implementation branch; run automated tests.
2. Obtain Hercules API docs and a least-privilege token. Confirm endpoint paths, pagination, field mappings, record IDs, and webhook/update semantics. Set `HERCULES_*` variables in a secret store and validate against a CRM sandbox/mock before production.
3. Set a real authorized Microsoft 365 mailbox in the local config, register the delegated OAuth app per `docs/microsoft365.md`, and run `sellbird microsoft auth` and `sellbird microsoft test`.
4. Review POPIA/legal basis, contact-source acceptability, privacy notice/retention, opt-out process, postal/signature details and suppression seed list with the responsible compliance owner. This implementation provides source fields and suppression primitives but is not a legal determination.
5. Import/verify existing Hercules contacts and test dedup, synchronization, audit and CRM-owned field preservation.
6. Review drafts and campaign limits. Explicitly approve any activation only after the above. `channels.email.enabled` remains false in the committed profile; no mail was sent and no deployment was performed.
