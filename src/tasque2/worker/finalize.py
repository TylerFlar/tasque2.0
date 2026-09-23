"""Turn a submitted worker payload into a completed result."""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from tasque2.artifacts import ArtifactStore
from tasque2.models import ProviderRun, WorkAttempt, WorkItem
from tasque2.providers import ProviderExecutionError
from tasque2.work.runner import WorkerResult

WAITING_STATUSES = {"blocked", "awaiting_user", "deferred"}
FAILED_STATUSES = {"failed", "error"}


def finalize_payload(
    session: Session,
    *,
    work_item: WorkItem,
    attempt: WorkAttempt,
    provider_run: ProviderRun,
    payload: dict[str, Any],
    store: ArtifactStore | None = None,
) -> WorkerResult:
    status, summary, report, produces = normalize_payload(payload)
    if status in FAILED_STATUSES:
        raise ProviderExecutionError(str(payload.get("error") or summary))
    if status in WAITING_STATUSES:
        produces["completion_signal"] = status
    report_artifact_id = None
    if report.strip():
        artifact = (store or ArtifactStore()).write_text(
            session,
            kind="worker_report",
            title=f"{work_item.title} report",
            content=report,
            suffix=".md",
            work_item_id=work_item.id,
            attempt_id=attempt.id,
            workflow_run_id=work_item.workflow_run_id,
            tags=["provider", provider_run.provider, "report"],
            source_kind="provider_run",
            source_id=provider_run.id,
        )
        report_artifact_id = artifact.id
    return WorkerResult(summary=summary, produces=produces, report_artifact_id=report_artifact_id)


def normalize_payload(payload: dict[str, Any]) -> tuple[str, str, str, dict[str, Any]]:
    report, summary, produces = payload.get("report"), payload.get("summary"), payload.get("produces") or {}
    if not isinstance(report, str):
        raise ProviderExecutionError("submit_worker_result payload is missing string field 'report'.")
    if not isinstance(summary, str):
        raise ProviderExecutionError("submit_worker_result payload is missing string field 'summary'.")
    if not isinstance(produces, dict):
        raise ProviderExecutionError("submit_worker_result payload field 'produces' must be an object.")
    status = str(payload.get("status") or "succeeded").strip().lower()
    error = payload.get("error")
    clean_error = error.strip() if isinstance(error, str) else ""
    produces = dict(produces)
    if clean_error and status in WAITING_STATUSES:
        produces.setdefault("blocker", clean_error)
    elif clean_error:
        status = "failed"
    return status, summary, report, produces
