"""The durable work queue: claim, finish, fail, retry, and recover work items.

Work item statuses: ``ready`` → ``running`` → ``succeeded`` | ``dead_letter`` |
``canceled``; ``paused`` and ``cancel_requested`` are operator states. Attempts record
each try with a lease that the daemon refreshes while the attempt runs.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, or_, select, update
from sqlalchemy.orm import Session

from tasque2.events import record_event
from tasque2.models import FailedWork, ProviderRun, WorkAttempt, WorkDependency, WorkItem, utc_now
from tasque2.telemetry import instruments
from tasque2.work.retry import capacity_gate, decide_retry

TERMINAL_WORK_STATUSES = {"succeeded", "dead_letter", "canceled"}
_CLAIM_WINDOW = 50


@dataclass(frozen=True)
class ClaimedWork:
    work_item: WorkItem
    attempt: WorkAttempt


class WorkQueue:
    def __init__(self, session: Session) -> None:
        self.session = session

    def claim_next_ready_work(
        self,
        *,
        lease_owner: str,
        lease_seconds: int | None = None,
        now: datetime | None = None,
    ) -> ClaimedWork | None:
        now = now or utc_now()
        gated = capacity_gate.is_closed(now)
        candidates = self.session.scalars(
            select(WorkItem)
            .where(WorkItem.status == "ready", or_(WorkItem.not_before.is_(None), WorkItem.not_before <= now))
            .order_by(WorkItem.priority.desc(), WorkItem.created_at.asc())
            .limit(_CLAIM_WINDOW)
        ).all()
        for work_item in candidates:
            if gated and work_item.worker_kind.startswith("provider."):
                continue
            if work_item.deadline_at is not None and work_item.deadline_at < now:
                self._expire_overdue(work_item, now=now)
                continue
            if self._has_unsatisfied_dependency(work_item):
                continue
            # The claim only succeeds while the item is still ready, so two processes polling the
            # same queue can never both run it.
            claimed = self.session.execute(
                update(WorkItem)
                .where(WorkItem.id == work_item.id, WorkItem.status == "ready")
                .values(status="running", attempt_count=WorkItem.attempt_count + 1),
                execution_options={"synchronize_session": False},
            )
            self.session.refresh(work_item)
            if claimed.rowcount != 1:
                continue
            attempt_number = work_item.attempt_count
            attempt = WorkAttempt(
                work_item_id=work_item.id,
                attempt_number=attempt_number,
                status="running",
                lease_owner=lease_owner,
                lease_expires_at=now + timedelta(seconds=lease_seconds) if lease_seconds is not None else None,
                started_at=now,
                heartbeat_at=now,
                worker_kind=work_item.worker_kind,
            )
            self.session.add(attempt)
            self.session.flush()
            self._event(
                "work.claimed",
                work_item,
                attempt_id=attempt.id,
                summary=f"Claimed by {lease_owner}",
                payload={"lease_owner": lease_owner, "attempt_number": attempt_number},
            )
            return ClaimedWork(work_item=work_item, attempt=attempt)
        return None

    def ready_count(self, *, now: datetime | None = None) -> int:
        """How many items are ready to claim now (dependencies and the capacity gate aside)."""
        now = now or utc_now()
        return int(
            self.session.scalar(
                select(func.count())
                .select_from(WorkItem)
                .where(WorkItem.status == "ready", or_(WorkItem.not_before.is_(None), WorkItem.not_before <= now))
            )
            or 0
        )

    def heartbeat_running_attempts(
        self,
        attempt_ids: Sequence[str],
        *,
        lease_seconds: int | None = None,
        now: datetime | None = None,
    ) -> int:
        """Refresh leases of in-flight attempts; a dead daemon stops refreshing and recovery takes over."""
        ids = [str(attempt_id) for attempt_id in attempt_ids if attempt_id]
        if not ids:
            return 0
        now = now or utc_now()
        expires_at = now + timedelta(seconds=lease_seconds) if lease_seconds is not None else None
        attempts = self.session.scalars(
            select(WorkAttempt).where(WorkAttempt.id.in_(ids), WorkAttempt.status == "running")
        ).all()
        for attempt in attempts:
            attempt.heartbeat_at = now
            attempt.lease_expires_at = expires_at
        self.session.flush()
        return len(attempts)

    def complete_attempt(
        self,
        attempt_id: str,
        *,
        summary: str,
        produces: dict[str, Any] | None = None,
        report_artifact_id: str | None = None,
        now: datetime | None = None,
    ) -> WorkAttempt:
        now = now or utc_now()
        attempt = self._get_attempt(attempt_id)
        work_item = attempt.work_item
        self.session.refresh(work_item)
        canceled = work_item.status in {"cancel_requested", "canceled"}
        attempt.status = work_item.status = "canceled" if canceled else "succeeded"
        attempt.ended_at = now
        attempt.summary = summary
        attempt.produces = produces or {}
        attempt.report_artifact_id = report_artifact_id
        self._close_sibling_attempts(attempt, now=now)
        self.session.flush()
        self._event(
            "work.canceled" if canceled else "work.succeeded",
            work_item,
            attempt_id=attempt.id,
            summary="Work was canceled before completion was recorded" if canceled else summary,
            payload={"produces": produces or {}, "report_artifact_id": report_artifact_id},
        )
        self._observe(attempt, work_item, outcome=work_item.status)
        return attempt

    def fail_attempt(
        self,
        attempt_id: str,
        *,
        error_type: str,
        error_message: str,
        now: datetime | None = None,
    ) -> WorkAttempt:
        now = now or utc_now()
        attempt = self._get_attempt(attempt_id)
        self.session.refresh(attempt.work_item)
        attempt.status = "failed"
        attempt.ended_at = now
        attempt.error_type = error_type
        attempt.error_message = error_message
        self._after_failure(attempt, now=now, event_type="work.failed")
        return attempt

    def recover_expired_leases(self, *, now: datetime | None = None) -> int:
        now = now or utc_now()
        attempts = self.session.scalars(
            select(WorkAttempt).where(
                WorkAttempt.status == "running",
                WorkAttempt.lease_expires_at.is_not(None),
                WorkAttempt.lease_expires_at < now,
            )
        ).all()
        for attempt in attempts:
            attempt.status = "expired"
            attempt.ended_at = now
            attempt.error_type = "LeaseExpired"
            attempt.error_message = "The work attempt lease expired before completion."
            self._close_provider_runs(attempt, now=now, marker="lease_expired")
            self._after_failure(attempt, now=now, event_type="work.lease_expired")
        return len(attempts)

    def recover_orphaned_attempts(
        self,
        *,
        lease_owner: str,
        orphaned_before: datetime,
        now: datetime | None = None,
    ) -> int:
        """Requeue attempts a previous daemon process left running without a lease."""
        now = now or utc_now()
        attempts = self.session.scalars(
            select(WorkAttempt).where(
                WorkAttempt.status == "running",
                WorkAttempt.lease_owner == lease_owner,
                WorkAttempt.lease_expires_at.is_(None),
                or_(WorkAttempt.heartbeat_at.is_(None), WorkAttempt.heartbeat_at < orphaned_before),
            )
        ).all()
        for attempt in attempts:
            attempt.status = "orphaned"
            attempt.ended_at = now
            attempt.error_type = "OrphanedAttempt"
            attempt.error_message = f"The {lease_owner!r} process restarted before this attempt completed."
            work_item = attempt.work_item
            if work_item.status == "running":
                work_item.status = "ready"
                work_item.not_before = None
                work_item.max_attempts = max(work_item.max_attempts, work_item.attempt_count + 1)
            provider_runs = self._close_provider_runs(attempt, now=now, marker="orphaned_recovered")
            self._event(
                "work.orphaned_attempt_recovered",
                work_item,
                attempt_id=attempt.id,
                summary="Recovered an attempt left running by a previous daemon process",
                payload={"lease_owner": lease_owner, "provider_run_ids": [run.id for run in provider_runs]},
            )
        self.session.flush()
        return len(attempts)

    def expire_overdue_work(self, *, now: datetime | None = None) -> int:
        """Dead-letter ready or paused work whose ``deadline_at`` passed before it started."""
        now = now or utc_now()
        overdue = self.session.scalars(
            select(WorkItem).where(
                WorkItem.status.in_(("ready", "paused")),
                WorkItem.deadline_at.is_not(None),
                WorkItem.deadline_at < now,
            )
        ).all()
        for work_item in overdue:
            self._expire_overdue(work_item, now=now)
        self.session.flush()
        return len(overdue)

    def pause_work(self, work_item_id: str) -> WorkItem:
        work_item = self._get_work_item(work_item_id)
        if work_item.status not in TERMINAL_WORK_STATUSES:
            work_item.status = "paused"
            self.session.flush()
            self._event("work.paused", work_item, summary="Work paused")
        return work_item

    def resume_work(self, work_item_id: str) -> WorkItem:
        work_item = self._get_work_item(work_item_id)
        if work_item.status == "paused":
            work_item.status = "ready"
            self.session.flush()
            self._event("work.resumed", work_item, summary="Work resumed")
        return work_item

    def request_cancel(self, work_item_id: str) -> WorkItem:
        work_item = self._get_work_item(work_item_id)
        if work_item.status in TERMINAL_WORK_STATUSES:
            return work_item
        if work_item.status == "running":
            work_item.status = "cancel_requested"
            event_type, summary = "work.cancel_requested", "Cancellation requested"
        else:
            work_item.status = "canceled"
            event_type, summary = "work.canceled", "Work canceled"
        self.session.flush()
        self._event(event_type, work_item, summary=summary)
        return work_item

    def retry_dead_letter(self, work_item_id: str) -> WorkItem:
        work_item = self._get_work_item(work_item_id)
        if work_item.status != "dead_letter":
            return work_item
        now = utc_now()
        work_item.status = "ready"
        work_item.not_before = None
        work_item.max_attempts = max(work_item.max_attempts, work_item.attempt_count + 1)
        for failed in self.session.scalars(
            select(FailedWork).where(FailedWork.work_item_id == work_item.id, FailedWork.status == "unresolved")
        ).all():
            failed.status = "retrying"
            failed.resolved_at = now
            failed.resolution_note = "Operator requested retry."
        self.session.flush()
        self._event("work.retry_requested", work_item, summary="Dead-letter work returned to ready state")
        return work_item

    def _after_failure(self, attempt: WorkAttempt, *, now: datetime, event_type: str) -> None:
        work_item = attempt.work_item
        self._event(
            event_type,
            work_item,
            attempt_id=attempt.id,
            summary=attempt.error_message,
            payload={
                "attempt_number": attempt.attempt_number,
                "error_type": attempt.error_type,
                "error_message": attempt.error_message,
            },
        )
        if work_item.status in {"cancel_requested", "canceled"}:
            attempt.status = work_item.status = "canceled"
            self._event("work.canceled", work_item, attempt_id=attempt.id, summary="Work canceled after attempt ended")
            self._observe(attempt, work_item, outcome="canceled")
            self.session.flush()
            return

        decision = decide_retry(
            error_type=attempt.error_type,
            error_message=attempt.error_message,
            attempt_number=attempt.attempt_number,
            max_attempts=work_item.max_attempts,
            base_delay_seconds=int((work_item.retry_policy or {}).get("delay_seconds", 0)),
            now=now,
        )
        if decision.limit_delay_seconds is not None:
            capacity_gate.hold_until(now + timedelta(seconds=decision.limit_delay_seconds))
            instruments().limit_stops.add(1, {"tasque.work.lane": work_item.lane or "unassigned"})
        if decision.retry:
            work_item.status = "ready"
            work_item.not_before = now + timedelta(seconds=decision.delay_seconds)
            self._event(
                "work.retry_scheduled",
                work_item,
                attempt_id=attempt.id,
                summary="Work returned to ready state for retry",
                payload={"delay_seconds": decision.delay_seconds, "transient": decision.transient},
            )
            self._observe(attempt, work_item, outcome="retry")
        else:
            work_item.status = "dead_letter"
            failed = FailedWork(
                work_item_id=work_item.id,
                attempt_id=attempt.id,
                status="unresolved",
                error_type=attempt.error_type,
                error_message=attempt.error_message,
                retry_count=work_item.attempt_count,
            )
            self.session.add(failed)
            self.session.flush()
            self._event(
                "work.dead_lettered",
                work_item,
                attempt_id=attempt.id,
                summary="Work moved to dead letter after retries were exhausted",
                payload={"failed_work_id": failed.id},
            )
            self._observe(attempt, work_item, outcome="dead_letter")
        self.session.flush()

    def _expire_overdue(self, work_item: WorkItem, *, now: datetime) -> None:
        deadline = work_item.deadline_at
        work_item.status = "dead_letter"
        failed = FailedWork(
            work_item_id=work_item.id,
            status="unresolved",
            error_type="DeadlineExceeded",
            error_message=(
                f"Work deadline {deadline.isoformat()} passed before it could run."
                if deadline is not None
                else "Work deadline passed before it could run."
            ),
            retry_count=work_item.attempt_count,
        )
        self.session.add(failed)
        self.session.flush()
        self._event(
            "work.deadline_exceeded",
            work_item,
            summary="Work expired: deadline passed before it could run",
            payload={"deadline_at": deadline.isoformat() if deadline else None, "failed_work_id": failed.id},
        )

    def _close_sibling_attempts(self, attempt: WorkAttempt, *, now: datetime) -> None:
        siblings = self.session.scalars(
            select(WorkAttempt).where(
                WorkAttempt.work_item_id == attempt.work_item_id,
                WorkAttempt.id != attempt.id,
                WorkAttempt.status == "running",
            )
        ).all()
        for sibling in siblings:
            sibling.status = "orphaned"
            sibling.ended_at = now
            sibling.error_type = "SupersededAttempt"
            sibling.error_message = "Another attempt finished this work item first."
            runs = self._close_provider_runs(
                sibling, now=now, marker="superseded", extra={"superseded_by_attempt_id": attempt.id}
            )
            self._event(
                "work.sibling_attempt_superseded",
                attempt.work_item,
                attempt_id=sibling.id,
                summary=sibling.error_message,
                payload={"completed_attempt_id": attempt.id, "provider_run_ids": [run.id for run in runs]},
            )

    def _close_provider_runs(
        self,
        attempt: WorkAttempt,
        *,
        now: datetime,
        marker: str,
        extra: dict[str, object] | None = None,
    ) -> list[ProviderRun]:
        runs = list(
            self.session.scalars(
                select(ProviderRun).where(ProviderRun.attempt_id == attempt.id, ProviderRun.status == "running")
            ).all()
        )
        for run in runs:
            run.status = "orphaned"
            run.ended_at = now
            run.usage = {**(run.usage or {}), marker: True, **(extra or {})}
        return runs

    def _has_unsatisfied_dependency(self, work_item: WorkItem) -> bool:
        for dependency in self.session.scalars(
            select(WorkDependency).where(WorkDependency.blocked_work_item_id == work_item.id)
        ).all():
            if dependency.dependency_workflow_node_id is not None:
                return True
            if dependency.dependency_work_item_id is None:
                continue
            upstream = self.session.get(WorkItem, dependency.dependency_work_item_id)
            if upstream is None or upstream.status != dependency.condition:
                return True
        return False

    def _observe(self, attempt: WorkAttempt, work_item: WorkItem, *, outcome: str) -> None:
        attributes = {
            "tasque.work.outcome": outcome,
            "tasque.work.lane": work_item.lane or "unassigned",
            "tasque.worker.kind": work_item.worker_kind,
        }
        metrics = instruments()
        metrics.work_runs.add(1, attributes)
        if attempt.started_at is not None:
            ended = attempt.ended_at or utc_now()
            metrics.work_duration.record(max(0.0, (ended - attempt.started_at).total_seconds()), attributes)

    def _get_work_item(self, work_item_id: str) -> WorkItem:
        work_item = self.session.get(WorkItem, work_item_id)
        if work_item is None:
            raise KeyError(f"Unknown work item: {work_item_id}")
        return work_item

    def _get_attempt(self, attempt_id: str) -> WorkAttempt:
        attempt = self.session.get(WorkAttempt, attempt_id)
        if attempt is None:
            raise KeyError(f"Unknown work attempt: {attempt_id}")
        return attempt

    def _event(
        self,
        event_type: str,
        work_item: WorkItem,
        *,
        attempt_id: str | None = None,
        summary: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        record_event(
            self.session,
            event_type=event_type,
            entity_kind="work_item",
            entity_id=work_item.id,
            work_item_id=work_item.id,
            attempt_id=attempt_id,
            workflow_run_id=work_item.workflow_run_id,
            source="queue",
            summary=summary,
            payload=payload,
        )
