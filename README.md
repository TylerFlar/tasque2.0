# Tasque 2.0

Tasque 2.0 is a local-first orchestration system for durable tasks, jobs,
workflows, schedules, memory, artifacts, Discord interaction, and
model-backed execution.

The current implementation covers the core local-first substrate: storage and
migrations, durable work queue/runtime, scheduler, provider/model runtime,
workflow supervisor, memory, artifacts, reports, backup/restore,
Discord intake/output, split channels and status threads, and
local file ingress/egress.
Discord attachments are persisted as local artifacts and referenced by path for
workers, and explicit worker output artifacts can be uploaded back into Discord
threads.
Replies in bound Discord threads are stored as a durable transcript and can
queue follow-up work with the parent report, recent messages, attachments, and
reply text in context. Replies to recorded Tasque bot messages in normal
channels are routed back to the referenced work instead of becoming
unrelated intake. Discord keeps a quiet ops embed, chain run status panels, and
per-work/per-workflow output threads; broad command dashboards are intentionally
avoided.

Local startup entrypoint:

```powershell
uv run tasque2 daemon
```

`tasque2 daemon` is the one long-running local service. It starts Discord intake
and output, schedule polling, workflow reconciliation, lease recovery, and
worker execution in one terminal. Configure the Discord token,
intake channel, split output channels, and allowed user ids in `.env` before
starting it. The daemon performs startup migrations automatically; `init-db`, `db-status`,
and `doctor` are available as explicit setup/debug checks but are not part of the
normal startup path. Idle daemon ticks are quiet; the terminal only prints tick
results when work actually changed.

Discord uses separate ops/jobs/chains/DLQ channels, a quiet ops panel, chain run panels in the chains channel, final work
and workflow output threads in jobs, and replies inside those threads to steer
work, answer questions, or attach files. The intake channel remains available
for natural-language one-off requests; the bot acknowledges queued intake
immediately and shows a typing indicator while model-backed work is still
waiting/running.

Discord channel routing:

```env
TASQUE2_DISCORD_INTAKE_CHANNEL_ID=...
TASQUE2_DISCORD_OPS_CHANNEL_ID=...
TASQUE2_DISCORD_JOBS_CHANNEL_ID=...
TASQUE2_DISCORD_CHAINS_CHANNEL_ID=...
TASQUE2_DISCORD_DLQ_CHANNEL_ID=...
```

All split channel ids are required. Tasque does not fall back from one Discord
route to another.

Provider smoke:

```powershell
$env:TASQUE2_DEFAULT_PROVIDER = "codex"  # or "claude"
uv run tasque2 provider-smoke codex
uv run tasque2 provider-smoke claude
```

Use `worker_kind=provider.default` for scheduled/model-backed jobs that should
resolve to the provider named by `TASQUE2_DEFAULT_PROVIDER`. Normal config
accepts only `codex` or `claude` for that default.

Model-backed jobs can set `runtime_contract.model` for an exact native-worker
model, or `runtime_contract.model_profile` for a portable native-worker profile.
Profiles are `low`, `medium`, and `high`. Profile names resolve through
provider-specific env vars such as `TASQUE2_CODEX_MODEL_MEDIUM` or
`TASQUE2_CLAUDE_MODEL_HIGH`. The coordinator/orchestrator itself always runs on
the selected provider's `high` profile. Jobs that omit `model_profile` use
`TASQUE2_NATIVE_WORKER_MODEL_PROFILE` for the native/domain worker.
Workers do not have execution timeouts; long runs are allowed to finish unless
they fail on their own or are explicitly canceled. Provider MCP config can still
raise MCP startup/tool timeouts so slow MCP calls are not cut short.

Worker context derives relevant memory/artifact searches from the job template,
with optional context hints available for unusual jobs. Text reports, readable
uploads, and inbound Discord messages can be ingested into searchable summary
and chunk memories, with Markdown mirrors under `data/memory-vault/` for local
inspection. Work thread replies can opt into memory capture and follow-up parser
work through `context.reply_memory` and `context.reply_followup_work`. Reply
follow-up work inherits the parent WorkItem's native-worker model/profile unless
the reply config sets its own.

Provider-backed Codex and Claude runs also receive a `tasque2` MCP server for
natural memory/artifact/work/workflow lookups and mutations during the run.
Provider work now uses an MCP result-inbox contract: Tasque gives the coordinator
a one-time `result_token`, and the coordinator must call `submit_worker_result`
exactly once. Provider stdout is kept for audit artifacts, not parsed as a
result fallback. Provider runs also keep a compact bundle artifact linking the
report, provider streams, trace, usage, and structured `produces`.

Useful worker MCP tools include `memory_search`, `memory_ingest_text`,
`memory_ingest_artifact`, `todo_write`, `ask_user`, `artifact_capture_file`,
`work_enqueue`, `workflow_start`, and `submit_worker_result`.

Manual MCP smoke:

```powershell
uv run python -c "from tasque2.mcp.server import build_server; print(type(build_server()).__name__)"
```

Schedules can start work or workflows:

```powershell
uv run tasque2 schedule-create "Workout" --type cron --expr "0 7 * * MON,WED,FRI" --worker-kind provider.default --task-template .\data\work-templates\workout-generator.template.md
uv run tasque2 schedule-workflow-create "Daily workflow" --type cron --expr "0 9 * * *" --workflow-definition-id <workflow-definition-id>
uv run tasque2 schedule-fire-now <schedule-id>
uv run tasque2 schedule-edit <schedule-id> --expr "minutes=30" --task "Updated task"
uv run tasque2 schedule-disable <schedule-id>
uv run tasque2 schedule-enable <schedule-id>
uv run tasque2 schedule-delete <schedule-id>
```

Artifacts:

```powershell
uv run tasque2 artifact-capture .\result.pdf --tag discord_upload
uv run tasque2 artifact-list --tag discord_upload
uv run tasque2 artifact-show <artifact-id>
uv run tasque2 memory-ingest-artifact <artifact-id>
uv run tasque2 memory-ingest-pending
```

Operations:

```powershell
uv run tasque2 doctor
uv run tasque2 doctor --json
uv run tasque2 runbook-smoke
uv run tasque2 backup-create
uv run tasque2 report-work <work-item-id>
uv run tasque2 events --work-item-id <work-item-id>
uv run tasque2 diagnose-work <work-item-id>
uv run ruff check .
```

Database-touching CLI commands run Alembic startup migrations before opening
sessions. `init-db` remains the explicit setup command, and `db-status` shows
the current revision.

Useful docs:

- [Local run notes](docs/phase1/local-run.md)
- [Work template format](docs/architecture/work-template-format.md)
- [Discord live checklist](docs/phase1/discord-live-checklist.md)
- [Troubleshooting](docs/phase1/troubleshooting.md)
