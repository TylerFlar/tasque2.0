# Workflow files

A workflow is a JSON file with a name, an optional version, and a list of nodes. Register it with
`tasque2 workflow-register <file>` and start it with `tasque2 workflow-start <name>` or from a
schedule (`schedule-workflow-create`). Templates are read each time a node is queued, so editing a
template needs no re-registration; changing the JSON does. `workflow-register --dry-run` shows what
would change, node by node.

```json
{
  "name": "weekly-review",
  "version": "1",
  "enabled": true,
  "definition": {"nodes": [ ... ]}
}
```

## Nodes

Every node has a `key` and a `kind` (default `work`), and may list `depends_on` keys. A node runs
when everything it depends on has succeeded.

**`work`**: one work item.

| Field | Meaning |
|---|---|
| `title` | the work item's title |
| `task_template_path` | Markdown instruction file, relative to the workflow file; read when the node is queued |
| `task_instruction` | inline instruction, instead of a template |
| `worker_kind` | `provider.default` (the configured provider), `provider.claude`, `provider.codex` |
| `runtime_contract` | `model_profile`, `model`, `effort`, `mcp_servers`, `disallowed_tools`, `max_turns`, `max_budget_usd`, `env`, `cwd` |
| `context` | the worker's context: memory namespace, canonical keys to pin, recall queries, digests to include |
| `max_attempts`, `retry_policy`, `priority` | retries and ordering |
| `deadline_seconds` / `deadline_at` | dead-letter the work if it has not finished by then |
| `tolerate_failure` | a dead-lettered node does not fail the run |

**`fan_out`**: one work item per item, run in parallel. Items come from `items` (a literal list),
`items_from` (a key of the run's input), or `items_from_output` (`"<node>.<path>"` into an upstream
node's output). The children use `child_title_template`, `child_task_template_path` or
`child_task_instruction_template` (both can use `{item[...]}` and `{index}`), `child_worker_kind`,
and the node's `runtime_contract` and `context`. Nodes that depend on the fan-out wait for all
children; with `tolerate_child_failures` they also run when some children dead-letter.

**`join`**: waits for its dependencies and collects their outputs, so the next node gets one
combined view.

**`gate`**: stops the run with a `prompt` until you answer it, from the run's Discord panel or
`tasque2 workflow-answer <run> <key> <answer>`. Nodes after it see the answer.

## What each node sees

A work node's context packet carries the workflow input, its own node definition, and the outputs
of the nodes it depends on (`produces` from each worker's result). Workers pass data forward by
putting it in `produces` when they submit their result.

## Where results go

A finished run posts its final nodes' reports in one Discord thread: a new thread per run, or
the thread named by the run's `discord_thread_id` (a schedule sets it with `discord_thread_id` in
its payload, so a daily workflow reports into one standing thread). When every final node
submits `"silent": true`, the run posts nothing.

## Lanes and tiers

Nodes run in the workflow's lane (the definition name, or `input.lane`), so `tasque2 usage` and
the traces group a workflow's nodes together. Give each node the tier its work needs: a
mechanical formatting step at `low`, research at `medium`, the one consequential decision at
`high`. `tasque2 lanes` lists every node's tier.
