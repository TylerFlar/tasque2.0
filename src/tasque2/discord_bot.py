from __future__ import annotations

import asyncio
from dataclasses import dataclass

import discord

from tasque2.config import Settings, get_settings
from tasque2.daemon import DaemonTickResult, TasqueDaemon
from tasque2.db import session_scope
from tasque2.discord_adapter import DiscordAttachmentPayload, DiscordService
from tasque2.discord_output import DiscordOutputService, DiscordPyOutputGateway
from tasque2.discord_ui import DiscordUIService, is_modal_action, parse_custom_id
from tasque2.migrations import upgrade_database
from tasque2.models import WorkItem, utc_now

TYPING_WORK_STATUSES = {"ready", "running", "cancel_requested"}


@dataclass(frozen=True)
class DiscordOutputChannels:
    ops: str
    jobs: str
    chains: str
    dlq: str


def run_bot(
    *,
    start_daemon: bool = False,
    daemon_interval_seconds: float = 5.0,
    daemon_max_work_items: int = 10,
) -> None:
    settings = get_settings()
    if not settings.discord_token:
        raise RuntimeError("TASQUE2_DISCORD_TOKEN is required to start the Discord bot.")
    if not settings.discord_intake_channel_id:
        raise RuntimeError("TASQUE2_DISCORD_INTAKE_CHANNEL_ID is required to start the Discord bot.")
    _output_channels(settings)

    upgrade_database()

    intents = discord.Intents.default()
    intents.message_content = True
    client = discord.Client(intents=intents)
    output_task_started = False
    daemon_task_started = False

    @client.event
    async def on_ready() -> None:
        nonlocal output_task_started, daemon_task_started
        print(f"Logged in as {client.user}")
        if not output_task_started:
            output_task_started = True
            await _ensure_control_panel(client)
            client.loop.create_task(_output_loop(client))
        if start_daemon and not daemon_task_started:
            daemon_task_started = True
            client.loop.create_task(
                _daemon_loop(
                    interval_seconds=daemon_interval_seconds,
                    max_work_items=daemon_max_work_items,
                )
            )

    @client.event
    async def on_message(message: discord.Message) -> None:
        if message.author.bot:
            return
        if not _is_author_allowed(str(message.author.id)):
            return

        channel_id = str(message.channel.id)
        thread_id = str(message.channel.id) if isinstance(message.channel, discord.Thread) else None
        try:
            attachments = await _read_attachments(message)
        except ValueError as exc:
            await message.channel.send(str(exc)[:1900])
            return
        with session_scope() as session:
            service = DiscordService(session)
            referenced_discord_message_id = _referenced_discord_message_id(message)
            if thread_id is not None:
                result = service.handle_thread_reply(
                    discord_message_id=str(message.id),
                    discord_channel_id=channel_id,
                    discord_thread_id=thread_id,
                    author=str(message.author),
                    content=message.content,
                    attachments=attachments,
                    referenced_discord_message_id=referenced_discord_message_id,
                )
                _start_typing_for_result(client, message.channel, result)
            elif referenced_discord_message_id:
                result = service.handle_channel_message(
                    discord_message_id=str(message.id),
                    discord_channel_id=channel_id,
                    author=str(message.author),
                    content=message.content,
                    attachments=attachments,
                    referenced_discord_message_id=referenced_discord_message_id,
                )
                if result.action == "unbound_channel" and (
                    settings.discord_intake_channel_id and channel_id == settings.discord_intake_channel_id
                ):
                    result = service.handle_intake_message(
                        discord_message_id=str(message.id),
                        discord_channel_id=channel_id,
                        author=str(message.author),
                        content=message.content,
                        attachments=attachments,
                    )
                    _start_typing_for_result(client, message.channel, result)
                else:
                    _start_typing_for_result(client, message.channel, result)
            elif settings.discord_intake_channel_id and channel_id == settings.discord_intake_channel_id:
                result = service.handle_intake_message(
                    discord_message_id=str(message.id),
                    discord_channel_id=channel_id,
                    author=str(message.author),
                    content=message.content,
                    attachments=attachments,
                )
                _start_typing_for_result(client, message.channel, result)

    @client.event
    async def on_interaction(interaction: discord.Interaction) -> None:
        custom_id = _custom_id_from_interaction(interaction)
        if custom_id is None:
            return
        action = parse_custom_id(custom_id)
        if action is None:
            return
        if interaction.user is not None and not _is_author_allowed(str(interaction.user.id)):
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    content="This Tasque bot is not configured to accept controls from your Discord user.",
                    ephemeral=True,
                )
            return

        if is_modal_action(action):
            await _send_modal_for_action(interaction, action)
            return

        await _safe_defer(interaction)
        try:
            result = await asyncio.to_thread(_handle_ui_action, custom_id)
            await interaction.followup.send(content=result.content[:1900], ephemeral=True)
        except Exception as exc:
            await interaction.followup.send(
                content=f"Tasque action failed: {exc}",
                ephemeral=True,
            )

    client.run(settings.discord_token)


async def _daemon_loop(*, interval_seconds: float, max_work_items: int) -> None:
    orphaned_before = utc_now()
    while True:
        try:
            result = await asyncio.to_thread(
                _run_daemon_once,
                max_work_items=max_work_items,
                recover_orphaned_lease_owner="daemon",
                orphaned_before=orphaned_before,
            )
            if result.has_activity:
                print(result)
        except Exception as exc:
            print(f"Tasque daemon tick failed: {exc}")
        await asyncio.sleep(interval_seconds)


def _run_daemon_once(
    *,
    max_work_items: int,
    recover_orphaned_lease_owner: str | None = None,
    orphaned_before=None,
) -> DaemonTickResult:
    with session_scope() as session:
        return TasqueDaemon(session).run_once(
            max_work_items=max_work_items,
            recover_orphaned_lease_owner=recover_orphaned_lease_owner,
            orphaned_before=orphaned_before,
        )


def _start_typing_for_result(client: discord.Client, channel, result) -> None:
    if result.action not in {"work_queued", "work_reply_recorded"} or result.entity_id is None:
        return
    client.loop.create_task(_typing_until_work_done(channel, result.entity_id))


async def _typing_until_work_done(channel, work_item_id: str, *, interval_seconds: float = 8.0) -> None:
    while True:
        if not await asyncio.to_thread(_work_is_waiting_for_response, work_item_id):
            return
        try:
            async with channel.typing():
                await asyncio.sleep(interval_seconds)
        except Exception:
            return


def _work_is_waiting_for_response(work_item_id: str) -> bool:
    with session_scope() as session:
        work_item = session.get(WorkItem, work_item_id)
        return work_item is not None and work_item.status in TYPING_WORK_STATUSES


async def _read_attachments(message: discord.Message) -> list[DiscordAttachmentPayload]:
    settings = get_settings()
    payloads: list[DiscordAttachmentPayload] = []
    for attachment in message.attachments:
        size = int(getattr(attachment, "size", 0) or 0)
        if size > settings.discord_max_attachment_bytes:
            raise ValueError(
                f"Discord attachment {attachment.filename!r} is {size} bytes; "
                f"limit is {settings.discord_max_attachment_bytes} bytes."
            )
        data = await attachment.read()
        payloads.append(
            DiscordAttachmentPayload(
                filename=attachment.filename,
                content_type=attachment.content_type,
                data=data,
            )
        )
    return payloads


def _is_author_allowed(user_id: str) -> bool:
    raw = get_settings().discord_allowed_user_ids
    if not raw:
        return True
    allowed = {part.strip() for part in raw.split(",") if part.strip()}
    return user_id in allowed


def _referenced_discord_message_id(message: discord.Message) -> str | None:
    reference = getattr(message, "reference", None)
    if reference is None:
        return None
    message_id = getattr(reference, "message_id", None)
    if message_id is None:
        resolved = getattr(reference, "resolved", None)
        message_id = getattr(resolved, "id", None)
    return str(message_id) if message_id is not None else None


async def _ensure_control_panel(client: discord.Client) -> None:
    settings = get_settings()
    channels = _output_channels(settings)
    gateway = DiscordPyOutputGateway(client)
    with session_scope() as session:
        await DiscordOutputService(session).ensure_control_panel(
            parent_channel_id=channels.ops,
            gateway=gateway,
        )


async def _output_loop(client: discord.Client) -> None:
    settings = get_settings()
    channels = _output_channels(settings)

    gateway = DiscordPyOutputGateway(client)
    await client.wait_until_ready()
    while not client.is_closed():
        with session_scope() as session:
            await DiscordOutputService(session).post_pending_updates(
                parent_channel_id=channels.ops,
                gateway=gateway,
                ops_channel_id=channels.ops,
                jobs_channel_id=channels.jobs,
                chains_channel_id=channels.chains,
                dlq_channel_id=channels.dlq,
            )
        await asyncio.sleep(settings.discord_output_poll_seconds)


def _output_channels(settings: Settings) -> DiscordOutputChannels:
    channels = {
        "TASQUE2_DISCORD_OPS_CHANNEL_ID": _clean_channel_id(settings.discord_ops_channel_id),
        "TASQUE2_DISCORD_JOBS_CHANNEL_ID": _clean_channel_id(settings.discord_jobs_channel_id),
        "TASQUE2_DISCORD_CHAINS_CHANNEL_ID": _clean_channel_id(settings.discord_chains_channel_id),
        "TASQUE2_DISCORD_DLQ_CHANNEL_ID": _clean_channel_id(settings.discord_dlq_channel_id),
    }
    missing = [name for name, value in channels.items() if value is None]
    if missing:
        raise RuntimeError(
            "Discord split channels are required; missing "
            + ", ".join(missing)
            + "."
        )
    return DiscordOutputChannels(
        ops=channels["TASQUE2_DISCORD_OPS_CHANNEL_ID"],
        jobs=channels["TASQUE2_DISCORD_JOBS_CHANNEL_ID"],
        chains=channels["TASQUE2_DISCORD_CHAINS_CHANNEL_ID"],
        dlq=channels["TASQUE2_DISCORD_DLQ_CHANNEL_ID"],
    )


def _clean_channel_id(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _custom_id_from_interaction(interaction: discord.Interaction) -> str | None:
    data = interaction.data
    if not isinstance(data, dict):
        return None
    custom_id = data.get("custom_id")
    return custom_id if isinstance(custom_id, str) else None


async def _safe_defer(interaction: discord.Interaction) -> None:
    try:
        if interaction.response.is_done():
            return
        await interaction.response.defer(ephemeral=True)
    except Exception:
        return


async def _send_modal_for_action(
    interaction: discord.Interaction,
    action,
) -> None:
    if action.scope == "work" and action.action == "new":
        await interaction.response.send_modal(_NewWorkModal())
        return
    if action.scope == "schedule" and action.action == "new":
        await interaction.response.send_modal(_NewScheduleModal())
        return
    if action.scope == "artifact" and action.action == "search":
        await interaction.response.send_modal(_ArtifactSearchModal())
        return
    if action.scope == "workflow" and action.action == "answer" and action.entity_id is not None:
        await interaction.response.send_modal(_WorkflowAnswerModal(workflow_run_id=action.entity_id))
        return
    await interaction.response.send_message(
        content="That Tasque control is missing an entity id.",
        ephemeral=True,
    )


def _handle_ui_action(custom_id: str):
    action = parse_custom_id(custom_id)
    if action is None:
        raise ValueError(f"Unknown Tasque custom id: {custom_id}")
    with session_scope() as session:
        return DiscordUIService(session).handle_action(action)


def _submit_new_work(
    *,
    title: str,
    task_instruction: str,
    worker_kind: str,
    priority: int,
    discord_interaction_id: str,
):
    with session_scope() as session:
        return DiscordUIService(session).create_work(
            title=title,
            task_instruction=task_instruction,
            worker_kind=worker_kind,
            priority=priority,
            discord_interaction_id=discord_interaction_id,
        )


def _submit_new_schedule(
    *,
    name: str,
    schedule_type: str,
    expression: str,
    task_instruction: str,
    worker_kind: str,
):
    with session_scope() as session:
        return DiscordUIService(session).create_schedule(
            name=name,
            schedule_type=schedule_type,
            expression=expression,
            task_instruction=task_instruction,
            worker_kind=worker_kind,
        )


def _submit_workflow_text_action(
    *,
    workflow_run_id: str,
    action: str,
    value: str,
    node_key: str | None,
):
    with session_scope() as session:
        return DiscordUIService(session).handle_workflow_text_action(
            workflow_run_id=workflow_run_id,
            action=action,
            value=value,
            node_key=node_key,
        )


def _submit_artifact_search(*, query: str, tags: str):
    tag_values = [tag.strip() for tag in tags.split(",") if tag.strip()]
    with session_scope() as session:
        return DiscordUIService(session).search_artifacts(
            query=query.strip() or None,
            tags=tag_values,
        )


async def _reply_to_modal(interaction: discord.Interaction, content: str) -> None:
    await interaction.response.send_message(content=content[:1900], ephemeral=True)


class _NewWorkModal(discord.ui.Modal):
    def __init__(self) -> None:
        super().__init__(title="New work")
        self.title_input = discord.ui.TextInput(
            label="Title",
            max_length=120,
        )
        self.instruction_input = discord.ui.TextInput(
            label="Instruction",
            style=discord.TextStyle.paragraph,
            max_length=4000,
        )
        self.worker_kind_input = discord.ui.TextInput(
            label="Worker kind",
            default="manual",
            max_length=80,
        )
        self.priority_input = discord.ui.TextInput(
            label="Priority",
            default="0",
            required=False,
            max_length=8,
        )
        self.add_item(self.title_input)
        self.add_item(self.instruction_input)
        self.add_item(self.worker_kind_input)
        self.add_item(self.priority_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            result = await asyncio.to_thread(
                _submit_new_work,
                title=str(self.title_input.value),
                task_instruction=str(self.instruction_input.value),
                worker_kind=str(self.worker_kind_input.value or "manual"),
                priority=int(str(self.priority_input.value or "0")),
                discord_interaction_id=str(interaction.id),
            )
            await _reply_to_modal(interaction, result.content)
        except Exception as exc:
            await _reply_to_modal(interaction, f"Could not queue work: {exc}")


class _NewScheduleModal(discord.ui.Modal):
    def __init__(self) -> None:
        super().__init__(title="New work schedule")
        self.name_input = discord.ui.TextInput(label="Name", max_length=120)
        self.type_input = discord.ui.TextInput(
            label="Type",
            default="interval",
            max_length=20,
            placeholder="date, interval, or cron",
        )
        self.expression_input = discord.ui.TextInput(
            label="Expression",
            max_length=240,
            placeholder="minutes=5, 0 9 * * *, or 2026-05-14T09:00:00-07:00",
        )
        self.task_input = discord.ui.TextInput(
            label="Task instruction",
            style=discord.TextStyle.paragraph,
            max_length=2000,
        )
        self.worker_kind_input = discord.ui.TextInput(
            label="Worker kind",
            default="manual",
            max_length=80,
        )
        self.add_item(self.name_input)
        self.add_item(self.type_input)
        self.add_item(self.expression_input)
        self.add_item(self.task_input)
        self.add_item(self.worker_kind_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            result = await asyncio.to_thread(
                _submit_new_schedule,
                name=str(self.name_input.value),
                schedule_type=str(self.type_input.value),
                expression=str(self.expression_input.value),
                task_instruction=str(self.task_input.value),
                worker_kind=str(self.worker_kind_input.value or "manual"),
            )
            await _reply_to_modal(interaction, result.content)
        except Exception as exc:
            await _reply_to_modal(interaction, f"Could not create schedule: {exc}")


class _WorkflowAnswerModal(discord.ui.Modal):
    def __init__(self, *, workflow_run_id: str) -> None:
        super().__init__(title="Answer workflow gate")
        self.workflow_run_id = workflow_run_id
        self.node_key_input = discord.ui.TextInput(
            label="Gate key",
            required=False,
            max_length=120,
            placeholder="Leave blank if there is only one open gate",
        )
        self.answer_input = discord.ui.TextInput(
            label="Answer",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=4000,
        )
        self.add_item(self.node_key_input)
        self.add_item(self.answer_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            result = await asyncio.to_thread(
                _submit_workflow_text_action,
                workflow_run_id=self.workflow_run_id,
                action="answer",
                value=str(self.answer_input.value),
                node_key=str(self.node_key_input.value or "").strip() or None,
            )
            await _reply_to_modal(interaction, result.content)
        except Exception as exc:
            await _reply_to_modal(interaction, f"Could not answer workflow gate: {exc}")


class _ArtifactSearchModal(discord.ui.Modal):
    def __init__(self) -> None:
        super().__init__(title="Find artifacts")
        self.query_input = discord.ui.TextInput(
            label="Search text",
            required=False,
            max_length=200,
        )
        self.tags_input = discord.ui.TextInput(
            label="Tags",
            required=False,
            max_length=200,
            placeholder="comma,separated,tags",
        )
        self.add_item(self.query_input)
        self.add_item(self.tags_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            result = await asyncio.to_thread(
                _submit_artifact_search,
                query=str(self.query_input.value or ""),
                tags=str(self.tags_input.value or ""),
            )
            await _reply_to_modal(interaction, result.content)
        except Exception as exc:
            await _reply_to_modal(interaction, f"Could not search artifacts: {exc}")
