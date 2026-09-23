"""The long-running daemon: tick loop, worker pool, optional Discord bot, graceful stop.

A stop request (Ctrl+C, SIGTERM, or ``tasque2 daemon-stop``) drains: the daemon stops
claiming new work, keeps ticking so running work is heartbeated and finalized, and exits
once nothing is in flight. A second Ctrl+C exits at once; results that runs deposit after
that are adopted by the next daemon start.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from collections.abc import Callable

import tasque2
from tasque2.config import Settings, get_settings
from tasque2.daemon import control
from tasque2.daemon.pool import WorkPool
from tasque2.daemon.tick import DaemonTick, TickResult
from tasque2.db import session_scope
from tasque2.models import utc_now
from tasque2.ops.status import work_status_counts
from tasque2.telemetry import instruments

logger = logging.getLogger(__name__)


class DaemonAlreadyRunning(RuntimeError):
    pass


class Daemon:
    def __init__(
        self, *, settings: Settings | None = None, discord: bool = True, max_claims: int | None = None
    ) -> None:
        self.settings = settings or get_settings()
        self.started_at = utc_now()
        self.pool = WorkPool(self.settings.daemon_concurrency)
        self.tick = DaemonTick(pool=self.pool, recover_before=self.started_at)
        self.max_claims = max_claims
        self.discord = discord
        self._stop: asyncio.Event | None = None
        self._stop_requests = 0

    async def run(self) -> None:
        self._stop = asyncio.Event()
        restore_signals = self._install_signal_handlers(asyncio.get_running_loop())
        instruments().observe_queue(_queue_counts)
        bot, bot_task = self._start_discord()
        logger.info(
            "Tasque daemon %s started (pid %s, concurrency %s)",
            tasque2.__version__,
            os.getpid(),
            self.pool.concurrency,
        )
        try:
            await self._tick_loop()
        finally:
            restore_signals()
            if bot is not None:
                await bot.close()
            if bot_task is not None:
                await asyncio.gather(bot_task, return_exceptions=True)
            self.pool.shutdown(wait=False)
            control.clear_state()
            control.clear_drain()
            logger.info("Tasque daemon stopped")

    def request_stop(self) -> None:
        self._stop_requests += 1
        if self._stop_requests > 1:
            logger.warning("Second stop request: exiting without waiting for work in flight")
            os._exit(130)
        logger.info("Stop requested: draining %s run(s) in flight", self.pool.in_flight_count())
        if self._stop is not None:
            self._stop.set()

    async def _tick_loop(self) -> None:
        assert self._stop is not None
        while True:
            draining = self._stop.is_set() or control.drain_requested()
            try:
                result = await asyncio.to_thread(self._tick_once, claim=not draining)
                if result.has_activity:
                    logger.info("Tick: %s", result.describe())
            except Exception:  # noqa: BLE001 - one failed tick must not stop the daemon
                logger.exception("Daemon tick failed")
            try:
                control.write_state(
                    started_at=self.started_at,
                    in_flight_attempt_ids=self.pool.in_flight_attempt_ids(),
                    draining=draining,
                    version=tasque2.__version__,
                )
            except OSError:
                logger.exception("Could not write the daemon state file")
            if draining and self.pool.in_flight_count() == 0:
                return
            await self._sleep(draining)

    def _tick_once(self, *, claim: bool) -> TickResult:
        with session_scope() as session:
            return self.tick.run(session, max_claims=self.max_claims, claim=claim)

    async def _sleep(self, draining: bool) -> None:
        assert self._stop is not None
        if draining:
            await asyncio.sleep(self.settings.daemon_tick_seconds)
            return
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=self.settings.daemon_tick_seconds)
        except TimeoutError:
            pass

    def _start_discord(self):
        if not self.discord:
            return None, None
        from tasque2.discord.bot import TasqueBot, discord_configured

        if not discord_configured(self.settings):
            logger.info("Discord is not configured; running without it")
            return None, None
        try:
            bot = TasqueBot(self.settings)
        except RuntimeError as exc:
            logger.error("Discord disabled: %s", exc)
            return None, None
        return bot, asyncio.create_task(_run_bot(bot, self.settings.discord_token or ""))

    def _install_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> Callable[[], None]:
        names = ["SIGINT", "SIGTERM", "SIGBREAK"]
        previous: dict[int, object] = {}

        def handler(_signum, _frame) -> None:
            loop.call_soon_threadsafe(self.request_stop)

        for name in names:
            number = getattr(signal, name, None)
            if number is None:
                continue
            try:
                previous[number] = signal.signal(number, handler)
            except (ValueError, OSError):
                continue

        def restore() -> None:
            for number, original in previous.items():
                try:
                    signal.signal(number, original)
                except (ValueError, OSError, TypeError):
                    pass

        return restore


async def _run_bot(bot, token: str) -> None:
    try:
        await bot.start(token)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - work keeps running when Discord is unavailable
        logger.exception("Discord client stopped; the daemon keeps running without it")


def _queue_counts() -> dict[str, int]:
    with session_scope() as session:
        return work_status_counts(session)


def serve(*, force: bool = False, discord: bool = True, max_claims: int | None = None) -> None:
    """Run the daemon in the foreground until it is stopped."""
    from tasque2.logs import configure_logging
    from tasque2.migrations import upgrade_database
    from tasque2.telemetry import configure_telemetry

    configure_logging()
    configure_telemetry("daemon")
    upgrade_database()
    reason = control.live_daemon_reason()
    if reason is not None and not force:
        raise DaemonAlreadyRunning(
            f"Another daemon looks alive ({reason}). Stop it first, or pass --force if it is gone."
        )
    control.clear_drain()
    asyncio.run(Daemon(discord=discord, max_claims=max_claims).run())
