import asyncio
import socket
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, cast

import structlog

from postwire.errors import MalformedEvent, PermanentEventError
from postwire.event import EventHandler, EventHandlerFn, ResolvedHandler, coerce_handler
from postwire.repository import ClaimedDelivery, EventRepository, RepositoryFactory

logger = structlog.get_logger()


@dataclass(frozen=True)
class RetryConfig:
    delays_seconds: list[int] = field(default_factory=lambda: [30, 60, 120, 240, 480])

    @property
    def max_attempts(self) -> int:
        return len(self.delays_seconds)

    def delay_for_attempt(self, attempts: int) -> int | None:
        if attempts < 0 or attempts >= len(self.delays_seconds):
            return None
        return self.delays_seconds[attempts]


@dataclass(frozen=True)
class ConsumerConfig:
    """``handler`` accepts an ``EventHandler`` instance (when you need state)
    or an async function ``(event, session) -> None`` (otherwise)."""

    name: str
    topics: list[str]
    handler: EventHandler[Any] | EventHandlerFn
    event_types: list[str] | None = None
    retry_config: RetryConfig = field(default_factory=RetryConfig)
    max_concurrency: int = 3
    batch_size: int = 5

    def __post_init__(self) -> None:
        object.__setattr__(self, "handler", coerce_handler(self.handler))

    @property
    def resolved_handler(self) -> ResolvedHandler:
        return cast(ResolvedHandler, self.handler)


class Consumer:
    def __init__(
        self,
        repos: RepositoryFactory,
        config: ConsumerConfig,
        *,
        worker_id: str | None = None,
        poll_interval_seconds: float = 1.0,
    ) -> None:
        self._repos = repos
        self._config = config
        self._worker_id = worker_id or f"{socket.gethostname()}:{uuid.uuid4()}"
        self._poll_interval_seconds = poll_interval_seconds

    @property
    def config(self) -> ConsumerConfig:
        return self._config

    async def run_once(self) -> int:
        async with self._repos.begin() as repo:
            claims = await repo.claim_batch(
                subscription_name=self._config.name,
                worker_id=self._worker_id,
                batch_size=self._config.batch_size,
            )
        if not claims:
            return 0

        semaphore = asyncio.Semaphore(self._config.max_concurrency)
        await asyncio.gather(*(self._process(claim, semaphore) for claim in claims))
        return len(claims)

    async def _process(self, claim: ClaimedDelivery, semaphore: asyncio.Semaphore) -> None:
        with structlog.contextvars.bound_contextvars(
            subscription_name=self._config.name,
            topics=self._config.topics,
            worker_id=self._worker_id,
            delivery_id=claim.delivery_id,
            event_id=str(claim.event_id),
            event_type=claim.event_type,
            key=claim.key,
            attempt_num=claim.attempt_num,
        ):
            async with semaphore:
                error = await self._run_handler(claim)
                async with self._repos.begin() as repo:
                    await self._finalize(repo, claim, error)

    async def _run_handler(self, claim: ClaimedDelivery) -> Exception | None:
        handler = self._config.resolved_handler
        try:
            event = handler.load_event(claim.event_type, claim.payload)
        except Exception as e:
            logger.exception("consumer.load_failed")
            return MalformedEvent(f"malformed event: {e}")

        started_at = time.monotonic()
        logger.info("handler.start")
        try:
            async with self._repos.session() as handler_session:
                await handler.run(event, handler_session)
        except (MalformedEvent, PermanentEventError) as e:
            logger.exception(
                "handler.rejected",
                elapsed_ms=int((time.monotonic() - started_at) * 1000),
            )
            return e
        except Exception as e:
            logger.exception(
                "handler.raised",
                elapsed_ms=int((time.monotonic() - started_at) * 1000),
            )
            return e
        logger.info(
            "handler.end",
            elapsed_ms=int((time.monotonic() - started_at) * 1000),
        )
        return None

    async def _finalize(
        self,
        repo: EventRepository,
        claim: ClaimedDelivery,
        error: Exception | None,
    ) -> None:
        if error is None:
            await repo.mark_done(
                claim.delivery_id, worker_id=self._worker_id, attempt_num=claim.attempt_num
            )
        elif isinstance(error, (MalformedEvent, PermanentEventError)):
            await repo.mark_dead(
                claim.delivery_id,
                worker_id=self._worker_id,
                attempt_num=claim.attempt_num,
                error=str(error),
            )
        else:
            await self._retry_or_dead(
                repo,
                delivery_id=claim.delivery_id,
                attempt_num=claim.attempt_num,
                error=str(error),
            )

    async def run(self) -> None:
        while True:
            processed = await self.run_once()
            if processed == 0:
                await asyncio.sleep(self._poll_interval_seconds)

    async def _retry_or_dead(
        self,
        repo: EventRepository,
        *,
        delivery_id: int,
        attempt_num: int,
        error: str,
    ) -> None:
        delivery = await repo.get_delivery_for_update(delivery_id)
        delay = self._config.retry_config.delay_for_attempt(attempt_num - 1)
        if delay is None or attempt_num >= delivery.max_attempts:
            logger.warning("consumer exhausted retries", max_attempts=delivery.max_attempts)
            await repo.mark_dead(
                delivery_id, worker_id=self._worker_id, attempt_num=attempt_num, error=error
            )
            return

        logger.warning("consumer will retry event", delay=delay)
        await repo.mark_retry(
            delivery,
            worker_id=self._worker_id,
            attempt_num=attempt_num,
            delay_seconds=delay,
            error=error,
        )


__all__ = ["Consumer", "ConsumerConfig", "RetryConfig"]
