import asyncio
import datetime
import socket
import uuid
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any, Self

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from postwire.consumer import Consumer, ConsumerConfig
from postwire.event import Event
from postwire.models import EventDB
from postwire.repository import RepositoryFactory
from postwire.retention import RetentionConfig, RetentionRunner

# Frozen dataclass — safe to share across instances.
_DEFAULT_RETENTION_CONFIG = RetentionConfig()


class Postwire:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        *,
        configs: Sequence[ConsumerConfig] = (),
        retention_config: RetentionConfig | None = _DEFAULT_RETENTION_CONFIG,
        poll_interval_seconds: float = 1.0,
        stale_lock_timeout: datetime.timedelta = datetime.timedelta(minutes=5),
        reclaim_poll_interval_seconds: float = 30.0,
        worker_id: str | None = None,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._repos = RepositoryFactory(sessionmaker)
        self._owned_engine: AsyncEngine | None = None
        self._worker_id = worker_id or f"{socket.gethostname()}:{uuid.uuid4()}"
        self._stale_lock_timeout = stale_lock_timeout
        self._reclaim_poll_interval_seconds = reclaim_poll_interval_seconds
        self._consumers = [
            Consumer(
                self._repos,
                config,
                worker_id=self._worker_id,
                poll_interval_seconds=poll_interval_seconds,
            )
            for config in configs
        ]
        self._retention_runner = (
            RetentionRunner(self._repos, retention_config) if retention_config is not None else None
        )

    @classmethod
    def from_url(
        cls,
        database_url: str,
        *,
        configs: Sequence[ConsumerConfig] = (),
        retention_config: RetentionConfig | None = _DEFAULT_RETENTION_CONFIG,
        poll_interval_seconds: float = 1.0,
        stale_lock_timeout: datetime.timedelta = datetime.timedelta(minutes=5),
        reclaim_poll_interval_seconds: float = 30.0,
        worker_id: str | None = None,
    ) -> Self:
        engine = create_async_engine(database_url, pool_pre_ping=True)
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        postwire = cls(
            sessionmaker,
            configs=configs,
            retention_config=retention_config,
            poll_interval_seconds=poll_interval_seconds,
            stale_lock_timeout=stale_lock_timeout,
            reclaim_poll_interval_seconds=reclaim_poll_interval_seconds,
            worker_id=worker_id,
        )
        postwire._owned_engine = engine
        return postwire

    @property
    def sessionmaker(self) -> async_sessionmaker[AsyncSession]:
        return self._sessionmaker

    @property
    def repos(self) -> RepositoryFactory:
        return self._repos

    @property
    def consumers(self) -> list[Consumer]:
        return list(self._consumers)

    async def publish(
        self,
        topic: str,
        event: Event,
        *,
        key: str | None = None,
        available_at: datetime.datetime | None = None,
        max_attempts: int = 5,
        source: str | None = None,
    ) -> EventDB:
        async with self._repos.begin() as repo:
            row = await repo.create_event(
                topic=topic,
                event=event,
                key=key,
                available_at=available_at,
                source=source,
            )
            await repo.publish_event(row, max_attempts=max_attempts)
            return row

    async def ensure_subscriptions(self) -> None:
        async with self._repos.begin() as repo:
            for consumer in self._consumers:
                await repo.ensure_subscription(
                    name=consumer.config.name,
                    topics=consumer.config.topics,
                    event_types=consumer.config.event_types,
                    max_attempts=consumer.config.retry_config.max_attempts,
                )

    async def reclaim_stale(self) -> int:
        async with self._repos.begin() as repo:
            return await repo.reclaim_stale_processing(lock_timeout=self._stale_lock_timeout)

    async def run_once(self) -> int:
        """Does not start the reclaim/retention loops — those only run under ``run()``."""
        await self.ensure_subscriptions()
        if not self._consumers:
            return 0
        results = await asyncio.gather(*(c.run_once() for c in self._consumers))
        return sum(results)

    async def run(self) -> None:
        if not self._consumers:
            raise RuntimeError(
                "Postwire.run() requires at least one ConsumerConfig; "
                "for publish-only use, call publish() directly without run()."
            )
        await self.ensure_subscriptions()
        async with self._background():
            await asyncio.gather(*(c.run() for c in self._consumers))

    @asynccontextmanager
    async def _background(self) -> AsyncIterator[None]:
        tasks: list[asyncio.Task[Any]] = [
            asyncio.create_task(
                self._reclaim_loop(),
                name=f"postwire-reclaim:{self._worker_id}",
            )
        ]
        if self._retention_runner is not None:
            tasks.append(
                asyncio.create_task(
                    self._retention_runner.run(),
                    name=f"postwire-retention:{self._worker_id}",
                )
            )
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _reclaim_loop(self) -> None:
        while True:
            reclaimed = await self.reclaim_stale()
            if reclaimed == 0:
                await asyncio.sleep(self._reclaim_poll_interval_seconds)

    async def close(self) -> None:
        """Disposes the engine only if Postwire created it (via ``from_url``)."""
        if self._owned_engine is not None:
            await self._owned_engine.dispose()
            self._owned_engine = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()


__all__ = ["Postwire"]
