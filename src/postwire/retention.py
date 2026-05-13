import asyncio
import datetime
from dataclasses import dataclass
from typing import Any

import structlog

from postwire.models import DeliveryDB, DeliveryStatus
from postwire.repository import RepositoryFactory, utcnow

logger = structlog.get_logger()


@dataclass(frozen=True)
class RetentionConfig:
    attempts_retention: datetime.timedelta = datetime.timedelta(days=7)
    deliveries_retention: datetime.timedelta = datetime.timedelta(days=14)
    dead_deliveries_retention: datetime.timedelta | None = None
    batch_size: int = 1000
    poll_interval_seconds: float = 3600.0


@dataclass(frozen=True)
class RetentionStats:
    attempts_deleted: int = 0
    completed_deliveries_deleted: int = 0
    dead_deliveries_deleted: int = 0

    @property
    def total_deleted(self) -> int:
        return (
            self.attempts_deleted + self.completed_deliveries_deleted + self.dead_deliveries_deleted
        )


class RetentionRunner:
    def __init__(
        self,
        repos: RepositoryFactory,
        config: RetentionConfig,
    ) -> None:
        self._repos = repos
        self._attempts_retention = config.attempts_retention
        self._deliveries_retention = config.deliveries_retention
        self._dead_deliveries_retention = (
            config.dead_deliveries_retention or config.deliveries_retention
        )
        self._batch_size = config.batch_size
        self._poll_interval_seconds = config.poll_interval_seconds

    async def run_once(self) -> RetentionStats:
        attempts_deleted = await self._retain_attempts()
        completed_deliveries_deleted = await self._retain_deliveries(
            statuses=(DeliveryStatus.DONE, DeliveryStatus.SKIPPED),
            retention=self._deliveries_retention,
            timestamp_column=DeliveryDB.processed_at,
        )
        dead_deliveries_deleted = await self._retain_deliveries(
            statuses=(DeliveryStatus.DEAD,),
            retention=self._dead_deliveries_retention,
            timestamp_column=DeliveryDB.dead_at,
        )
        stats = RetentionStats(
            attempts_deleted=attempts_deleted,
            completed_deliveries_deleted=completed_deliveries_deleted,
            dead_deliveries_deleted=dead_deliveries_deleted,
        )
        logger.info(
            "retention.pass_finished",
            attempts_deleted=stats.attempts_deleted,
            completed_deliveries_deleted=stats.completed_deliveries_deleted,
            dead_deliveries_deleted=stats.dead_deliveries_deleted,
            total_deleted=stats.total_deleted,
        )
        return stats

    async def run(self) -> None:
        while True:
            stats = await self.run_once()
            if stats.total_deleted == 0:
                await asyncio.sleep(self._poll_interval_seconds)

    async def _retain_attempts(self) -> int:
        cutoff = utcnow() - self._attempts_retention
        total_deleted = 0
        while True:
            async with self._repos.begin() as repo:
                deleted = await repo.delete_attempts_before(
                    cutoff,
                    batch_size=self._batch_size,
                )
            total_deleted += deleted
            if deleted < self._batch_size:
                break
        if total_deleted:
            logger.info(
                "retention.attempts_deleted",
                attempts_deleted=total_deleted,
                cutoff=cutoff,
            )
        return total_deleted

    async def _retain_deliveries(
        self,
        *,
        statuses: tuple[DeliveryStatus, ...],
        retention: datetime.timedelta,
        timestamp_column: Any,
    ) -> int:
        cutoff = utcnow() - retention
        total_deleted = 0
        while True:
            async with self._repos.begin() as repo:
                deleted = await repo.delete_deliveries_before(
                    cutoff,
                    statuses=statuses,
                    timestamp_column=timestamp_column,
                    batch_size=self._batch_size,
                )
            total_deleted += deleted
            if deleted < self._batch_size:
                break
        if total_deleted:
            logger.info(
                "retention.deliveries_deleted",
                statuses=[status.value for status in statuses],
                deleted=total_deleted,
                cutoff=cutoff,
            )
        return total_deleted


__all__ = ["RetentionRunner", "RetentionStats"]
