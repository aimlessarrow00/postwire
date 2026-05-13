import datetime
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from postwire.event import Event
from postwire.models import (
    AttemptDB,
    AttemptStatus,
    DeliveryDB,
    DeliveryStatus,
    EventDB,
    SubscriptionDB,
)


def utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


@dataclass(frozen=True)
class DueDelivery:
    delivery_id: int
    event_id: int
    event_type: str
    key: str | None
    payload: dict[str, Any]
    attempts: int


@dataclass(frozen=True)
class ClaimedDelivery:
    delivery_id: int
    event_id: int
    event_type: str
    key: str | None
    payload: dict[str, Any]
    attempt_num: int


def dedupe_by_key(deliveries: list[DueDelivery]) -> list[DueDelivery]:
    """Drop within-batch duplicates of the same key (first wins).

    The cross-worker partition-key guard in ``list_due_deliveries`` only blocks
    keys already PROCESSING; a single batch can still see multiple PENDING rows
    for the same key. Dropped rows stay PENDING and get picked up later.
    Unkeyed rows pass through unchanged.
    """
    seen_keys: set[str] = set()
    result: list[DueDelivery] = []
    for delivery in deliveries:
        if delivery.key is not None:
            if delivery.key in seen_keys:
                continue
            seen_keys.add(delivery.key)
        result.append(delivery)
    return result


class EventRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @property
    def session(self) -> AsyncSession:
        return self._session

    async def create_event(
        self,
        *,
        topic: str,
        event: Event,
        key: str | None = None,
        available_at: datetime.datetime | None = None,
        source: str | None = None,
    ) -> EventDB:
        now = utcnow()
        effective_key = key if key is not None else event.partition_key()
        insert_stmt = (
            pg_insert(EventDB)
            .values(
                topic=topic,
                event_type=event.event_type,
                event_id=event.event_id,
                key=effective_key,
                payload=event.payload(),
                headers=dict(event.headers),
                correlation_id=event.correlation_id,
                causation_id=event.causation_id,
                source=source,
                available_at=available_at or now,
                created_at=now,
            )
            .on_conflict_do_nothing(index_elements=[EventDB.event_id])
        )
        await self._session.execute(insert_stmt)
        result = await self._session.execute(
            sa.select(EventDB).where(EventDB.event_id == event.event_id)
        )
        return result.scalar_one()

    async def ensure_subscription(
        self,
        *,
        name: str,
        topics: list[str],
        event_types: list[str] | None = None,
        max_attempts: int = 5,
    ) -> SubscriptionDB:
        now = utcnow()
        stmt = (
            pg_insert(SubscriptionDB)
            .values(
                name=name,
                topics=topics,
                event_types=event_types,
                max_attempts=max_attempts,
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_update(
                index_elements=[SubscriptionDB.name],
                set_={
                    "topics": topics,
                    "event_types": event_types,
                    "max_attempts": max_attempts,
                    "updated_at": now,
                },
            )
            .returning(SubscriptionDB)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.scalar_one()

    async def publish_event(self, event: EventDB, *, max_attempts: int) -> None:
        result = await self._session.execute(
            sa.select(SubscriptionDB).where(
                SubscriptionDB.enabled.is_(True),
                SubscriptionDB.topics.contains([event.topic]),
                sa.or_(
                    SubscriptionDB.event_types.is_(None),
                    SubscriptionDB.event_types.contains([event.event_type]),
                ),
            )
        )
        subscriptions = list(result.scalars().all())
        if not subscriptions:
            return

        rows = [
            {
                "event_id": event.id,
                "subscription_id": subscription.id,
                "status": DeliveryStatus.PENDING,
                "attempts": 0,
                "max_attempts": max_attempts,
                "next_attempt_at": event.available_at,
                "created_at": event.created_at,
                "updated_at": event.created_at,
            }
            for subscription in subscriptions
        ]
        await self._session.execute(
            pg_insert(DeliveryDB)
            .values(rows)
            .on_conflict_do_nothing(
                constraint="uq_postwire_delivery_event_subscription",
            )
        )
        await self._session.flush()

    async def list_due_deliveries(
        self, *, subscription_name: str, batch_size: int
    ) -> list[DueDelivery]:
        now = utcnow()
        # Globally locked keys: keys with at least one delivery currently
        # PROCESSING in any subscription. At most one handler per key runs
        # globally at a time (partition-key serialization).
        processing_keys = (
            sa.select(EventDB.key)
            .join(DeliveryDB, DeliveryDB.event_id == EventDB.id)
            .where(
                DeliveryDB.status == DeliveryStatus.PROCESSING,
                EventDB.key.is_not(None),
            )
        )
        result = await self._session.execute(
            sa.select(DeliveryDB, EventDB)
            .join(EventDB, DeliveryDB.event_id == EventDB.id)
            .join(SubscriptionDB, DeliveryDB.subscription_id == SubscriptionDB.id)
            .where(
                SubscriptionDB.name == subscription_name,
                SubscriptionDB.enabled.is_(True),
                DeliveryDB.status.in_([DeliveryStatus.PENDING, DeliveryStatus.RETRY]),
                EventDB.available_at <= now,
                DeliveryDB.next_attempt_at <= now,
                sa.or_(
                    EventDB.key.is_(None),
                    EventDB.key.not_in(processing_keys),
                ),
            )
            .order_by(DeliveryDB.id)
            .limit(batch_size)
            .with_for_update(skip_locked=True, of=DeliveryDB)
        )
        rows = list(result.all())
        return [
            DueDelivery(
                delivery_id=delivery.id,
                event_id=event.id,
                event_type=event.event_type,
                key=event.key,
                payload=dict(event.payload or {}),
                attempts=delivery.attempts,
            )
            for delivery, event in rows
        ]

    async def mark_claimed(self, delivery: DueDelivery, *, worker_id: str) -> ClaimedDelivery:
        now = utcnow()
        attempt_num = delivery.attempts + 1
        await self._session.execute(
            sa.update(DeliveryDB)
            .where(DeliveryDB.id == delivery.delivery_id)
            .values(
                status=DeliveryStatus.PROCESSING,
                locked_by=worker_id,
                locked_at=now,
                last_attempted_at=now,
                updated_at=now,
            )
        )
        await self.record_attempt(
            delivery_id=delivery.delivery_id,
            attempt_num=attempt_num,
            worker_id=worker_id,
            status=AttemptStatus.CLAIMED,
            started_at=now,
        )
        await self._session.flush()
        return ClaimedDelivery(
            delivery_id=delivery.delivery_id,
            event_id=delivery.event_id,
            event_type=delivery.event_type,
            key=delivery.key,
            payload=delivery.payload,
            attempt_num=attempt_num,
        )

    async def claim_batch(
        self, *, subscription_name: str, worker_id: str, batch_size: int
    ) -> list[ClaimedDelivery]:
        due_deliveries = await self.list_due_deliveries(
            subscription_name=subscription_name,
            batch_size=batch_size,
        )
        claimed: list[ClaimedDelivery] = []
        for delivery in dedupe_by_key(due_deliveries):
            claimed.append(await self.mark_claimed(delivery, worker_id=worker_id))
        await self._session.flush()
        return claimed

    async def record_attempt(
        self,
        *,
        delivery_id: int,
        attempt_num: int,
        worker_id: str,
        status: AttemptStatus,
        error: str | None = None,
        result: dict[str, Any] | None = None,
        started_at: datetime.datetime | None = None,
        finished_at: datetime.datetime | None = None,
    ) -> None:
        self._session.add(
            AttemptDB(
                delivery_id=delivery_id,
                attempt_num=attempt_num,
                worker_id=worker_id,
                status=status,
                error=error,
                result=result,
                started_at=started_at,
                finished_at=finished_at,
            )
        )
        await self._session.flush()

    async def mark_done(
        self,
        delivery_id: int,
        *,
        worker_id: str,
        attempt_num: int,
        result: dict[str, Any] | None = None,
    ) -> None:
        now = utcnow()
        await self._session.execute(
            sa.update(DeliveryDB)
            .where(DeliveryDB.id == delivery_id)
            .values(
                status=DeliveryStatus.DONE,
                attempts=attempt_num,
                locked_by=None,
                locked_at=None,
                result=result,
                processed_at=now,
                updated_at=now,
            )
        )
        await self.record_attempt(
            delivery_id=delivery_id,
            attempt_num=attempt_num,
            worker_id=worker_id,
            status=AttemptStatus.SUCCEEDED,
            result=result,
            finished_at=now,
        )
        await self._session.flush()

    async def mark_skipped(
        self,
        delivery_id: int,
        *,
        worker_id: str,
        attempt_num: int,
        result: dict[str, Any] | None = None,
    ) -> None:
        now = utcnow()
        await self._session.execute(
            sa.update(DeliveryDB)
            .where(DeliveryDB.id == delivery_id)
            .values(
                status=DeliveryStatus.SKIPPED,
                attempts=attempt_num,
                locked_by=None,
                locked_at=None,
                result=result,
                processed_at=now,
                updated_at=now,
            )
        )
        await self.record_attempt(
            delivery_id=delivery_id,
            attempt_num=attempt_num,
            worker_id=worker_id,
            status=AttemptStatus.SKIPPED,
            result=result,
            finished_at=now,
        )
        await self._session.flush()

    async def mark_dead(
        self, delivery_id: int, *, worker_id: str, attempt_num: int, error: str
    ) -> None:
        now = utcnow()
        await self._session.execute(
            sa.update(DeliveryDB)
            .where(DeliveryDB.id == delivery_id)
            .values(
                status=DeliveryStatus.DEAD,
                attempts=attempt_num,
                locked_by=None,
                locked_at=None,
                last_error=error,
                dead_at=now,
                updated_at=now,
            )
        )
        await self.record_attempt(
            delivery_id=delivery_id,
            attempt_num=attempt_num,
            worker_id=worker_id,
            status=AttemptStatus.DEAD,
            error=error,
            finished_at=now,
        )
        await self._session.flush()

    async def get_delivery_for_update(self, delivery_id: int) -> DeliveryDB:
        result = await self._session.execute(
            sa.select(DeliveryDB).where(DeliveryDB.id == delivery_id).with_for_update()
        )
        return result.scalar_one()

    async def mark_retry(
        self,
        delivery: DeliveryDB,
        *,
        worker_id: str,
        attempt_num: int,
        delay_seconds: int,
        error: str,
    ) -> None:
        now = utcnow()
        delivery.status = DeliveryStatus.RETRY
        delivery.attempts = attempt_num
        delivery.next_attempt_at = now + datetime.timedelta(seconds=delay_seconds)
        delivery.locked_by = None
        delivery.locked_at = None
        delivery.last_error = error
        delivery.updated_at = now
        await self.record_attempt(
            delivery_id=delivery.id,
            attempt_num=attempt_num,
            worker_id=worker_id,
            status=AttemptStatus.RETRY_SCHEDULED,
            error=error,
            finished_at=now,
        )
        await self._session.flush()

    async def reclaim_stale_processing(self, *, lock_timeout: datetime.timedelta) -> int:
        now = utcnow()
        stale = (
            await self._session.execute(
                sa.select(
                    DeliveryDB.id,
                    DeliveryDB.attempts,
                    DeliveryDB.max_attempts,
                    DeliveryDB.locked_by,
                )
                .where(
                    DeliveryDB.status == DeliveryStatus.PROCESSING,
                    DeliveryDB.locked_at < now - lock_timeout,
                )
                .with_for_update(skip_locked=True)
            )
        ).all()
        if not stale:
            return 0

        retry_rows: list[Any] = []
        dead_rows: list[Any] = []
        for row in stale:
            if row.attempts + 1 >= row.max_attempts:
                dead_rows.append(row)
            else:
                retry_rows.append(row)

        if retry_rows:
            await self._session.execute(
                sa.update(DeliveryDB)
                .where(DeliveryDB.id.in_([r.id for r in retry_rows]))
                .values(
                    status=DeliveryStatus.RETRY,
                    attempts=DeliveryDB.attempts + 1,
                    locked_by=None,
                    locked_at=None,
                    next_attempt_at=now,
                    updated_at=now,
                )
            )

        if dead_rows:
            await self._session.execute(
                sa.update(DeliveryDB)
                .where(DeliveryDB.id.in_([r.id for r in dead_rows]))
                .values(
                    status=DeliveryStatus.DEAD,
                    attempts=DeliveryDB.attempts + 1,
                    locked_by=None,
                    locked_at=None,
                    last_error="stale lock reclaimed; retries exhausted",
                    dead_at=now,
                    updated_at=now,
                )
            )

        attempt_rows: list[dict[str, Any]] = []
        for row in retry_rows:
            attempt_rows.append(
                {
                    "delivery_id": row.id,
                    "attempt_num": row.attempts + 1,
                    "worker_id": row.locked_by or "unknown",
                    "status": AttemptStatus.RECLAIMED,
                    "error": "stale lock reclaimed",
                    "finished_at": now,
                    "created_at": now,
                }
            )
        for row in dead_rows:
            attempt_rows.append(
                {
                    "delivery_id": row.id,
                    "attempt_num": row.attempts + 1,
                    "worker_id": row.locked_by or "unknown",
                    "status": AttemptStatus.DEAD,
                    "error": "stale lock reclaimed; retries exhausted",
                    "finished_at": now,
                    "created_at": now,
                }
            )
        await self._session.execute(sa.insert(AttemptDB), attempt_rows)
        await self._session.flush()
        return len(stale)

    async def delete_attempts_before(self, cutoff: datetime.datetime, *, batch_size: int) -> int:
        old_attempts = (
            sa.select(AttemptDB.id)
            .where(AttemptDB.created_at < cutoff)
            .order_by(AttemptDB.id)
            .limit(batch_size)
            .cte("old_attempts")
        )
        result = await self._session.execute(
            sa.delete(AttemptDB)
            .where(AttemptDB.id.in_(sa.select(old_attempts.c.id)))
            .returning(AttemptDB.id)
        )
        await self._session.flush()
        return len(result.scalars().all())

    async def delete_deliveries_before(
        self,
        cutoff: datetime.datetime,
        *,
        statuses: tuple[DeliveryStatus, ...],
        timestamp_column: Any,
        batch_size: int,
    ) -> int:
        old_deliveries = (
            sa.select(DeliveryDB.id)
            .where(
                DeliveryDB.status.in_(statuses),
                timestamp_column < cutoff,
            )
            .order_by(DeliveryDB.id)
            .limit(batch_size)
            .cte("old_deliveries")
        )
        result = await self._session.execute(
            sa.delete(DeliveryDB)
            .where(DeliveryDB.id.in_(sa.select(old_deliveries.c.id)))
            .returning(DeliveryDB.id)
        )
        await self._session.flush()
        return len(result.scalars().all())


class RepositoryFactory:
    """``begin()`` for Postwire's tx-scoped state writes; ``session()`` for
    handler use — the handler owns commits there."""

    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
        self._sessionmaker = sessionmaker

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[EventRepository]:
        async with self._sessionmaker.begin() as session:
            yield EventRepository(session)

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        async with self._sessionmaker() as session:
            yield session


__all__ = [
    "ClaimedDelivery",
    "DueDelivery",
    "EventRepository",
    "RepositoryFactory",
    "utcnow",
]
