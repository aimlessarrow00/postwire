import datetime
import enum
import uuid
from typing import Any, ClassVar

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    type_annotation_map: ClassVar[dict[Any, Any]] = {
        datetime.datetime: sa.DateTime(timezone=True),
    }


SCHEMA_NAME = "postwire"


class DeliveryStatus(enum.StrEnum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    RETRY = "RETRY"
    DONE = "DONE"
    SKIPPED = "SKIPPED"
    DEAD = "DEAD"


class AttemptStatus(enum.StrEnum):
    CLAIMED = "CLAIMED"
    SUCCEEDED = "SUCCEEDED"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    SKIPPED = "SKIPPED"
    FAILED = "FAILED"
    DEAD = "DEAD"
    RECLAIMED = "RECLAIMED"


class EventDB(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    topic: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    event_type: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, unique=True)
    key: Mapped[str | None] = mapped_column(sa.String(255), nullable=True)

    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    headers: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=sa.text("'{}'::jsonb"),
    )

    correlation_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    causation_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    source: Mapped[str | None] = mapped_column(sa.String(255), nullable=True)

    available_at: Mapped[datetime.datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC),
        server_default=sa.text("CURRENT_TIMESTAMP"),
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC),
        server_default=sa.text("CURRENT_TIMESTAMP"),
    )

    deliveries: Mapped[list["DeliveryDB"]] = relationship(
        "DeliveryDB", back_populates="event", cascade="all, delete-orphan"
    )

    __table_args__ = (
        sa.Index("ix_postwire_events_topic_id", "topic", "id"),
        sa.Index("ix_postwire_events_type_id", "event_type", "id"),
        sa.Index("ix_postwire_events_correlation_id", "correlation_id"),
        {"schema": SCHEMA_NAME},
    )


class SubscriptionDB(Base):
    __tablename__ = "subscriptions"

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(sa.String(255), nullable=False, unique=True)
    topics: Mapped[list[str]] = mapped_column(ARRAY(sa.String(255)), nullable=False)
    event_types: Mapped[list[str] | None] = mapped_column(ARRAY(sa.String(255)), nullable=True)
    enabled: Mapped[bool] = mapped_column(
        sa.Boolean, nullable=False, default=True, server_default=sa.text("true")
    )
    max_attempts: Mapped[int] = mapped_column(
        sa.Integer, nullable=False, default=5, server_default="5"
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC),
        server_default=sa.text("CURRENT_TIMESTAMP"),
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC),
        onupdate=lambda: datetime.datetime.now(datetime.UTC),
        server_default=sa.text("CURRENT_TIMESTAMP"),
    )

    deliveries: Mapped[list["DeliveryDB"]] = relationship(
        "DeliveryDB", back_populates="subscription", cascade="all, delete-orphan"
    )

    __table_args__ = (
        sa.Index("ix_postwire_subscriptions_topics", "topics", postgresql_using="gin"),
        {"schema": SCHEMA_NAME},
    )


class DeliveryDB(Base):
    __tablename__ = "deliveries"

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[int] = mapped_column(
        sa.ForeignKey(f"{SCHEMA_NAME}.events.id", ondelete="CASCADE"), nullable=False
    )
    subscription_id: Mapped[int] = mapped_column(
        sa.ForeignKey(f"{SCHEMA_NAME}.subscriptions.id", ondelete="CASCADE"), nullable=False
    )

    status: Mapped[DeliveryStatus] = mapped_column(
        sa.Enum(
            DeliveryStatus,
            name="postwire_delivery_status",
            schema=SCHEMA_NAME,
            values_callable=lambda statuses: [status.value for status in statuses],
        ),
        nullable=False,
        default=DeliveryStatus.PENDING,
        server_default=DeliveryStatus.PENDING.value,
    )
    attempts: Mapped[int] = mapped_column(sa.Integer, nullable=False, default=0, server_default="0")
    max_attempts: Mapped[int] = mapped_column(
        sa.Integer, nullable=False, default=5, server_default="5"
    )
    next_attempt_at: Mapped[datetime.datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC),
        server_default=sa.text("CURRENT_TIMESTAMP"),
    )

    locked_by: Mapped[str | None] = mapped_column(sa.String(255), nullable=True)
    locked_at: Mapped[datetime.datetime | None] = mapped_column(sa.DateTime(timezone=True))
    last_attempted_at: Mapped[datetime.datetime | None] = mapped_column(sa.DateTime(timezone=True))

    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    last_error: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    processed_at: Mapped[datetime.datetime | None] = mapped_column(sa.DateTime(timezone=True))
    dead_at: Mapped[datetime.datetime | None] = mapped_column(sa.DateTime(timezone=True))

    created_at: Mapped[datetime.datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC),
        server_default=sa.text("CURRENT_TIMESTAMP"),
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC),
        onupdate=lambda: datetime.datetime.now(datetime.UTC),
        server_default=sa.text("CURRENT_TIMESTAMP"),
    )

    event: Mapped[EventDB] = relationship("EventDB", back_populates="deliveries")
    subscription: Mapped[SubscriptionDB] = relationship(
        "SubscriptionDB", back_populates="deliveries"
    )
    attempts_log: Mapped[list["AttemptDB"]] = relationship(
        "AttemptDB", back_populates="delivery", cascade="all, delete-orphan"
    )

    __table_args__ = (
        sa.UniqueConstraint(
            "event_id", "subscription_id", name="uq_postwire_delivery_event_subscription"
        ),
        sa.Index(
            "ix_postwire_deliveries_claim",
            "subscription_id",
            "status",
            "next_attempt_at",
            "id",
            postgresql_where=sa.text("status IN ('PENDING', 'RETRY')"),
        ),
        sa.Index(
            "ix_postwire_deliveries_locked",
            "status",
            "locked_at",
            postgresql_where=sa.text("status = 'PROCESSING'"),
        ),
        sa.Index(
            "ix_postwire_deliveries_dead",
            "subscription_id",
            "dead_at",
            postgresql_where=sa.text("status = 'DEAD'"),
        ),
        sa.Index(
            "ix_postwire_deliveries_done_retention",
            "processed_at",
            postgresql_where=sa.text("status IN ('DONE', 'SKIPPED')"),
        ),
        {"schema": SCHEMA_NAME},
    )


class AttemptDB(Base):
    __tablename__ = "attempts"

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    delivery_id: Mapped[int] = mapped_column(
        sa.ForeignKey(f"{SCHEMA_NAME}.deliveries.id", ondelete="CASCADE"), nullable=False
    )
    attempt_num: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    worker_id: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    status: Mapped[AttemptStatus] = mapped_column(
        sa.Enum(
            AttemptStatus,
            name="postwire_attempt_status",
            schema=SCHEMA_NAME,
            values_callable=lambda statuses: [status.value for status in statuses],
        ),
        nullable=False,
    )
    error: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    started_at: Mapped[datetime.datetime | None] = mapped_column(sa.DateTime(timezone=True))
    finished_at: Mapped[datetime.datetime | None] = mapped_column(sa.DateTime(timezone=True))
    created_at: Mapped[datetime.datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC),
        server_default=sa.text("CURRENT_TIMESTAMP"),
    )

    delivery: Mapped[DeliveryDB] = relationship("DeliveryDB", back_populates="attempts_log")

    __table_args__ = (
        sa.Index("ix_postwire_attempts_delivery_id", "delivery_id", "id"),
        {"schema": SCHEMA_NAME},
    )
