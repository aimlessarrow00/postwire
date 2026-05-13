"""E-commerce pub/sub example.

Two consumers run concurrently, each on its own topic. A publisher fans events
out to three topics — including ``carts``, which has no subscriber, so those
events sit in the outbox unconsumed.

Set ``POSTWIRE_DATABASE_URL`` to an async Postgres URL and run:

    uv run python examples/pubsub.py
"""

import asyncio
import contextlib
import os
import random
from typing import Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from postwire import ConsumerConfig, Event, Postwire, RetryConfig
from postwire.models import SCHEMA_NAME, Base


class OrderPlaced(Event):
    event_type: Literal["order.placed"] = "order.placed"
    order_id: str
    customer_id: str
    total_cents: int


class StockAdjusted(Event):
    event_type: Literal["stock.adjusted"] = "stock.adjusted"
    sku: str
    delta: int


class CartAbandoned(Event):
    event_type: Literal["cart.abandoned"] = "cart.abandoned"
    cart_id: str
    customer_id: str


async def order_handler(event: OrderPlaced, session: AsyncSession) -> None:
    print(
        f"[orders] {event.order_id} customer={event.customer_id} "
        f"total=${event.total_cents / 100:.2f}"
    )


async def stock_handler(event: StockAdjusted, session: AsyncSession) -> None:
    print(f"[stock]  {event.sku} delta={event.delta:+d}")


DATABASE_URL = os.environ.get(
    "POSTWIRE_DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/postwire",
)


async def publish_demo(postwire: Postwire, *, iterations: int = 60) -> None:
    rng = random.Random(0)
    topics = ("orders", "stock", "carts")
    for i in range(iterations):
        topic = rng.choices(topics, weights=[3, 2, 1])[0]
        if topic == "orders":
            await postwire.publish(
                "orders",
                OrderPlaced(
                    order_id=f"o-{i}",
                    customer_id=f"c-{rng.randint(1, 20)}",
                    total_cents=rng.randint(500, 25_000),
                ),
            )
        elif topic == "stock":
            await postwire.publish(
                "stock",
                StockAdjusted(sku=f"sku-{rng.randint(0, 9)}", delta=rng.choice([-2, -1, 1, 3])),
            )
        else:
            await postwire.publish(
                "carts",
                CartAbandoned(cart_id=f"cart-{i}", customer_id=f"c-{rng.randint(1, 20)}"),
            )
        await asyncio.sleep(rng.uniform(0.3, 1.5))


async def main() -> None:
    engine = create_async_engine(DATABASE_URL)
    sessionmaker = async_sessionmaker(engine, expire_on_commit=False)

    async with engine.begin() as conn:
        await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))
        await conn.run_sync(Base.metadata.create_all)

    postwire = Postwire(
        sessionmaker,
        configs=[
            ConsumerConfig(
                name="orders-worker",
                topics=["orders"],
                handler=order_handler,
                retry_config=RetryConfig(delays_seconds=[1, 2, 4]),
            ),
            ConsumerConfig(
                name="stock-worker",
                topics=["stock"],
                handler=stock_handler,
                retry_config=RetryConfig(delays_seconds=[1, 2, 4]),
            ),
        ],
        poll_interval_seconds=0.2,
    )

    consumers = asyncio.create_task(postwire.run())
    try:
        await publish_demo(postwire)
        await asyncio.sleep(5)
    finally:
        consumers.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consumers
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
