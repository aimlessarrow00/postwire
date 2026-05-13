"""Verify ``EventHandler`` dispatches a union type parameter via the
``event_type`` discriminator auto-applied in ``__init_subclass__``."""

from typing import Literal

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from postwire.event import Event, EventHandler


class EventA(Event):
    event_type: Literal["evt.a"] = "evt.a"
    a_value: int


class EventB(Event):
    event_type: Literal["evt.b"] = "evt.b"
    b_value: str


class EventC(Event):
    event_type: Literal["evt.c"] = "evt.c"
    c_value: bool


class UnionHandler(EventHandler[EventA | EventB | EventC]):
    async def run(self, event: EventA | EventB | EventC, session: AsyncSession) -> None:
        pass


def test_dispatches_event_a() -> None:
    handler = UnionHandler()
    event = handler.load_event("evt.a", {"a_value": 42})
    assert isinstance(event, EventA)
    assert event.a_value == 42


def test_dispatches_event_b() -> None:
    handler = UnionHandler()
    event = handler.load_event("evt.b", {"b_value": "hello"})
    assert isinstance(event, EventB)
    assert event.b_value == "hello"


def test_dispatches_event_c() -> None:
    handler = UnionHandler()
    event = handler.load_event("evt.c", {"c_value": True})
    assert isinstance(event, EventC)
    assert event.c_value is True


def test_rejects_unknown_event_type() -> None:
    handler = UnionHandler()
    with pytest.raises(ValidationError):
        handler.load_event("evt.unknown", {})


def test_rejects_body_mismatched_with_event_type() -> None:
    """event_type 'evt.a' requires `a_value`; passing `b_value` must fail."""
    handler = UnionHandler()
    with pytest.raises(ValidationError):
        handler.load_event("evt.a", {"b_value": "wrong"})
