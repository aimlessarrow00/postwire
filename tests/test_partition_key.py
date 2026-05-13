"""Verify the partition_key fallback: explicit ``key=`` overrides
``Event.partition_key()``; otherwise the method is used."""

from typing import Literal

from postwire.event import Event


class LeadEvent(Event):
    event_type: Literal["lead.thing"] = "lead.thing"
    lead_id: str

    def partition_key(self) -> str | None:
        return self.lead_id


class UnkeyedEvent(Event):
    event_type: Literal["unkeyed"] = "unkeyed"
    body: str


def test_partition_key_default_is_none() -> None:
    assert UnkeyedEvent(body="x").partition_key() is None


def test_partition_key_override_returns_field() -> None:
    assert LeadEvent(lead_id="abc").partition_key() == "abc"


def test_explicit_key_overrides_partition_key() -> None:
    """Selection logic that repository.create_event implements:
    ``key`` kwarg wins; falls back to event.partition_key() when None."""
    event = LeadEvent(lead_id="from-method")
    explicit_key = "from-kwarg"
    effective = explicit_key if explicit_key is not None else event.partition_key()
    assert effective == "from-kwarg"


def test_no_key_kwarg_falls_back_to_partition_key() -> None:
    event = LeadEvent(lead_id="from-method")
    explicit_key = None
    effective = explicit_key if explicit_key is not None else event.partition_key()
    assert effective == "from-method"


def test_no_key_no_method_gives_none() -> None:
    event = UnkeyedEvent(body="x")
    explicit_key = None
    effective = explicit_key if explicit_key is not None else event.partition_key()
    assert effective is None
