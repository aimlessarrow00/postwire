"""Verify ``dedupe_by_key`` (used inside ``claim_batch``) keeps at most one
delivery per non-None key, preserving order, and lets unkeyed deliveries
pass through unchanged.

This protects the global "at most one handler per partition key" guarantee
within a single claim batch — the SQL exclusion subquery only handles the
cross-batch case (where the conflicting delivery is already PROCESSING)."""

from postwire.repository import DueDelivery, dedupe_by_key


def make(delivery_id: int, key: str | None) -> DueDelivery:
    return DueDelivery(
        delivery_id=delivery_id,
        event_id=delivery_id,
        event_type="x",
        key=key,
        payload={},
        attempts=0,
    )


def test_dedupe_drops_repeated_keys_keeps_first() -> None:
    deliveries = [
        make(1, "A"),
        make(2, "A"),
        make(3, "B"),
        make(4, "A"),
        make(5, "C"),
    ]
    result = dedupe_by_key(deliveries)
    assert [d.delivery_id for d in result] == [1, 3, 5]


def test_dedupe_passes_unkeyed_through() -> None:
    """``key is None`` events have no serialization constraint — multiple
    can run in parallel within one batch."""
    deliveries = [
        make(1, None),
        make(2, None),
        make(3, "A"),
        make(4, "A"),
        make(5, None),
    ]
    result = dedupe_by_key(deliveries)
    assert [d.delivery_id for d in result] == [1, 2, 3, 5]


def test_dedupe_empty() -> None:
    assert dedupe_by_key([]) == []


def test_dedupe_all_distinct_keys() -> None:
    deliveries = [make(i, f"k{i}") for i in range(1, 6)]
    result = dedupe_by_key(deliveries)
    assert [d.delivery_id for d in result] == [1, 2, 3, 4, 5]


def test_dedupe_all_same_key_collapses_to_one() -> None:
    deliveries = [make(i, "lead-1") for i in range(1, 11)]
    result = dedupe_by_key(deliveries)
    assert [d.delivery_id for d in result] == [1]


def test_dedupe_preserves_input_order() -> None:
    """The list_due_deliveries query orders by id; dedup must preserve that
    so the lowest-id delivery per key wins (oldest first)."""
    deliveries = [
        make(10, "B"),
        make(11, "A"),
        make(12, "C"),
        make(13, "B"),
        make(14, "A"),
    ]
    result = dedupe_by_key(deliveries)
    assert [d.delivery_id for d in result] == [10, 11, 12]
