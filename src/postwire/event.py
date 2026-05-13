import inspect
import types
import uuid
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import (
    Annotated,
    Any,
    ClassVar,
    Literal,
    Protocol,
    Union,
    get_args,
    get_origin,
    runtime_checkable,
)

from pydantic import BaseModel, Field, TypeAdapter
from sqlalchemy.ext.asyncio import AsyncSession

_ENVELOPE_FIELDS = {
    "event_type",
    "event_id",
    "correlation_id",
    "causation_id",
    "headers",
}


class Event(BaseModel):
    """Subclass with ``event_type: Literal["..."] = "..."`` — Postwire uses
    that field as the discriminator for routing and handler dispatch."""

    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    correlation_id: uuid.UUID | None = None
    causation_id: uuid.UUID | None = None
    headers: dict[str, Any] = Field(default_factory=dict)
    event_type: Literal["default"] = "default"

    def payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude=_ENVELOPE_FIELDS)

    def partition_key(self) -> str | None:
        """Return a key to serialize handlers per entity: at most one delivery
        per key runs at a time, across all subscriptions. The ``key=`` kwarg
        on ``Postwire.publish`` overrides this."""
        return None


EventHandlerFn = Callable[..., Awaitable[None]]
"""Async ``(event, session) -> None``. The first parameter's annotation declares
the accepted event type; a union is auto-discriminated by ``event_type``."""


@runtime_checkable
class ResolvedHandler(Protocol):
    """What ``Consumer`` calls. Satisfied by ``EventHandler`` and the function wrapper."""

    def load_event(self, event_type: str, body: dict[str, Any]) -> Any: ...

    async def run(self, event: Any, session: AsyncSession) -> None: ...


def _build_event_adapter(annotation: Any) -> TypeAdapter[Any]:
    origin = get_origin(annotation)
    if origin is Union or origin is types.UnionType:
        annotation = Annotated[annotation, Field(discriminator="event_type")]
    return TypeAdapter(annotation)


class EventHandler[E: Event](ABC):
    """Subclass ``EventHandler[MyEvent]`` (or ``EventHandler[A | B]`` for a union).

    Delivery is at-least-once — ``run`` must be idempotent. The ``session``
    is handler-owned and runs in a separate transaction from Postwire's
    state machine, so a raise from ``run`` triggers retry regardless of
    what the handler already committed.
    """

    _adapter: ClassVar[TypeAdapter[Any] | None] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for base in getattr(cls, "__orig_bases__", ()):
            if get_origin(base) is EventHandler:
                (param,) = get_args(base)
                cls._adapter = _build_event_adapter(param)
                break
        if not cls._adapter:
            raise RuntimeError("Failed to set TypeAdapter")

    def load_event(self, event_type: str, body: dict[str, Any]) -> E:
        assert self._adapter is not None  # set by __init_subclass__
        return self._adapter.validate_python({**body, "event_type": event_type})

    @abstractmethod
    async def run(self, event: E, session: AsyncSession) -> None: ...


class _FunctionHandler:
    """Wraps an async function as a ``ResolvedHandler``. The event type is
    inferred from the function's first parameter annotation."""

    def __init__(self, fn: EventHandlerFn) -> None:
        self._fn = fn
        params = list(inspect.signature(fn).parameters.values())
        if not params:
            raise TypeError(f"handler {fn!r} must accept (event, session); got zero parameters")
        annotation = params[0].annotation
        if annotation is inspect.Parameter.empty:
            name = getattr(fn, "__qualname__", repr(fn))
            raise TypeError(
                f"handler {name}: first parameter must be annotated with the "
                "Event subclass (or a Union of subclasses)"
            )
        self._adapter = _build_event_adapter(annotation)

    def load_event(self, event_type: str, body: dict[str, Any]) -> Any:
        return self._adapter.validate_python({**body, "event_type": event_type})

    async def run(self, event: Any, session: AsyncSession) -> None:
        await self._fn(event, session)


def coerce_handler(
    handler: "EventHandler[Any] | EventHandlerFn",
) -> ResolvedHandler:
    if isinstance(handler, EventHandler):
        return handler
    if callable(handler):
        return _FunctionHandler(handler)
    raise TypeError(
        f"handler must be an EventHandler instance or async callable, got {type(handler).__name__}"
    )
