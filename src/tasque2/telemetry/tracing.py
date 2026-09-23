from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace import Span, SpanKind, Status, StatusCode

import tasque2

_SCOPE = "tasque2"


def get_tracer() -> trace.Tracer:
    return trace.get_tracer(_SCOPE, tasque2.__version__)


@contextmanager
def span(
    name: str,
    *,
    attributes: Mapping[str, Any] | None = None,
    kind: SpanKind = SpanKind.INTERNAL,
    context: Context | None = None,
) -> Iterator[Span]:
    """Start a span as the current span; an escaping exception marks it as an error."""
    with get_tracer().start_as_current_span(
        name,
        kind=kind,
        context=context,
        attributes=clean_attributes(attributes),
        record_exception=False,
        set_status_on_exception=False,
    ) as current:
        try:
            yield current
        except BaseException as exc:
            record_exception(current, exc)
            raise


def record_exception(current: Span, exc: BaseException) -> None:
    current.record_exception(exc)
    current.set_attribute("error.type", type(exc).__name__)
    current.set_status(Status(StatusCode.ERROR, str(exc)[:500]))


def clean_attributes(attributes: Mapping[str, Any] | None) -> dict[str, Any]:
    """Drop None values and coerce anything the SDK cannot store to a string."""
    if not attributes:
        return {}
    cleaned: dict[str, Any] = {}
    for key, value in attributes.items():
        if value is None:
            continue
        if isinstance(value, bool | int | float | str):
            cleaned[key] = value
        elif isinstance(value, list | tuple) and all(isinstance(item, str) for item in value):
            cleaned[key] = list(value)
        else:
            cleaned[key] = str(value)
    return cleaned
