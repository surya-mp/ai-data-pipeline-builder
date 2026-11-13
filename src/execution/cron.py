from __future__ import annotations

from datetime import datetime


FIELD_RANGES = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 6))


def validate_cron(expression: str) -> None:
    fields = expression.split()
    if len(fields) != 5:
        raise ValueError("Schedules must use a five-field cron expression.")
    for field, bounds in zip(fields, FIELD_RANGES):
        _parse_field(field, *bounds)


def cron_matches(expression: str, now: datetime) -> bool:
    validate_cron(expression)
    values = (now.minute, now.hour, now.day, now.month, (now.weekday() + 1) % 7)
    return all(value in _parse_field(field, *bounds) for field, value, bounds in zip(expression.split(), values, FIELD_RANGES))


def _parse_field(field: str, minimum: int, maximum: int) -> set[int]:
    if not field:
        raise ValueError("Cron fields cannot be empty.")
    values: set[int] = set()
    for part in field.split(","):
        if part == "*":
            values.update(range(minimum, maximum + 1))
            continue
        if "-" in part:
            pieces = part.split("-", 1)
            if len(pieces) != 2 or not all(piece.isdigit() for piece in pieces):
                raise ValueError(f"Invalid cron range '{part}'.")
            start, end = (int(piece) for piece in pieces)
            if start > end or start < minimum or end > maximum:
                raise ValueError(f"Cron range '{part}' is outside {minimum}-{maximum}.")
            values.update(range(start, end + 1))
            continue
        if not part.isdigit() or not minimum <= int(part) <= maximum:
            raise ValueError(f"Cron value '{part}' is outside {minimum}-{maximum}.")
        values.add(int(part))
    return values
