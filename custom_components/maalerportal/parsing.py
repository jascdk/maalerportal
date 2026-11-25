"""Parse numbers and timestamps as the Målerportal API sends them.

Kept free of Home Assistant imports so the logic can be unit tested on its own.
"""

from __future__ import annotations

from datetime import datetime, timezone
import math
import re

_NUMBER_JUNK = re.compile(r"[^\d.,\-]")


def parse_number(raw: object) -> float | None:
    """Return a float for an API value, or None if it is missing or not a number.

    Numbers pass through. Strings may carry a unit or spaces, and may use a
    decimal comma: "1234,56", "1.234,56" and "1,234.56" all give 1234.56. When
    both separators appear, the last one is the decimal separator.
    """
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        value = float(raw)
    elif isinstance(raw, str):
        text = _NUMBER_JUNK.sub("", raw.strip())
        if not text:
            return None
        if "," in text and "." in text:
            if text.rfind(",") > text.rfind("."):
                text = text.replace(".", "").replace(",", ".")
            else:
                text = text.replace(",", "")
        elif "," in text:
            # A lone comma is a decimal comma ("1234,56"); several are thousands
            # separators ("1,234,567").
            text = text.replace(",", ".") if text.count(",") == 1 else text.replace(",", "")
        try:
            value = float(text)
        except ValueError:
            return None
    else:
        return None
    if math.isnan(value) or math.isinf(value):
        return None
    return value


def parse_api_timestamp(raw: object) -> datetime | None:
    """Parse an API timestamp to an aware UTC datetime, or None.

    Accepts a "Z" suffix or an explicit offset. A timestamp without an offset is
    taken as UTC.
    """
    if not raw or not isinstance(raw, str):
        return None
    text = raw.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
