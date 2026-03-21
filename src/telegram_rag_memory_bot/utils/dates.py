"""
????: dates.py
???????? ??????? ??? ????????, ???????????? ? ?????????? ???
?? ??????, ???????? ???????? ? ????????????????? ?????.
"""

from __future__ import annotations

from datetime import date, datetime
import re

ISO_DATE_RE = re.compile(r"(?<!\d)(?P<date>\d{4}-\d{2}-\d{2})(?!\d)")
RANGE_RE = re.compile(r"(?P<start>\d{4}-\d{2}-\d{2})\.\.(?P<end>\d{4}-\d{2}-\d{2})")
DATE_PATTERNS = [
    re.compile(r"(?<!\d)(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})(?!\d)"),
    re.compile(r"(?<!\d)(?P<year>\d{4})/(?P<month>\d{2})/(?P<day>\d{2})(?!\d)"),
    re.compile(r"(?<!\d)(?P<day>\d{2})\.(?P<month>\d{2})\.(?P<year>\d{4})(?!\d)"),
    re.compile(r"(?<!\d)(?P<day>\d{2})-(?P<month>\d{2})-(?P<year>\d{4})(?!\d)"),
]
RU_MONTHS_GENITIVE = (
    "",
    "января",
    "февраля",
    "марта",
    "апреля",
    "мая",
    "июня",
    "июля",
    "августа",
    "сентября",
    "октября",
    "ноября",
    "декабря",
)


class DateParseError(ValueError):
    pass


def parse_iso_date(raw_value: str) -> str:
    value = raw_value.strip()
    if not value:
        raise DateParseError("Дата должна быть в формате DD-MM-YYYY.")
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            parsed = date.fromisoformat(value)
        elif re.fullmatch(r"\d{4}/\d{2}/\d{2}", value):
            year, month, day = value.split("/")
            parsed = date(int(year), int(month), int(day))
        elif re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", value):
            day, month, year = value.split(".")
            parsed = date(int(year), int(month), int(day))
        elif re.fullmatch(r"\d{2}-\d{2}-\d{4}", value):
            day, month, year = value.split("-")
            parsed = date(int(year), int(month), int(day))
        else:
            raise ValueError(value)
    except ValueError as exc:
        raise DateParseError("Дата должна быть в формате DD-MM-YYYY.") from exc
    return parsed.isoformat()


def extract_date_filters(text: str) -> tuple[str | None, str | None]:
    range_match = RANGE_RE.search(text)
    if range_match:
        return (
            parse_iso_date(range_match.group("start")),
            parse_iso_date(range_match.group("end")),
        )

    matches = find_all_dates(text)
    if len(matches) == 1:
        return matches[0], matches[0]
    if len(matches) >= 2:
        return matches[0], matches[1]
    return None, None


def find_first_date(text: str | None) -> str | None:
    matches = find_all_dates(text)
    return matches[0] if matches else None


def find_all_dates(text: str | None) -> list[str]:
    if not text:
        return []

    candidates: list[tuple[int, str]] = []
    for pattern in DATE_PATTERNS:
        for match in pattern.finditer(text):
            try:
                parsed = date(
                    int(match.group("year")),
                    int(match.group("month")),
                    int(match.group("day")),
                ).isoformat()
            except ValueError:
                continue
            candidates.append((match.start(), parsed))

    candidates.sort(key=lambda item: item[0])
    ordered_unique: list[str] = []
    seen: set[str] = set()
    for _, parsed in candidates:
        if parsed in seen:
            continue
        seen.add(parsed)
        ordered_unique.append(parsed)
    return ordered_unique


def today_iso() -> str:
    return datetime.now().astimezone().date().isoformat()


def format_display_date(raw_value: str | None) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    try:
        parsed = date.fromisoformat(parse_iso_date(value))
    except DateParseError:
        return value
    return parsed.strftime("%d-%m-%Y")


def format_display_date_range(date_from: str | None, date_to: str | None, *, separator: str = "..") -> str:
    start = format_display_date(date_from)
    end = format_display_date(date_to)
    if start and end:
        return f"{start}{separator}{end}"
    return start or end


def infer_content_date(text: str | None, fallback: str | None = None) -> str:
    parsed = find_first_date(text)
    if parsed:
        return parsed
    if fallback:
        return parse_iso_date(fallback)
    return today_iso()


def format_russian_date_range(date_from: str, date_to: str) -> str:
    start = date.fromisoformat(parse_iso_date(date_from))
    end = date.fromisoformat(parse_iso_date(date_to))
    if start > end:
        raise DateParseError("Дата начала смены должна быть не позже даты окончания.")
    if start == end:
        return f"{start.day} {RU_MONTHS_GENITIVE[start.month]} {start.year} года"
    if start.year == end.year and start.month == end.month:
        return f"{start.day}-{end.day} {RU_MONTHS_GENITIVE[start.month]} {start.year} года"
    if start.year == end.year:
        return (
            f"{start.day} {RU_MONTHS_GENITIVE[start.month]} - "
            f"{end.day} {RU_MONTHS_GENITIVE[end.month]} {start.year} года"
        )
    return (
        f"{start.day} {RU_MONTHS_GENITIVE[start.month]} {start.year} - "
        f"{end.day} {RU_MONTHS_GENITIVE[end.month]} {end.year} года"
    )
