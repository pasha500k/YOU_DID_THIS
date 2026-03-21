"""
????: text.py
???????? ??????? ??? ?????????, ???????, ???????? ? ??????????
?????? ? ??????????, ??????? ???? ? ???????? ?? ??????????.
"""

from __future__ import annotations


def compact_whitespace(text: str) -> str:
    return " ".join(text.split())


def trim_text(text: str, max_chars: int) -> str:
    normalized = text.strip()
    if len(normalized) <= max_chars:
        return normalized
    head = max_chars // 2
    tail = max_chars - head - len("\n\n[...]\n\n")
    return normalized[:head].rstrip() + "\n\n[...]\n\n" + normalized[-tail:].lstrip()


def split_into_chunks(text: str, max_chars: int, overlap_chars: int) -> list[str]:
    normalized = text.strip()
    if not normalized:
        return []

    if len(normalized) <= max_chars:
        return [normalized]

    chunks: list[str] = []
    start = 0
    while start < len(normalized):
        end = min(start + max_chars, len(normalized))
        if end < len(normalized):
            newline_break = normalized.rfind("\n", start, end)
            sentence_break = max(
                normalized.rfind(". ", start, end),
                normalized.rfind("! ", start, end),
                normalized.rfind("? ", start, end),
            )
            best_break = max(newline_break, sentence_break)
            if best_break > start + max_chars // 2:
                end = best_break + 1

        chunk = normalized[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= len(normalized):
            break
        start = max(end - overlap_chars, start + 1)
    return chunks


def split_for_telegram(text: str, limit: int = 3900) -> list[str]:
    normalized = text.strip()
    if not normalized:
        return []
    if len(normalized) <= limit:
        return [normalized]

    parts: list[str] = []
    current = ""
    for line in normalized.splitlines(keepends=True):
        if len(line) > limit:
            if current:
                parts.append(current.rstrip())
                current = ""
            start = 0
            while start < len(line):
                parts.append(line[start : start + limit].rstrip())
                start += limit
            continue

        if len(current) + len(line) > limit and current:
            parts.append(current.rstrip())
            current = line
            continue
        current += line

    if current:
        parts.append(current.rstrip())
    return [part for part in parts if part]
