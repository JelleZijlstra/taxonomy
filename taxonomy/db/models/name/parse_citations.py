import re
from dataclasses import dataclass
from pathlib import Path

from taxonomy.db import helpers as _helpers


@dataclass(frozen=True)
class ParsedCitation:
    series: str | None
    volume: str | None
    issue: str | None
    start_page: str | None
    end_page: str | None


_DASH = r"[-\u2012-\u2015\u2212]"  # hyphen and common dash ranges


def _norm(s: str) -> str:
    # normalize various dashes to hyphen and collapse whitespace
    s = re.sub(r"[\u2012-\u2015\u2212]", "-", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _to_roman_or_int(token: str) -> str:
    token = token.strip()
    if not token:
        return token
    # Already a number
    if token.isdigit():
        return token
    # Try Roman numerals
    if re.fullmatch(r"[IVXLCDM]+", token, flags=re.IGNORECASE):
        try:
            if _helpers is not None:
                return str(_helpers.parse_roman_numeral(token))
        except Exception:
            pass
    return token


def _extract_pages(text: str) -> tuple[str | None, str | None]:
    # pattern 1: volume: start-end or : start only
    m = re.search(r":\s*(\d{1,4})(?:\s*" + _DASH + r"\s*(\d{1,4}))?\b", text)
    if m:
        return m.group(1), m.group(2)
    # pattern 2: pp. 12-34; p. 12; S. 123; Seiten 11–12
    m = re.search(
        r"\b(?:pp?\.|S\.|Seiten?|pages?)\s*(\d{1,4})(?:\s*"
        + _DASH
        + r"\s*(\d{1,4}))?\b",
        text,
        flags=re.IGNORECASE,
    )
    if m:
        return m.group(1), m.group(2)
    # pattern 3: trailing , 12-34
    m = re.search(r"[,;] *([0-9]{1,4})\s*" + _DASH + r"\s*([0-9]{1,4})\b", text)
    if m:
        return m.group(1), m.group(2)
    # pattern 4: explicit p. N following volume patterns
    m = re.search(r"\bp\.?\s*(\d{1,4})\b", text, flags=re.IGNORECASE)
    if m:
        return m.group(1), None
    return None, None


def parse_citation(citation: str) -> ParsedCitation:
    text = _norm(citation)
    series: str | None = None
    volume: str | None = None
    issue: str | None = None
    start_page: str | None = None
    end_page: str | None = None

    # 1) Pages first; helps anchor nearby volume patterns
    start_page, end_page = _extract_pages(text)

    # 2) Common "(series)volume(issue)" pattern: e.g., "Ann. Mag. Nat. Hist. (4)1(2):113"
    m = re.search(r"\((\d{1,2})\)\s*(\d{1,4})\s*\((\d{1,3})\)", text)
    if m:
        series = m.group(1)
        volume = m.group(2)
        issue = m.group(3)
    else:
        # 3) Series specified explicitly: "Ser. 3, 1:472" or "Series 2, 3: 1-10"
        m = re.search(
            r"\bSer(?:ies)?\.?\s*(\d{1,2})[,; ]+([0-9IVXLCDM]{1,8})\b",
            text,
            flags=re.IGNORECASE,
        )
        if m:
            series = m.group(1)
            volume = _to_roman_or_int(m.group(2))

    # 4) volume(issue) pattern, e.g., "9(2): 186-265"
    if volume is None:
        m = re.search(r"\b(\d{1,4})\s*\((\d{1,3})\)\s*:\s*\d", text)
        if m:
            volume = m.group(1)
            issue = m.group(2)

    # 5) Generic volume: "... 9: 186–265" (avoid picking up the year if present)
    if volume is None:
        m = re.search(r"\b(\d{1,4})\s*:\s*\d", text)
        if m:
            candidate = m.group(1)
            # Disallow clearly non-volume small numbers attached to months, also allow a 4-digit year
            if len(candidate) <= 3 or 1700 <= int(candidate) <= 2025:
                volume = candidate

    # 6) Year-as-volume case like "... 1858: 339–352"
    if volume is None:
        m = re.search(r"\b(1[6-9]\d{2}|20\d{2})\s*:\s*\d", text)
        if m:
            volume = m.group(1)

    # 7) Roman numerals often used as volume when followed by pp/p. (e.g., ", XXV, pp. 518-521")
    if volume is None:
        m = re.search(r"[,; ]+([IVXLCDM]{1,8})\b[^\d]*(?:pp?\.|S\.|Seiten?)\b", text)
        if m:
            volume = _to_roman_or_int(m.group(1))

    # 8) Volume via labels: vol., Bd., t., v. etc. (allow punctuation after label)
    if volume is None:
        m = re.search(
            r"\b(?:vol|bd|t|tom|tome|v)\.?\s*[,;:]?\s*([0-9IVXLCDM]{1,8})\b",
            text,
            flags=re.IGNORECASE,
        )
        if m:
            volume = _to_roman_or_int(m.group(1))

    # 9) Volume before comma followed by pages: "... 43, 417–435"
    if volume is None:
        m = re.search(r"\b(\d{1,4})\s*,\s*\d{1,4}\b", text)
        if m:
            cand = m.group(1)
            # Avoid taking a year as volume in this comma form
            if len(cand) <= 3:
                volume = cand

    # 10) Volume before colon with section/part tokens: "90: sect A, pt. 1, p. 127"
    if volume is None:
        m = re.search(r"\b(\d{1,4})\s*:\s*.*?\bp\.?\s*\d", text, flags=re.IGNORECASE)
        if m:
            cand = m.group(1)
            if len(cand) <= 3:  # again avoid most years
                volume = cand

    # 11) Issue sometimes appears as standalone parentheses after a comma: ", (2) ,"
    if issue is None:
        m = re.search(r",\s*\((\d{1,3})\)\s*[,;]", text)
        if m:
            issue = m.group(1)

    return ParsedCitation(
        series=series,
        volume=volume,
        issue=issue,
        start_page=start_page,
        end_page=end_page,
    )


def run_test() -> None:
    training_data = Path(__file__).parent / "training.txt"
    lines = training_data.read_text().splitlines()
    series_count = 0
    volume_count = 0
    issue_count = 0
    start_page_count = 0
    end_page_count = 0
    missing_volume: list[str] = []
    for line in lines:
        parsed = parse_citation(line)
        if parsed.series is not None:
            series_count += 1
        if parsed.volume is not None:
            volume_count += 1
        else:
            missing_volume.append(line)
        if parsed.issue is not None:
            issue_count += 1
        if parsed.start_page is not None:
            start_page_count += 1
        if parsed.end_page is not None:
            end_page_count += 1
    print(f"Out of {len(lines)} citations:")
    print(f"  Series found: {series_count}")
    print(f"  Volume found: {volume_count}")
    print(f"  Issue found: {issue_count}")
    print(f"  Start page found: {start_page_count}")
    print(f"  End page found: {end_page_count}")
    if missing_volume:
        print()
        print(f"Citations missing volume ({len(missing_volume)}):")
        for cit in missing_volume:
            print(f"  - {cit}")


if __name__ == "__main__":
    run_test()
