import re
import sys
from pathlib import Path

SUFFIX = ".new"
SAFE_REMOVE = {
    "IUCN Red List",
    "TFTSG Provisional Red List",
    "Size (Max SCL)",
    "Distribution",
    "CBFTT Account",
    "Presumed Historic Indigenous Range",
    "Nesting",
    "Foraging",
    "Vagrant",
    "CITES",
    "Possible Holocene Range",
    "Previously",
    "Introduced",
}
COLON_REMOVE = SAFE_REMOVE | {"just to keep it non-empty"}


def consolidate_lines(lines: list[str]) -> list[str]:
    new_lines: list[str] = []
    for line in lines:
        if (
            line.strip()
            and (not line.startswith(("\t", "")))
            and new_lines
            and new_lines[-1]
            and new_lines[-1].startswith(("\t", "("))
        ):
            new_lines[-1] = new_lines[-1] + " " + line
        else:
            new_lines.append(line)
    return new_lines


CLEANUPS = [
    (r"cm( \[estimated\])? \([^)]+\)", "cm"),
    (r"\([A-Za-z\. ]+ \d{4}\); ", "; "),
]


def remove_lines(lines: list[str]) -> list[str]:
    new_lines = []
    warnings = []
    for line in lines:
        label = line.strip().split(":")[0]
        should_remove = label in COLON_REMOVE
        safe = label in SAFE_REMOVE
        if " / " in line and "Tahanaoute" not in line:
            should_remove = True
            safe = True
        if line.startswith(
            "Turtles of the World: Annotated Checklist and Atlas (9th Ed.) – 2021"
        ):
            should_remove = True
            safe = True
        if line.startswith(
            "Conservation Biology of Freshwater Turtles and Tortoises • Chelonian"
            " Research Monographs, No. 8"
        ):
            should_remove = True
            safe = True
        if line.startswith("(") and line.endswith(")"):
            should_remove = True
            safe = True
        if line.strip() in ("*", "Synonymy:"):
            should_remove = True
            safe = True
        if re.match(r"^\d+$", line):
            should_remove = True
            safe = True

        if should_remove:
            if not safe:
                search_line = line
                for pattern, replacement in CLEANUPS:
                    search_line = re.sub(pattern, replacement, search_line)
                if re.search(r"[a-z] \d{4}", search_line):
                    print("WARNING", line)
                    warnings.append(line)
                print(line)
            continue
        new_lines.append(line)

    if warnings:
        print()
        print()
        print("WARNINGS")
        for line in warnings:
            print(line)
    return new_lines


def clean_up_lines(lines: list[str]) -> list[str]:
    lines = [line.rstrip() for line in lines]
    new_lines: list[str] = []
    for line in lines:
        if new_lines and (not line) and (not new_lines[-1]):
            continue
        new_lines.append(line)
    return new_lines


def run(file: Path, dry_run: bool) -> None:
    lines = file.read_text().splitlines()
    lines = consolidate_lines(lines)
    lines = remove_lines(lines)
    lines = clean_up_lines(lines)
    new_file = file.parent / (file.name + SUFFIX)
    if not dry_run:
        new_file.write_text("".join(line + "\n" for line in lines))


if __name__ == "__main__":
    run(Path(sys.argv[1]), len(sys.argv) > 2)
