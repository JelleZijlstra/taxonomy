"""Abstraction for updating data in a Google Sheet."""

import csv
import datetime
import itertools
import pprint
import time
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TypeVar

import google.auth.exceptions
import gspread

from taxonomy import getinput
from taxonomy.config import get_options

T = TypeVar("T")


def batched(iterable: Iterable[T], n: int) -> Iterable[list[T]]:
    it = iter(iterable)
    while chunk := list(itertools.islice(it, n)):
        yield chunk


def pprint_nonempty(row: dict[str, str]) -> None:
    pprint.pp({key: value for key, value in row.items() if value})


@dataclass
class FixableDifference:
    row_idx: int
    col_idx: int
    column_name: str
    old_value: str
    new_value: str
    old_row: dict[str, str]
    new_row: dict[str, str]
    kind: Literal["update", "add", "delete"]
    row_name: str

    def key(self) -> tuple[str, str]:
        return (self.column_name, self.kind)

    def print(self) -> None:
        print(f"- {self.column_name} ({self.row_name})")
        if self.old_value:
            print(f"    - Old: {self.old_value}")
        if self.new_value:
            print(f"    - New: {self.new_value}")


def upsheet(
    *,
    sheet_name: str,
    worksheet_gid: int,
    data: Sequence[dict[str, str]],
    matching_column: str,
    backup_path_name: str,
) -> None:
    options = get_options()
    backup_path = (
        options.data_path
        / backup_path_name
        / datetime.datetime.now(tz=datetime.UTC).isoformat()
    )
    backup_path.mkdir(parents=True, exist_ok=True)

    print(f"Downloading {backup_path_name} sheet...")
    try:
        gc = gspread.oauth()
        sheet = gc.open(sheet_name)
    except google.auth.exceptions.RefreshError:
        print("need to refresh token")
        token_path = Path("~/.config/gspread/authorized_user.json").expanduser()
        token_path.unlink(missing_ok=True)
        gc = gspread.oauth()
        sheet = gc.open(sheet_name)

    worksheet = sheet.get_worksheet_by_id(worksheet_gid)
    raw_rows = worksheet.get()
    headings = raw_rows[0]
    column_to_idx = {heading: i for i, heading in enumerate(headings, start=1)}
    rows = [dict(zip(headings, row, strict=False)) for row in raw_rows[1:]]
    print(f"done, {len(rows)} found")

    print(f"backing up {backup_path_name}... ")
    with (backup_path / "data.csv").open("w") as file:
        writer = csv.writer(file)
        for row in raw_rows:
            writer.writerow(row)
    print(f"done, backup at {backup_path}")

    existing_data = {
        row[matching_column]: (row_idx, row)
        for row_idx, row in enumerate(rows, start=2)
    }
    if len(existing_data) != len(rows):
        counts = Counter(row[matching_column] for row in rows)
        duplicates = [item for item, count in counts.items() if count > 1]
        assert duplicates
        raise ValueError(
            f"Duplicate entries found in existing sheet for column '{matching_column}': {duplicates}"
        )
    new_data = {row[matching_column]: row for row in data}
    if len(new_data) != len(data):
        counts = Counter(row[matching_column] for row in data)
        duplicates = [item for item, count in counts.items() if count > 1]
        assert duplicates
        raise ValueError(
            f"Duplicate entries found in new data for column '{matching_column}': {duplicates}"
        )

    matched_rows = {
        key: (existing_data[key][0], existing_data[key][1], new_data[key])
        for key in existing_data.keys() & new_data.keys()
    }
    rows_to_add = {key: new_data[key] for key in new_data.keys() - existing_data.keys()}
    rows_to_delete = {
        key: existing_data[key] for key in existing_data.keys() - new_data.keys()
    }
    assert len(matched_rows) + len(rows_to_add) == len(data)
    assert len(matched_rows) + len(rows_to_delete) == len(rows)

    # 1. Updating rows
    differences: list[FixableDifference] = []
    for row_idx, old_row, new_row in matched_rows.values():
        for column, new_value in new_row.items():
            old_value = old_row.get(column, "")
            if old_value != new_value:
                differences.append(
                    FixableDifference(
                        row_idx=row_idx,
                        col_idx=column_to_idx[column],
                        column_name=column,
                        old_value=old_value,
                        new_value=new_value,
                        old_row=old_row,
                        new_row=new_row,
                        kind=(
                            "add"
                            if not old_value
                            else ("delete" if not new_value else "update")
                        ),
                        row_name=new_row[matching_column],
                    )
                )
    differences.sort(key=lambda diff: diff.key())

    getinput.print_header("Summary of changes")
    for (column_name, kind), group_iter in itertools.groupby(
        differences, key=lambda diff: diff.key()
    ):
        print(f"- {kind} '{column_name}': {len(list(group_iter))}")
    print(f"- add rows: {len(rows_to_add)}")
    print(f"- delete rows: {len(rows_to_delete)}")

    for (column_name, kind), group_iter in itertools.groupby(
        differences, key=lambda diff: diff.key()
    ):
        group = list(group_iter)
        header = f"{kind.capitalize()} '{column_name}' ({len(group)})"
        getinput.print_header(header)
        for diff in group:
            diff.print()
        print(header)
        can_edit_db = False  # TODO
        choices = ["sheet_edit", "ask_individually", "skip"]
        if can_edit_db:
            choices.append("db_edit")
        choice = getinput.choose_one_by_name(
            choices, allow_empty=False, history_key="overall_choice"
        )
        updates_to_make = []
        for diff in group:
            should_edit_sheet = False
            should_edit_db = False
            match choice:
                case "sheet_edit":
                    should_edit_sheet = True
                case "db_edit":
                    should_edit_db = True
                case "ask_individually":
                    pprint.pp(diff.new_row)
                    diff.print()
                    choices = ["sheet_edit", "skip"]
                    if can_edit_db:
                        choices.append("db_edit")
                    individual_choice = getinput.choose_one_by_name(
                        choices, allow_empty=False, history_key="individual_choice"
                    )
                    match individual_choice:
                        case "sheet_edit":
                            should_edit_sheet = True
                        case "db_edit":
                            should_edit_db = True
            if should_edit_sheet:
                updates_to_make.append(
                    gspread.cell.Cell(
                        row=diff.row_idx, col=diff.col_idx, value=diff.new_value
                    )
                )
            elif should_edit_db:
                raise NotImplementedError

        done = 0
        print("Applying changes for column", column_name)
        for batch in batched(updates_to_make, 500):
            worksheet.update_cells(batch)
            done += len(batch)
            print(f"Done {done}/{len(updates_to_make)}")
            if len(batch) == 500:
                time.sleep(5)

    # 2. Adding rows
    if rows_to_add:
        getinput.print_header(f"Add rows ({len(rows_to_add)})")
        for row in rows_to_add.values():
            pprint_nonempty(row)
        choice = getinput.choose_one_by_name(
            ["sheet_edit", "skip"], allow_empty=False, history_key="add_rows_choice"
        )
        if choice == "sheet_edit":
            new_rows = [
                [new_row.get(heading, "") for heading in headings]
                for new_row in rows_to_add.values()
            ]
            done = 0
            print("Adding rows...")
            for batch in batched(new_rows, 500):
                worksheet.append_rows(batch)
                done += len(batch)
                print(f"Done {done}/{len(new_rows)}")
                if len(batch) == 500:
                    time.sleep(5)

    # 3. Deleting rows
    if rows_to_delete:
        getinput.print_header(f"Delete rows ({len(rows_to_delete)})")
        for _, old_row in rows_to_delete.values():
            pprint_nonempty(old_row)
        choice = getinput.choose_one_by_name(
            ["sheet_edit", "skip"], allow_empty=False, history_key="delete_rows_choice"
        )
        if choice == "sheet_edit":
            row_indices = sorted(
                (row_idx for row_idx, _ in rows_to_delete.values()), reverse=True
            )
            print("Deleting rows...")
            for row_idx in row_indices:
                worksheet.delete_rows(row_idx)
                print(f"Deleted row {row_idx}")
                time.sleep(1)

    print(f"Done updating {backup_path_name}.")
