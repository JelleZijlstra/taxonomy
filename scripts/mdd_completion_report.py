"""

Script to print completion rates for MDD.

"""

import gspread

from taxonomy.config import get_options


def run() -> None:
    options = get_options()

    print("downloading MDD names... ")
    gc = gspread.oauth()
    sheet = gc.open(options.mdd_sheet)
    worksheet = sheet.get_worksheet_by_id(options.mdd_worksheet_gid)
    rows = worksheet.get()
    print(f"done, {len(rows)} found")
    headings = rows[0]
    column_to_idx = {heading: i for i, heading in enumerate(headings)}
    content_rows = rows[1:]
    column_to_count = {
        column: sum(
            bool(column_to_idx[column] < len(row) and row[column_to_idx[column]])
            for row in content_rows
        )
        for column in headings
    }
    for column, count in sorted(column_to_count.items(), key=lambda pair: pair[1]):
        print(f"{column}: {count}/{len(content_rows)} ({count/len(content_rows):.1%})")


def main() -> None:
    run()


if __name__ == "__main__":
    main()
