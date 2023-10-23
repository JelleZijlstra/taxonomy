"""

Script to concatenate a series of PDFs into one.

Requires GhostScript to be installed.

"""

import argparse
import shlex
import subprocess
from pathlib import Path

from taxonomy.config import get_options


def main() -> None:
    parser = argparse.ArgumentParser("concatenate.py")
    parser.add_argument("directory", help="Directory containing PDFs to concatenate")
    parser.add_argument("-o", "--output", help="Output filename", default="out.pdf")
    parser.add_argument(
        "-F",
        "--remove-first",
        help="Remove the first page from each input PDF",
        action="store_true",
        default=False,
    )
    args = parser.parse_args()
    options = get_options()

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Path("out.pdf")
    output_path = options.new_path / output_path

    inputs = sorted(Path(args.directory).glob("*.pdf"))

    print("Concatenating the following files:")
    for file in inputs:
        print(file)

    command = [
        "gs",
        "-dBATCH",
        "-dNOPAUSE",
        "-q",
        "-sDEVICE=pdfwrite",
        f"-sOUTPUTFILE={output_path}",
    ]
    if args.remove_first:
        command.append("-dFirstPage=2")
    command += [str(p) for p in inputs]
    print("Running command:")
    print(" ".join(map(shlex.quote, command)))
    subprocess.check_call(command)


if __name__ == "__main__":
    main()
