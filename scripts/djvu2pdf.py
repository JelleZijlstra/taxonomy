# Needed:
# - brew install djvulibre
# Ghostscript (forgot how I got that)

import argparse
from pathlib import Path
import subprocess
import tempfile


def run(input_file: Path, output_file: Path) -> None:
    with tempfile.NamedTemporaryFile() as f:
        subprocess.check_call(["djvups", input_file, f.name])
        subprocess.check_call(
            [
                "gs",
                "-dNOPAUSE",
                "-dBATCH",
                "-q",
                "-sDEVICE=pdfwrite",
                f"-sOutputFile={output_file}",
                f.name,
            ]
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("inputfile")
    parser.add_argument("outputfile")
    args = parser.parse_args()
    run(Path(args.inputfile), Path(args.outputfile))
