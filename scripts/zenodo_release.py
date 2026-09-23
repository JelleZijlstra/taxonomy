"""Prepare, upload and publish Hesperomys releases on Zenodo.

Run with python -m scripts.zenodo_release. See RELEASE.md.
"""

import argparse
import datetime
import os
from pathlib import Path

import httpx

from taxonomy import zenodo_release
from taxonomy.apis.zenodo import PRODUCTION_URL, SANDBOX_URL, ZenodoClient, ZenodoError

REPO_ROOT = Path(__file__).resolve().parents[1]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sandbox",
        action="store_true",
        help="Use Zenodo's sandbox and ZENODO_SANDBOX_TOKEN; leave production docs unchanged",
    )
    parser.add_argument(
        "--attempts",
        type=int,
        default=5,
        help="Maximum attempts for retryable requests/uploads (default: 5)",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser(
        "prepare",
        help="Create/reuse a version draft, reserve its DOI and update release documentation",
    )
    prepare.add_argument("version")
    prepare.add_argument(
        "--previous",
        type=int,
        help="Previous published version's record ID; defaults to the citation in docs/home.md",
    )
    prepare.add_argument(
        "--date",
        type=datetime.date.fromisoformat,
        help="Publication date (default: today; retained on reruns)",
    )
    export = commands.add_parser(
        "export",
        help="Generate the five standard CSV exports into a versioned local bundle",
    )
    export.add_argument("version")
    export.add_argument(
        "--files-dir",
        type=Path,
        help="Export directory (default: outputs/releases/VERSION)",
    )
    for name in ("upload", "publish"):
        subparser = commands.add_parser(
            name, help=f"{name.capitalize()} the complete set of release files"
        )
        subparser.add_argument("version")
        subparser.add_argument(
            "files",
            type=Path,
            nargs="*",
            help="Explicit files; defaults to the generated export bundle",
        )
        subparser.add_argument(
            "--files-dir",
            type=Path,
            help="Generated export bundle to use (default: outputs/releases/VERSION)",
        )
        if name == "upload":
            subparser.add_argument(
                "--prune",
                action="store_true",
                help="Remove other files from the draft after all selected uploads verify",
            )
    status = commands.add_parser(
        "status", help="Show the saved draft's current metadata and files"
    )
    status.add_argument("version")
    args = parser.parse_args(argv)
    if args.attempts < 1:
        parser.error("--attempts must be positive")
    if args.command == "export":
        try:
            paths = zenodo_release.export_release(
                REPO_ROOT, args.version, directory=args.files_dir
            )
        except (ZenodoError, OSError, ValueError) as exc:
            parser.exit(1, f"Error: {exc}\n")
        print(f"Export bundle ready: {paths[0].parent}")
        return
    if args.command in {"upload", "publish"} and args.files and args.files_dir:
        parser.error("Supply explicit files or --files-dir, not both")
    token_variable = "ZENODO_SANDBOX_TOKEN" if args.sandbox else "ZENODO_TOKEN"
    token = os.environ.get(token_variable)
    if not token:
        parser.error(f"Set {token_variable} to a Zenodo personal access token")
    base_url = SANDBOX_URL if args.sandbox else PRODUCTION_URL
    try:
        with httpx.Client(
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            timeout=httpx.Timeout(120, connect=30),
        ) as http:
            client = ZenodoClient(http, base_url=base_url, attempts=args.attempts)
            if args.command == "prepare":
                release = zenodo_release.prepare(
                    client,
                    REPO_ROOT,
                    args.version,
                    previous_id=args.previous,
                    publication_date=args.date,
                )
                print(
                    f"Release record: {zenodo_release.release_path(REPO_ROOT, args.version, base_url)}"
                )
                if not args.sandbox:
                    print(
                        "Review and commit the release record, docs/home.md and docs/release-notes.md before tagging/deploying."
                    )
            else:
                release = zenodo_release.load_release(REPO_ROOT, args.version, base_url)
                files: list[Path] = []
                if args.command in {"upload", "publish"}:
                    files = args.files or zenodo_release.bundle_paths(
                        args.files_dir
                        or zenodo_release.export_directory(REPO_ROOT, args.version),
                        args.version,
                    )
                match args.command:
                    case "upload":
                        zenodo_release.upload(client, release, files, prune=args.prune)
                        print(
                            "All release files verified. The draft is ready for review and publication."
                        )
                    case "publish":
                        zenodo_release.publish(client, release, files)
                        print("Publication confirmed.")
                    case "status":
                        deposition = client.deposition(release.deposition_id)
                        release.validate(deposition)
                        print(
                            f"State: {deposition['state']}; submitted: {deposition['submitted']}"
                        )
                        for file in deposition["files"]:
                            print(
                                f"  {file['filename']}: {file['filesize']} bytes; {file['checksum']}"
                            )
            print(f"DOI: https://doi.org/{release.doi}")
            print(f"Zenodo: {release.draft_url}")
    except (ZenodoError, OSError, ValueError) as exc:
        parser.exit(1, f"Error: {exc}\n")


if __name__ == "__main__":
    main()
