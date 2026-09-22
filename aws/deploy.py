#!/usr/bin/env python
import argparse
import datetime
import shlex
import shutil
import subprocess
import time
from pathlib import Path

from taxonomy.config import Options, get_options

STAGING_DIR = "/home/ec2-user/staging/"
RSYNC_ATTEMPTS = 5
RSYNC_RETRY_DELAY = 10
# Socket errors, broken streams, timeouts, and SSH connection failures.
RSYNC_RETRY_CODES = {10, 12, 30, 35, 255}


def run_ssh(options: Options, command: str) -> None:
    print(f"# {command}")
    subprocess.check_call(
        ["ssh", "-i", options.pem_file, options.hesperomys_host, command]
    )


def run_scp(
    options: Options, local_path: Path, remote_path: str, *, is_directory: bool
) -> None:
    command: list[str | Path] = ["scp"]
    if is_directory:
        command.append("-r")
    command += [
        "-i",
        options.pem_file,
        str(local_path),
        f"{options.hesperomys_host}:{remote_path}",
    ]
    print(f"# {command}")
    subprocess.check_call(command)


def run_rsync(options: Options, local_path: Path, remote_path: str) -> None:
    """Upload into staging resumably; requires rsync locally and on the server.

    The destination must not be a live file: an interrupted upload leaves it partial.
    """
    ssh_command = shlex.join(
        [
            "ssh",
            "-i",
            str(options.pem_file),
            "-o",
            "ConnectTimeout=30",
            "-o",
            "ServerAliveInterval=30",
            "-o",
            "ServerAliveCountMax=3",
        ]
    )
    command = [
        "rsync",
        "--times",
        "--progress",
        # Staging is promoted only after success. Avoid an extra database-sized copy
        # on each attempt; --inplace also retains partial files for resumption.
        "--inplace",
        "--timeout=120",
        "-e",
        ssh_command,
        str(local_path),
        f"{options.hesperomys_host}:{remote_path}",
    ]
    for attempt in range(1, RSYNC_ATTEMPTS + 1):
        print(f"# {shlex.join(command)}")
        try:
            subprocess.check_call(command)
        except subprocess.CalledProcessError as exc:
            if exc.returncode not in RSYNC_RETRY_CODES or attempt == RSYNC_ATTEMPTS:
                raise
            print(
                f"# rsync failed (exit {exc.returncode}, attempt"
                f" {attempt}/{RSYNC_ATTEMPTS}); resuming in {RSYNC_RETRY_DELAY}s"
            )
            time.sleep(RSYNC_RETRY_DELAY)
        else:
            return


def save_data(options: Options, version: str) -> None:
    saved_filename = (
        options.db_filename.parent / f"{options.db_filename.name}.{version}"
    )
    assert not saved_filename.exists(), f"{saved_filename} already exists"
    shutil.copy(options.db_filename, saved_filename)


def deploy_data(options: Options) -> None:
    run_ssh(options, f"mkdir -p {STAGING_DIR}")
    run_rsync(options, options.db_filename, STAGING_DIR)
    run_rsync(options, options.derived_data_filename, STAGING_DIR)
    # Also deploy the full-text search database used by hsweb
    if options.search_db_filename and options.search_db_filename.exists():
        run_rsync(options, options.search_db_filename, STAGING_DIR)


def validate_version(version: str) -> None:
    today = datetime.datetime.now(tz=datetime.UTC).date()
    expected_start = f"{str(today.year)[-2:]}.{today.month}."
    assert version.startswith(expected_start)


def assert_git_clean(repo: Path) -> None:
    output = subprocess.check_output(["git", "status", "--porcelain"], cwd=repo)
    assert not output, f"{repo} has uncommitted changes"


def push_taxonomy(options: Options, version: str) -> None:
    release_notes = (options.taxonomy_repo / "docs/release-notes.md").read_text()
    assert version in release_notes, f"{version} is not documented"
    subprocess.check_call(["git", "tag", version], cwd=options.taxonomy_repo)
    subprocess.check_call(["git", "push"], cwd=options.taxonomy_repo)
    subprocess.check_call(["git", "push", "--tags"], cwd=options.taxonomy_repo)


def build_hesperomys(options: Options) -> None:
    subprocess.check_call(["npm", "run", "relay"], cwd=options.hesperomys_repo)
    subprocess.check_call(["npm", "run", "build"], cwd=options.hesperomys_repo)


def push_hesperomys(options: Options, version: str) -> None:
    build_hesperomys(options)
    subprocess.check_call(["git", "tag", version], cwd=options.hesperomys_repo)
    subprocess.check_call(["git", "push"], cwd=options.hesperomys_repo)
    subprocess.check_call(["git", "push", "--tags"], cwd=options.hesperomys_repo)


def deploy_hesperomys(options: Options) -> None:
    run_ssh(options, "cd hesperomys/ && git pull")
    run_ssh(options, "rm -rf /home/ec2-user/hesperomys/build")
    run_scp(
        options,
        options.hesperomys_repo / "build",
        "/home/ec2-user/hesperomys/build",
        is_directory=True,
    )


def deploy_taxonomy(options: Options) -> None:
    run_ssh(options, "cd taxonomy/ && git pull")
    run_scp(
        options,
        options.taxonomy_repo / "taxonomy-public.ini",
        "/home/ec2-user/taxonomy/taxonomy.ini",
        is_directory=False,
    )


def deploy_game_data(options: Options) -> None:
    """Upload generated hsweb game_data files to the server.

    Expects locally generated files in `hsweb/game_data/` within the taxonomy repo.
    """
    local_dir = options.taxonomy_repo / "hsweb" / "game_data"
    if not local_dir.exists():
        print(f"# {local_dir} not found; skipping game_data upload")
        return
    # Ensure parent directory exists on the server, then copy the whole directory.
    run_ssh(options, "mkdir -p /home/ec2-user/taxonomy/hsweb")
    # Copying the directory into the parent so it lands at hsweb/game_data on the server.
    run_scp(options, local_dir, "/home/ec2-user/taxonomy/hsweb", is_directory=True)


def restart(options: Options, *, kill: bool = True, port: int = 80) -> None:
    if kill:
        run_ssh(options, "sudo pkill -f hsweb")
        time.sleep(1)
    run_scp(
        options,
        options.taxonomy_repo / "aws" / "run_hsweb.sh",
        "/home/ec2-user/run_hsweb.sh",
        is_directory=False,
    )
    run_ssh(
        options,
        f"cd taxonomy && /home/ec2-user/run_hsweb.sh {port} >/home/ec2-user/hesperomys_runner.log 2>&1 &",
    )


def full_deploy(options: Options, version: str) -> None:
    assert_git_clean(options.taxonomy_repo)
    assert_git_clean(options.hesperomys_repo)
    validate_version(version)
    push_taxonomy(options, version)
    push_hesperomys(options, version)

    save_data(options, version)
    deploy_data(options)
    run_ssh(options, "mv /home/ec2-user/staging/* /home/ec2-user/")
    deploy_hesperomys(options)
    deploy_taxonomy(options)
    deploy_game_data(options)
    restart(options)


def update_code(options: Options) -> None:
    build_hesperomys(options)
    deploy_hesperomys(options)
    deploy_taxonomy(options)
    deploy_game_data(options)
    restart(options)


def interactive_ssh(options: Options) -> None:
    subprocess.check_call(["ssh", "-i", options.pem_file, options.hesperomys_host])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["ssh", "deploy", "restart", "code"])
    parser.add_argument("version", nargs="?")
    parser.add_argument("--port", type=int, default=80)
    parser.add_argument("--kill", action="store_true", default=False)
    args = parser.parse_args()

    options = get_options()

    match args.command:
        case "restart":
            restart(options, kill=args.kill, port=args.port)
        case "ssh":
            interactive_ssh(options)
        case "deploy":
            assert args.version
            full_deploy(options, args.version)
        case "code":
            update_code(options)


if __name__ == "__main__":
    main()
