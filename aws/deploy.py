#!/usr/bin/env python
import argparse
import datetime
import shutil
import subprocess
import time
from pathlib import Path

from taxonomy.config import Options, get_options

STAGING_DIR = "/home/ec2-user/staging/"
PYTHON = "/usr/local/bin/python3.11"


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


def save_data(options: Options, version: str) -> None:
    saved_filename = (
        options.db_filename.parent / f"{options.db_filename.name}.{version}"
    )
    assert not saved_filename.exists(), f"{saved_filename} already exists"
    shutil.copy(options.db_filename, saved_filename)


def deploy_data(options: Options) -> None:
    run_ssh(options, f"mkdir -p {STAGING_DIR}")
    run_scp(options, options.db_filename, STAGING_DIR, is_directory=False)
    run_scp(options, options.derived_data_filename, STAGING_DIR, is_directory=False)
    run_scp(options, options.cached_data_filename, STAGING_DIR, is_directory=False)


def validate_version(version: str) -> None:
    today = datetime.date.today()
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


def push_hesperomys(options: Options, version: str) -> None:
    subprocess.check_call(["npm", "run", "relay"], cwd=options.hesperomys_repo)
    subprocess.check_call(["npm", "run", "build"], cwd=options.hesperomys_repo)
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
    run_ssh(
        options,
        (
            f"cd taxonomy/ && sudo {PYTHON} -m pip install -U pip setuptools wheel &&"
            f" sudo {PYTHON} -m pip install -e ."
        ),
    )
    run_scp(
        options,
        options.taxonomy_repo / "taxonomy-public.ini",
        "/home/ec2-user/taxonomy/taxonomy.ini",
        is_directory=False,
    )


def restart(options: Options, kill: bool = True, port: int = 80) -> None:
    if kill:
        run_ssh(options, "sudo pkill -f hsweb")
        time.sleep(1)
    run_ssh(
        options,
        (
            f"sudo nohup {PYTHON} -m hsweb -p {port} -b ~/hesperomys"
            " >/home/ec2-user/hesperomys.log 2>&1 &"
        ),
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
    restart(options)


def interactive_ssh(options: Options) -> None:
    subprocess.check_call(["ssh", "-i", options.pem_file, options.hesperomys_host])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["ssh", "deploy", "restart"])
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


if __name__ == "__main__":
    main()
