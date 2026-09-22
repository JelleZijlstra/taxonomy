import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from aws.deploy import RSYNC_ATTEMPTS, RSYNC_RETRY_DELAY, STAGING_DIR, run_rsync
from taxonomy.config import Options


@pytest.mark.parametrize("returncode", [10, 12, 30, 35, 255])
def test_rsync_resumes_after_connection_failure(returncode: int) -> None:
    failure = subprocess.CalledProcessError(returncode, "rsync")
    with (
        patch("aws.deploy.subprocess.check_call", side_effect=[failure, 0]) as transfer,
        patch("aws.deploy.time.sleep") as sleep,
    ):
        run_rsync(Options(), Path("search.db"), STAGING_DIR)

    assert transfer.call_count == 2
    assert transfer.call_args_list[0] == transfer.call_args_list[1]
    sleep.assert_called_once_with(RSYNC_RETRY_DELAY)


def test_rsync_aborts_after_repeated_connection_failures() -> None:
    failure = subprocess.CalledProcessError(255, "rsync")
    with (
        patch("aws.deploy.subprocess.check_call", side_effect=failure) as transfer,
        patch("aws.deploy.time.sleep") as sleep,
        pytest.raises(subprocess.CalledProcessError) as exc,
    ):
        run_rsync(Options(), Path("search.db"), STAGING_DIR)

    assert exc.value is failure
    assert transfer.call_count == RSYNC_ATTEMPTS
    assert sleep.call_count == RSYNC_ATTEMPTS - 1


@pytest.mark.parametrize("returncode", [1, 11, 20, 23, 24])
def test_rsync_does_not_retry_file_errors_or_cancellation(returncode: int) -> None:
    failure = subprocess.CalledProcessError(returncode, "rsync")
    with (
        patch("aws.deploy.subprocess.check_call", side_effect=failure) as transfer,
        patch("aws.deploy.time.sleep") as sleep,
        pytest.raises(subprocess.CalledProcessError) as exc,
    ):
        run_rsync(Options(), Path("search.db"), STAGING_DIR)

    assert exc.value is failure
    transfer.assert_called_once()
    sleep.assert_not_called()
