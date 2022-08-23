"""
Test Suite

Mocks SLURM executing environment by setting os env variables before
initializing test task queues. `ibrun` is aliased to `echo` within each task so
that main parallel command does not fail (when we don't want it to).

TODO:
    - Figure out and make sure the following directories are not left after tests:
        .stq-job123456-8uhc7ekv/
        tests/test_tq/
    - Check functional output/logs of excuting queues. Coverage there as of now
    but need to check logic.
"""
import json
import time
import os
import pdb
import random
import shutil
import sys
from pathlib import Path
from unittest.mock import patch
from conftest import test_dir

import pytest
from pyslurmtq.pyslurmtq import main, run

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

test_dir = Path(__file__).parent / "test_tq"

def test_run(capsys):
    """Test run entrypoint. Path command line arguments"""
    _setup_test_dir()
    os.environ["SLURM_JOB_ID"] = "123456"
    os.environ["SLURM_JOB_NODELIST"] = "c303-005"
    os.environ["SLURM_TASKS_PER_NODE"] = "1"

    test_file = _test_task_list(cmnd="echo main", cores=1, num_tasks=1, sleep=1)
    testargs = [
        "pyslurmtq",
        f"{test_file}",
        "--delay",
        "1",
        "--task-max-rt",
        "5",
        "--max-rt",
        "10",
        "--no-cleanup",
        "-vv",
    ]
    with patch.object(sys, "argv", testargs):
        run()

    # TODO: Check output and delte default workdir created by task queue
