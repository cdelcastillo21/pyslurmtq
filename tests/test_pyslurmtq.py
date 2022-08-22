import json
import time
import os
import pdb
import random
import shutil
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from pyslurmtq.pyslurmtq import main, run
from pyslurmtq.SLURMTaskQueue import Slot, Task, SLURMTaskQueue

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

test_dir = Path(__file__).parent / "test_tq"


def _setup_test_dir():
    if test_dir.exists():
        shutil.rmtree(test_dir)
    test_dir.mkdir(exist_ok=True)


def _test_bad_task():
    task_dict = {"cmnd": "echo bad", "cores": -1}
    return task_dict


def _bad_test_task_list(num_tasks=1):
    test_file = test_dir / ".test_task_file.json"
    tasks = [_test_bad_task() for x in range(num_tasks)]
    with open(str(test_file), "w") as fp:
        json.dump(tasks, fp)
    return str(test_file)



def _test_task_list(
    cmnd="echo main",
    cores=1,
    num_tasks=4,
    sleep=1,
    pre=None,
    post=None,
    cdir=None,
):
    if pre is not None:
        pre = f"sleep {sleep}; {pre}"
    if post is not None:
        post = f"sleep {sleep}; {post}"
    cdir = f"{test_dir}" if cdir else None

    test_file = test_dir / ".test_task_file.json"
    tasks = [{"cmnd": cmnd, "cores": cores,
        "pre": pre, "post": post, 'cdir':cdir} for x in range(num_tasks)]
    with open(str(test_file), "w") as fp:
        json.dump(tasks, fp)
    return str(test_file)


def _test_random_task_list(
    cmnd="echo main", max_cores=4, num_tasks=4, max_sleep=1,
):
    test_file = test_dir / ".test_task_file.json"
    tasks = [{"cmnd":cmnd,
              "cores": random.randint(1, max_cores),
              "pre": f"sleep {random.uniform(0.1, max_sleep)}",
        }
        for x in range(num_tasks)
    ]
    with open(str(test_file), "w") as fp:
        json.dump(tasks, fp)
    return str(test_file)


def test_simple(capsys):
    # 1 task, requiring 4 cores, occupying all 4 slots. One round of executing
    _setup_test_dir()
    os.environ["SLURM_JOB_ID"] = "123456"
    os.environ["SLURM_JOB_NODELIST"] = "c304-005"
    os.environ["SLURM_TASKS_PER_NODE"] = "4"

    # Note: ibrun failures supressed by aliasing command
    test_file = _test_task_list(cmnd="echo main", cores=4,
            num_tasks=1, sleep=0.1)
    main(
        [
            f"{test_file}",
            "--workdir",
            f"{test_dir}",
            "--delay",
            "1",
            "--task-max-rt",
            "5",
            "--max-rt",
            "10",
            "--no-cleanup",
            "-vv",
        ]
    )
    shutil.rmtree("tests/test_tq")


def test_simple_pre_post(capsys):
    # 1 task, requiring 4 cores, occupying all 4 slots. One round of executing
    _setup_test_dir()
    os.environ["SLURM_JOB_ID"] = "123456"
    os.environ["SLURM_JOB_NODELIST"] = "c304-005"
    os.environ["SLURM_TASKS_PER_NODE"] = "4"

    # Note: ibrun failures supressed by aliasing command
    test_file = _test_task_list(cmnd="pwd", cores=4,
            pre='alias ibrun="echo"; shopt -s expand_aliases',
            post='echo $(cwd)',
            cdir=True,
            num_tasks=1, sleep=0.1)
    main(
        [
            f"{test_file}",
            "--workdir",
            f"{test_dir}",
            "--delay",
            "1",
            "--task-max-rt",
            "5",
            "--max-rt",
            "10",
            "--no-cleanup",
            "-vv",
        ]
    )
    shutil.rmtree("tests/test_tq")


def test_simple_error(capsys):
    # 1 task, requiring 4 cores, occupying all 4 slots. One round of executing
    _setup_test_dir()
    os.environ["SLURM_JOB_ID"] = "123456"
    os.environ["SLURM_JOB_NODELIST"] = "c304-005"
    os.environ["SLURM_TASKS_PER_NODE"] = "4"
    test_file = _test_task_list(
        cmnd="echo main", cores=4, num_tasks=1, sleep=0.1,
    )
    main(
        [
            f"{test_file}",
            "--workdir",
            f"{test_dir}",
            "--delay",
            "1",
            "--task-max-rt",
            "5",
            "--max-rt",
            "10",
            "--no-cleanup",
            "-vv",
        ]
    )
    shutil.rmtree("tests/test_tq")


def test_cleanup(capsys):
    # 1 task, requiring 4 cores, occupying all 4 slots. One round of executing
    _setup_test_dir()
    os.environ["SLURM_JOB_ID"] = "123456"
    os.environ["SLURM_JOB_NODELIST"] = "c304-005"
    os.environ["SLURM_TASKS_PER_NODE"] = "4"
    test_file = _test_task_list(cmnd="echo main", cores=4, num_tasks=1, sleep=0.1)
    main(
        [
            f"{test_file}",
            "--workdir",
            f"{test_dir}",
            "--delay",
            "1",
            "--task-max-rt",
            "5",
            "--max-rt",
            "10",
            "--cleanup",
            "-vv",
        ]
    )
    assert test_dir.exists() == False


def test_too_large(capsys):
    # 4 tasks, each requiring 1 core, go to 4 slots. One round of executing
    _setup_test_dir()
    os.environ["SLURM_JOB_ID"] = "123456"
    os.environ["SLURM_JOB_NODELIST"] = "c304-005"
    os.environ["SLURM_TASKS_PER_NODE"] = "4"
    test_file = _test_task_list(cmnd="echo main", cores=10, num_tasks=1, sleep=0.1)
    main(
        [
            f"{test_file}",
            "--workdir",
            f"{test_dir}",
            "--delay",
            "1",
            "--max-rt",
            "10",
            "--no-cleanup",
            "-vv",
        ]
    )


def test_multiple(capsys):
    """Multiple jobs, will iterate through queue a couple of time"""
    _setup_test_dir()
    os.environ["SLURM_JOB_ID"] = "123456"
    os.environ["SLURM_JOB_ID"] = "123456"
    os.environ["SLURM_JOB_NODELIST"] = "c303-[005-006],c304-005"
    os.environ["SLURM_TASKS_PER_NODE"] = "2(x2),1"
    test_file = _test_task_list(cmnd="echo main", cores=1, num_tasks=10, sleep=0.1)
    main(
        [
            f"{test_file}",
            "--workdir",
            f"{test_dir}",
            "--delay",
            "1",
            "--max-rt",
            "10",
            "--no-cleanup",
            "-vv",
        ]
    )


def test_random(capsys):
    """Multiple jobs, will iterate through queue a couple of time"""
    _setup_test_dir()
    os.environ["SLURM_JOB_ID"] = "123456"
    os.environ["SLURM_JOB_ID"] = "123456"
    os.environ["SLURM_JOB_NODELIST"] = "c303-[005-006],c304-005"
    os.environ["SLURM_TASKS_PER_NODE"] = "2(x2),1"
    test_file = _test_random_task_list(
        cmnd="echo main", max_cores=4, num_tasks=10, max_sleep=1
    )
    main(
        [
            f"{test_file}",
            "--workdir",
            f"{test_dir}",
            "--delay",
            "0.01",
            "--task-max-rt",
            "20",
            "--max-rt",
            "20",
            "--no-cleanup",
            "-vv",
        ]
    )


def test_task_timeout(capsys):
    """Time-out tasks in queue by setting task max runtime very low"""
    _setup_test_dir()
    os.environ["SLURM_JOB_ID"] = "123456"
    os.environ["SLURM_JOB_NODELIST"] = "c303-005"
    os.environ["SLURM_TASKS_PER_NODE"] = "1"

    # Note: ibrun failures supressed by added semicolon to end of main command
    test_file = _test_task_list(cmnd="echo main",
            cores=1,
            num_tasks=1,
            pre='alias ibrun="echo"; shopt -s expand_aliases',
            sleep=20)
    main(
        [
            f"{test_file}",
            "--workdir",
            f"{test_dir}",
            "--delay",
            "1",
            "--task-max-rt",
            "0.01",
            "--max-rt",
            "10",
            "--no-cleanup",
            "-vv",
        ]
    )
    # TODO: Check that tasks indeed timed out

def test_waiting(capsys):
    """Time-out whole queue by setting max runtime very low"""
    _setup_test_dir()
    os.environ["SLURM_JOB_ID"] = "123456"
    os.environ["SLURM_JOB_NODELIST"] = "c303-005"
    os.environ["SLURM_TASKS_PER_NODE"] = "2(x5)"

    # Note: ibrun failures supressed by added semicolon to end of main command
    test_file = _test_task_list(cmnd="echo main",
            pre='alias ibrun="echo"; shopt -s expand_aliases',
            cores=7, num_tasks=3, sleep=0.5)
    main(
        [
            f"{test_file}",
            "--workdir",
            f"{test_dir}",
            "--delay",
            "0.1",
            "--task-max-rt",
            "1",
            "--max-rt",
            "10",
            "--no-cleanup",
            "-vv",
        ]
    )

    # TODO check timeout


def test_queue_timeout(capsys):
    """Time-out whole queue by setting max runtime very low"""
    _setup_test_dir()
    os.environ["SLURM_JOB_ID"] = "123456"
    os.environ["SLURM_JOB_NODELIST"] = "c303-[005-006],c304-005"
    os.environ["SLURM_TASKS_PER_NODE"] = "2(x2),1"

    # Note: ibrun failures supressed by added semicolon to end of main command
    test_file = _test_task_list(cmnd="echo main",
            pre='alias ibrun="echo"; shopt -s expand_aliases',
            cores=1, num_tasks=1, sleep=10)
    main(
        [
            f"{test_file}",
            "--workdir",
            f"{test_dir}",
            "--delay",
            "1",
            "--task-max-rt",
            "0.1",
            "--max-rt",
            "1",
            "--no-cleanup",
            "-vv",
        ]
    )

    # TODO check timeout


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


def test_default_workdir(capsys):
    """Test using default workdir"""
    _setup_test_dir()
    os.environ["SLURM_JOB_ID"] = "123456"
    os.environ["SLURM_JOB_NODELIST"] = "c303-005"
    os.environ["SLURM_TASKS_PER_NODE"] = "1"

    test_file = _test_task_list(cmnd="echo main", cores=1, num_tasks=1, sleep=1)
    tq = SLURMTaskQueue(test_file)
    tq.run()
    assert str(tq.workdir.name).startswith(".stq-job123456-")
    tq.cleanup()

    # TODO: Check output and delte default workdir created by task queue


def test_bad_task(capsys):
    """Test using default workdir"""
    _setup_test_dir()
    os.environ["SLURM_JOB_ID"] = "123456"
    os.environ["SLURM_JOB_NODELIST"] = "c303-005"
    os.environ["SLURM_TASKS_PER_NODE"] = "1"

    test_file = _bad_test_task_list()
    tq = SLURMTaskQueue(test_file)
    tq.run()
    tq.cleanup()

    # TODO: Check output and delte default workdir created by task queue

def test_err_no_errfile(capsys):
    # 10 hosts with 2 processes, one with 3, total of 23 processes
    _setup_test_dir()
    os.environ["SLURM_JOB_ID"] = "123456"
    os.environ["SLURM_JOB_NODELIST"] = "c303-[005-009,016-020],c304-005"
    os.environ["SLURM_TASKS_PER_NODE"] = "2(x10),3"

    test_file = _test_task_list(cmnd="echo main 2>&1", cores=1, num_tasks=1)
    main(
        [
            f"{test_file}",
            "--workdir",
            f"{test_dir}",
            "--delay",
            "1",
            "--max-rt",
            "10",
            "--no-cleanup",
            "-vv",
        ]
    )

def test_slot():
    """Basic tests for the slot class"""

    slot = Slot('c101', 1)
    task = Task(0, 'echo test', test_dir)
    task2 = Task(1, 'echo test', test_dir)
    assert slot.is_free()
    slot.occupy(task)
    assert not slot.is_free()
    assert slot.tasks[-1] == task
    with pytest.raises(AttributeError):
        slot.occupy(task2)
    slot.release()
    assert slot.is_free()
    slot.occupy(task2)
    assert not slot.is_free()
    assert slot.tasks[-1] == task2
    assert slot.tasks[0] == task


def test_task():
    """Basic tests for the task class"""
    _setup_test_dir()

    # Task that will fail because ibrun not defined
    task = Task(0, 'echo test', test_dir)
    task.execute(0, 1)
    time.sleep(0.5)
    rc = task.get_rc()
    assert rc != 0
    assert 'ibrun: command not found' in task.err_msg

    pre = 'alias ibrun="echo"; shopt -s expand_aliases'
    task2 = Task(1, 'echo test', test_dir, pre=pre)
    task2.execute(1, 2)
    time.sleep(0.5)
    rc = task2.get_rc()
    assert rc == 0

    pre = 'sleep 20'
    task3 = Task(2, 'echo test', test_dir, pre=pre)
    task3.terminate() # should do nothing
    task3.execute(3, 2)
    rc = task3.get_rc()
    assert rc is None
    task3.terminate()
    time.sleep(0.5)
    rc = task3.get_rc()
    assert rc != 0
