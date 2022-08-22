"""

SLURMTaskQueue

Defines classes for executing a collection of tasks in a single SLURM job. A
task is defined as a command to be run in parallel using a given number of
SLURM tasks, as would be run with `ibrun -n`.
"""

import argparse as ap
import copy
from prettytable import PrettyTable
import json
import logging
import os
import pdb
import re
import stat
import subprocess
import sys
import tempfile
import time
import json
from pathlib import Path
import shutil
from pythonjsonlogger import jsonlogger
import traceback

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

def _expand_int_list(s):
    """Expands int lists with ranges."""
    r = []
    for i in s.split(","):
        if "-" not in i:
            r.append(int(i))
        else:
            l, h = map(int, i.split("-"))
            r += range(l, h + 1)
    return r


def _compact_int_list(i, delim=","):
    """Compacts int lists with ranges."""
    if len(i) == 0:
        return ""
    elif len(i) == 1:
        return f"{i[0]}"
    for e in range(1, len(i)):
        if i[e] != i[0] + e:
            return f"{i[0]}-{i[e-1]}{delim}{_compact_int_list(i[e:])}"
    return f"{i[0]}-{i[-1]}"

def _print_res(res, fields, search=None, match=r'.'):
    """
    Print results

    Prints dictionary keys in list `fields` for each dictionary in res,
    filtering on the search column if specified with regular expression
    if desired.

    Parameters
    ----------
    res : List[dict]
        List of dictionaries containing response of an AgavePy call
    fields : List[string]
        List of strings containing names of fields to extract for each element.
    search : string, optional
        String containing column to perform string patter matching on to
        filter results.
    match : str, default='.'
        Regular expression to match strings in search column.

    """
    # Initialize Table
    x = PrettyTable()
    x.field_names = fields

    # Build table from results
    for r in res:
        if search is not None:
            if re.search(match, r[search]) is not None:
                x.add_row([r[f] for f in fields])
        else:
            x.add_row([r[f] for f in fields])

    # Print Table
    print(x)

class Slot:
    """
    Combination of (host, idx) that can run a SLURM task. A slot can have
    a task associated with it or be free. `host` corresponds to the name of the
    host that can execute the task, as accessible in the SLURM environment
    variable SLURM_JOB_NODELIST or, locally from the host, in SLURM_NODENAME.
    `idx` corresponds to the index in the total task list available to the
    SLURM job that the slot corresponds to. For example if a job is being run
    with 3 nodes and 5 total tasks (`-N 3 -n 5`), then the SLURM execution
    environment will look something like:

    .. code-block:: bash
        SLURM_JOB_NODELIST=c303-[005-006],c304-005
        SLURM_TASKS_PER_NODE=2(x2),1

    In this scenario, host `cs303-005` would have slot idxs 1 and
    2, `cs303-006` would have slots 3 and 4 associated with it, and host
    `cs304-005` would have only slot 5 associated with it. Note that these task
    slots do not corespond to the available CPUs per host available, which can
    vary depending on the cluster being used.

    Attributes
    ----------
    host : str
        Name of compute node on a SLURM execution system. Corresponds to a host 
        listed in the environment variable SLURM_JOB_NODELIST.
    idx : int
        Index of task in total available SLURM task slots. See above for more
        details.
    free : bool
        False if slot is being occupied currently by a task, True otherwise.
    tasks : List[Task]
        List of Task objects that have been executed on this slot. If the
        slot is currently occupied, the last element in the list corresponds to
        the currently running task.
    """

    def __init__(self, host, idx):
        self.host = host
        self.idx = idx
        self.free = True
        self.tasks = []

    def occupy(self, task):
        """
        Occupy a slot with a task.

        Parameters
        ----------
        task: :class:Task
            Task object that will occupy the slot.

        Raises
        -------
        ValueError
            If trying to occupy a slot that is not currently free.

        """
        if not self.free:
            raise AttributeError(f"Trying to occupy a busy node {self}")
        self.tasks.append(task)
        self.free = False

    def release(self):
        """Make slot unoccupied."""
        self.free = True

    def is_free(self):
        """Test whether slot is occupied"""
        return self.free

    def __str__(self):
        s = "FREE - " if self.free else "BUSY - "
        s += f"({self.host}, {self.idx}), tasks:{self.tasks}"
        return s

    def __repr__(self):
        s = f"Slot({self.host}, {self.idx})"
        return s


class Task:
    """
    Command to be executed in parallel using ibrun on a slot of SLURM tasks as
    designated by :class:SLURMTaskQueue. This class contains the particulars of
    a task to be executing, including the main parallel command to be executed
    in parallel using ibrun, optional pre/post process commands to be executed
    serially, and an optional directory to change to before executing the main
    parallel command. Once appropriate resources for the task have been found,
    and the execute() method is called, the class `slots` attribute will be
    filled with an `(offset, extent)` pair indicating what continuous region of
    the available task slots is being occupied by the currently running task.
    The command to be executed is then wrapped into a script file that is stored
    in `workdir` and a :class:subprocess.Popen object is opened to execute the
    script.

    Note that the :class:SLURMTaskQueue handles the initialization and
    management of task objects, and in general a user has no need to initialize
    task objects individually.

    Attributes
    ----------
    task_id : int
        Unique task ID to assign to this Task.
    cmnd : str
        Main command to be wrapped in `ibrun` with the appropriate offset/extent
        parameters for parallel execution.
    cores : int
        Number of cores, which correspond to SLURM job task numbers, to use for
        the job.
    pre : str
        Command to be executed in serial before the main parallel command.
    post : str
        Command to be executed in serial after the main parallel command.
    cdir: str
        directory to change to before executing the main parallel command.
    workdir : str
        directory to store execution script, along with output and error files.
    execfile : str
        Path to shell script containing wrapped command to be run by the
        subprocess that is spawned to executed the SLURM task. Note this file
        won't exist until the task is executed.
    logfile : str
        Path to file where stdout of the SLURM task will be redirected to. Note
        this file won't exist until the task is executed.
    errfile : str
        Path to file where stderr of the SLURM task will be redirected to. Note
        this file won't exist until the task is executed.
    slots : Tuple(int)
        `(offset, extent)` tuple in SLURM Task slots where task is currently
        being/was executed, or None if task has not been executed yet.
    start_ts : float
        Timestamp, in seconds since epoch, when task execution started, or None
        if task has not been executed  yet.
    end_ts : float
        Timestamp, in seconds since epoch, when task execution finished, as
        measured by first instance the process is polled using `get_rc()` with a
        non-negative response, or None if task has not finished yet.
    running_time : float
        Timestamp, in seconds since epoch, when task execution finished, as
        measured by first instance the process is polled using `get_rc()` with a
        non-negative response, or None if task has not finished yet.
    """
    def __init__(self,
            task_id: int,
            cmnd: str,
            workdir: str,
            cores: int=1,
            pre: str=None,
            post: str=None,
            cdir: str=None):

        self.task_id = task_id
        self.command = cmnd
        self.cores = int(cores)
        self.pre = pre
        self.post = post
        self.cdir = cdir

        if self.cores <= 0:
            raise ValueError(f'Cores for task must be >=0')

        self.workdir = Path(workdir)
        self.workdir.mkdir(exist_ok=True)
        self.logfile = self.workdir / f"{task_id}-log"
        self.errfile = self.workdir / f"{task_id}-err"
        self.execfile = self.workdir / f"{task_id}-exec"

        self.slots = None
        self.start_ts = None
        self.end_ts = None
        self.running_time = None
        self.rc = None
        self.err_msg = None
        self.pid = None
        self.sub_proc = None

    def __str__(self):
        s = f"(id:{self.task_id}, cmnd:<<{self.command}>>, cores:{self.cores}, "
        s += f"pre:<<{self.pre}>>, post:<<{self.post}>>, cdir:{self.cdir})"
        return s

    def __repr__(self):
        s = f"Task({self.task_id}, {self.command}, {self.cores}, "
        s += f"{self.pre}, '{self.post}', {self.cdir})"
        return s

    def _wrap(self, offset, extent):
        """Take a commandline, write it to a small file, and return the
        commandline that sources that file
        """
        f = open(self.execfile, "w")
        f.write("#!/bin/bash\n\n")
        if self.pre is not None:
            f.write(f"{self.pre}\n")
            f.write("if [ $? -ne 0 ]\n")
            f.write("then\n")
            f.write("  exit 1\n")
            f.write("fi\n")
        if self.cdir is not None:
            f.write(f"cd {self.cdir}\n")
            f.write(f"cwd=$(pwd)\n")
        f.write(f"ibrun -o {offset} -n {extent} {self.command}\n")
        if self.cdir is not None:
            f.write(f"cd $cwd\n")
        f.write("if [ $? -ne 0 ]\n")
        f.write("then\n")
        f.write("  exit 1\n")
        f.write("fi\n")
        if self.post is not None:
            f.write(f"{self.post}\n")
            f.write("if [ $? -ne 0 ]\n")
            f.write("then\n")
            f.write("  exit 1\n")
            f.write("fi\n")
        f.close()
        os.chmod(
            self.execfile,
            stat.S_IXUSR
            + +stat.S_IXGRP
            + stat.S_IXOTH
            + stat.S_IWUSR
            + +stat.S_IWGRP
            + stat.S_IWOTH
            + stat.S_IRUSR
            + +stat.S_IRGRP
            + stat.S_IROTH,
        )

        new_command = f"{self.execfile} > {self.logfile} 2> {self.errfile}"

        return new_command

    def execute(self, offset, extent):
        """
        Execute a wrapped command on subprocesses given a task slot range.

        Parameters
        ----------
        offset : int
            Offset in list of total available SLURM tasks available. This will
            determine the `-o` paraemter to run ibrun with.
        extent : int
            Extent, or number of slots, to occupy in list of total available
            SLURM tasks available. This will determine the `-n` parameter to
            run ibrun with.

        """
        self.start_ts = time.time()
        self.slots = (offset, extent)
        self.sub_proc = subprocess.Popen(
            self._wrap(offset, extent), shell=True, stdout=subprocess.PIPE
        )
        self.pid = self.sub_proc.pid

    def terminate(self):
        """Terminate subprocess executing task if it exists."""
        if self.sub_proc is not None:
            self.sub_proc.terminate()

    def get_rc(self):
        """Poll process to see if completed"""
        self.rc = self.sub_proc.poll()
        if self.rc is not None:
            self.end_ts = time.time()
            self.running_time = self.end_ts - self.start_ts
            if self.rc > 0:
                if Path(self.errfile).stat().st_size>0:
                    with open(self.errfile, 'r') as lf:
                        self.err_msg = lf.readlines()[-1]
            return self.rc

        return self.rc


class SLURMTaskQueue:
    """
    Object that does the maintains a list of Task objects.
    This is internally created inside a ``LauncherJob`` object.


    Attributes
    ----------
    task_slots : List(:class:Slot)
        List of task slots available. This is parsed upon initialization from
        SLURM environment variables SLURM_JOB_NODELIST and SLURM_TASKS_PER_NODE.
    workdir : str
        Path to directory to store files for tasks executed, if the tasks
        themselves dont specify their own work directories. Defaults to a
        directory with the prefix `.stq-job{SLURM_JOB_ID}-` in the
        current working directory.
    delay : float
        Number of seconds to pause between iterations of updating the queue.
        Default is 1 second. Note this affects the poll rate of tasks runing
        in the queue.
    task_max_runtime : float
        Max run time, in seconds, any individual task in the queue can run for.
    max_runtime : float
        Max run time, in seconds, for execution of `run()` to empty the queue.
    task_count : int
        Running counter, starting from 0, of total tasks that pass through the
        queue. The current count is used for the task_id of the next task added
        to the queue, so that a tasks task_id corresponds to the order in which
        it was added to the queue.
    running_time : float
        Total running time of the queue when `run()` is executed.
    queue : List(:class:Task)
        List of :class:Task in queue. Populated via the
        `enqueue_from_json()` method.
    running : List(:clas:Task)
        List of :class:Task that are currently running.
    completed : List(:clas:Task)
        List of :class:Task that are completed running successfully, in
        that the process executing them returned a 0 exit code.
    errored : List(:clas:Task)
        List of :class:Task that failed to run successfully in that the
        processes executing them returned a non-zero exit code..
    timed_out : List(:clas:Task)
        List of :class:Task that failed to run successfully in that the
        their runtime exceeded `task_max_runtime`.
    invalid : List(:clas:Task)
        List of :class:Task that were not run because their configurations
        were invalid, or the amount of resources required to run them was too
        large.
    """
    def __init__(
        self,
        commandfile: str,
        workdir: str = None,
        task_max_runtime: float = 1e10,
        max_runtime: float = 1e10,
        delay: float = 1,
        loglevel: int = logging.DEBUG,
    ):
        # Default workdir for executing tasks if task doesn't specify workdir
        self.workdir = workdir
        if self.workdir is None:
            self.workdir = Path(
                tempfile.mkdtemp(
                    prefix=f'.stq-job{os.environ["SLURM_JOB_ID"]}-',
                    dir=Path.cwd(),
                )
            )
        else:
            self.workdir = Path(workdir) if type(workdir) != Path else workdir
            self.workdir.mkdir(exist_ok=True)

        # Set-up job logging
        self._logger = logging.getLogger(__name__)
        _logHandler = logging.FileHandler(self.workdir / 'tq_log.json')
        _formatter = jsonlogger.JsonFormatter(
                '%(asctime)s %(name)s - %(levelname)s:%(message)s')
        _logHandler.setFormatter(_formatter)
        self._logger.addHandler(_logHandler)
        self._logger.setLevel(loglevel)

        # Node list - Initialize from SLURM environment
        self.task_slots = []
        self._init_task_slots()

        # Set queue runtime constants
        self.delay = delay
        self.task_max_runtime = task_max_runtime
        self.max_runtime = max_runtime

        # Initialize Task Queue Arrays
        self.task_count = 0
        self.running_time = 0.0
        self.queue = []
        self.running = []
        self.completed = []
        self.errored = []
        self.timed_out = []
        self.invalid = []

        # Enqueue tasks from json file
        self.enqueue_from_json(commandfile)

        self._logger.info(f'Queue initialized: {self}', extra=self.__dict__)

    def __str__(self):
        queue_str = ""
        sc = lambda x :  _compact_int_list(sorted([t.task_id for t in x]))
        if len(self.queue) > 0:
            queue_str += f"queued=[{sc(self.queue)}], "
        if len(self.running) > 0:
            queue_str += f"running=[{sc(self.running)}], "
        if len(self.completed) > 0:
            queue_str += f"completed=[{sc(self.completed)}], "
        if len(self.timed_out) > 0:
            queue_str += f"timed_out=[{sc(self.timed_out)}], "
        if len(self.errored) > 0:
            queue_str += f"errored=[{sc(self.errored)}, "
        queue_str = queue_str[:-2] if len(queue_str)!=0 else queue_str

        unique_slots = list(set([s.host for s in self.task_slots]))
        status = []
        for h in unique_slots:
            free = []; busy = [];
            for s in self.task_slots:
                if s.host == h:
                    if s.is_free():
                        free.append(s.idx)
                    else:
                        busy.append(s.idx)
            status.append((h, _compact_int_list(free), _compact_int_list(busy)))
        slots = [f'{x[0]}: (FREE: [{x[1]}], BUSY: [{x[2]}])' for x in status]

        s = f"(workdir: {self.workdir}, "
        s += f"slots: [{', '.join(slots)}], "
        s += f"queue-state:[{queue_str}])"

        return s

    def _init_task_slots(self):
        """Initialize available task slots from SLURM environment variables"""
        hl = []
        slurm_nodelist = os.environ["SLURM_JOB_NODELIST"]
        self._logger.debug(f'Parsing SLURM_JOB_NODELIST {slurm_nodelist}',
                extra={'SLURM_JOB_NODELIST':slurm_nodelist})
        host_groups = re.split(r",\s*(?![^\[\]]*\])", slurm_nodelist)
        for hg in host_groups:
            splt = hg.split("-")
            h = splt[0] if type(splt) == list else splt
            ns = "-".join(splt[1:])
            ns = ns[1:-1] if ns[0] == "[" else ns
            padding = min([len(x) for x in re.split(r"[,-]", ns)])
            hl += [f"{h}-{str(x).zfill(padding)}" for x in _expand_int_list(ns)]
        self._logger.debug(f'Parsed nodelist {hl}', extra={'hl':hl})

        tasks_per_host = []
        slurm_tph = os.environ["SLURM_TASKS_PER_NODE"]
        self._logger.debug(f'Parsing SLURM_TAKS_PER_NODE {slurm_tph}')
        total_idx = 0
        for idx, tph in enumerate(slurm_tph.split(",")):
            mult_split = tph.split("(x")
            ntasks = int(mult_split[0])
            if len(mult_split) > 1:
                for i in range(int(mult_split[1][:-1])):
                    tasks_per_host.append(ntasks)
                    for j in range(ntasks):
                        self.task_slots.append(Slot(hl[idx], total_idx + j))
                        self._logger.debug(
                                f'Initialized slot {self.task_slots[-1]}')
                    total_idx += ntasks
            else:
                for j in range(ntasks):
                    self.task_slots.append(Slot(hl[idx], total_idx + j))
                    self._logger.debug(f'Initialized slot {self.task_slots[-1]}')
                total_idx += ntasks
        self._logger.debug(f'Initialized {len(self.task_slots)}')

    def _request_slots(self, task):
        """Request a number of slots for a task"""
        start = 0
        found = False
        cores = task.cores
        while not found:
            if start + cores > len(self.task_slots):
                return False
            for i in range(start, start + cores):
                found = self.task_slots[i].is_free()
                if not found:
                    start = i + 1
                    break

        # If reach here -> Execute task on offset equal to start
        self._logger.debug(f"Starting {task.task_id} at slot index {start}",
                extra=task.__dict__)
        task.execute(start, cores)
        self._logger.info(
                f"{task.task_id} running on process {task.sub_proc.pid}",
                extra=task.__dict__)

        # Mark slots as occupied with with task_id
        for n in range(start, start + cores):
            s = self.task_slots[n]
            self._logger.debug(f'Occupying slot{s}', extra=s.__dict__)
            s.occupy(task)
            self._logger.debug(f'Slot{s} occupied', extra=s.__dict__)

        return True

    def _release_slots(self, task_id):
        """Given a task id, release the slots that are associated with it"""
        for s in self.task_slots:
            if not s.is_free():
                if s.tasks[-1].task_id == task_id:
                    self._logger.debug(f'Releasing slot {s}', extra=s.__dict__)
                    s.release()
                    self._logger.debug(f'Slot {s} released', extra=s.__dict__)

    def _start_queued(self):
        """
        Start queued tasks. For all queued, try to find a continuous set of
        slots equal to the number of cores required for the task. The tasks are 
        looped through in decreasing order of number of cores required. If the
        task is to big for the whole set of available slots, it is automatically
        added to the invalid list. Otherwise `_request_slots` is called to see
        if there space for the task to be run in the available slots.
        """
        # Sort queue in decreasing order of # of cores
        tqueue = copy.copy(self.queue)
        tqueue.sort(key=lambda x: -x.cores)
        for task in tqueue:
            if task.cores > len(self.task_slots):
                self._logger.warning(
                        f"Task {task} to large. Adding to invalid list.",
                        extra=task.__dict__)
                self.queue.remove(task)
                self.invalid.append(task)
                continue
            if self._request_slots(task):
                self._logger.info(
                        f"Successfully found resources for task {task}",
                        extra=task.__dict__)
                self.queue.remove(task)
                self.running.append(task)
            else:
                self._logger.debug(
                        f"Unable to find resources for {task}.",
                        extra=task.__dict__)

        num_removed = len(tqueue) - len(self.queue)
        if num_removed > 0:
            self._logger.info(f"{num_removed} tasks removed from queue",
                    extra=self.__dict__)

    def _update(self):
        """
        Update status of tasks in queue by calling polling subprocesses
        executing them with `get_rc()`. Tasks are added to the erorred or
        completed lists, or terminated and added to timed_out list if
        `task_max_runtime` is exceeded.
        """
        to_release_task_ids = []
        running = []
        for t in self.running:
            rc = t.get_rc()
            if rc is None:
                rt = time.time() - t.start_ts
                if rt > self.task_max_runtime:
                    self._logger.error(
                            f"Task {t.task_id} has exceeded max runtime {rt}",
                            extra=t.__dict__)
                    t.terminate()
                    self.timed_out.append(t)
                    to_release_task_ids.append(t.task_id)
                else:
                    running.append(t)
            else:
                if rc == 0:
                    self._logger.info(
                            f"{t.task_id} DONE: {t.running_time:5.3f}s",
                            extra=t.__dict__)
                    self.completed.append(t)
                    to_release_task_ids.append(t.task_id)
                else:
                    msg = f"{t.task_id} FAILED: rt = {t.running_time:5.3f}s, "
                    msg += f"rc = {t.rc}, err file (last_line) = {t.err_msg}"
                    self._logger.error(msg, extra=t.__dict__)
                    self.errored.append(t)
                    to_release_task_ids.append(t.task_id)

        self.running = running

        # Release slots for completed tasks
        if len(to_release_task_ids) > 0:
            self._logger.info(
                    f"Releasing slots related to nodes {to_release_task_ids}",
                    extra={'to_release': to_release_task_ids})
            for task_id in to_release_task_ids:
                self._release_slots(task_id)
            self._logger.info(f"Queue updated {self}", extra=self.__dict__)

    def enqueue_from_json(self, filename, cores=1):
        """
        Add a list of tasks to the queue from a JSON file. The json file must
        contain a list of configurations, with at mininum each containing a
        `cmnd` field indicating the command to be executed in parallel using
        a corresponding number of `cores`, which defaults to the passed in value
        if not specified per task configuration.

        Parameters
        ----------
        filename : str
            Path to json files containing list of json configurations, one per
            task to add to the queue.
        cores : int
            Default number of cores to use for each task if not specified within
            task configuration.

        """
        self._logger.debug(f'Loading json task file {filename}')
        with open(filename, "r") as fp:
            task_list = json.load(fp)
        self._logger.debug(f'Found {len(task_list)} tasks.')

        for i, t in enumerate(task_list):
            self._logger.debug(f'Attempting to create task {self.task_count}',
                    extra=t)
            try:
                task = Task(
                        self.task_count,
                        t.pop('cmnd', None),
                        t.pop('workdir', self.workdir),
                        t.pop('cores', cores),
                        t.pop('pre', None),
                        t.pop('post', None),
                        t.pop('cdir', None))
            except ValueError as v:
                self._logger.error(f'Bad task in list at idx {i}: {v}', extra=t)
                continue
            self._logger.debug(f'Enqueing {task}', extra=task.__dict__)
            self.queue.append(task)
            self.task_count += 1

    def run(self):
        """
        Runs tasks and wait for all tasks in queue to complete, or until
        `max_runtime` is exceeded.
        """
        self.start_ts = time.time()
        self._logger.info("Starting launcher job", extra=self.__dict__)
        while True:
            # Check to see if max runtime is exceeded
            elapsed = time.time() - self.start_ts
            self._logger.debug("Starting run iteration",
                    extra={'elapsed_time': elapsed})
            if elapsed >= self.max_runtime:
                self._logger.info("Exceeded max runtime", extra=self.__dict__)
                break

            # Start queued jobs
            self._logger.debug("Starting queued tasks", extra=self.__dict__)
            self._start_queued()

            # Update queue for completed/errored jobs
            self._logger.debug("Updating task lists", extra=self.__dict__)
            self._update()

            # Wait for a bit
            time.sleep(self.delay)

            # Check if done
            if len(self.running) == 0:
                if len(self.queue) == 0:
                    self._logger.info(f"Running and queue are empty.")
                    break

        self.running_time = time.time() - self.start_ts

    def read_log(self):
        """Return read json log"""
        log_entries = []
        with open(self.workdir / 'tq_log.json', 'r') as f:
            for line in f:
                log_entries.append(json.loads(line))
        return log_entries

    def view_log(self,
            fields=['asctime', 'levelname', 'message'],
            search='message',
            match=r'.'):
        """Print log entries"""
        log = self.read_log()
        _print_res(log, fields=fields, search=search, match=match)

    def cleanup(self):
        """Clean-up Task Queue by removing workdir"""
        shutil.rmtree(str(self.workdir))

