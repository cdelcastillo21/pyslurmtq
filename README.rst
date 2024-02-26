.. image:: https://readthedocs.org/projects/pyslurmtq/badge/?version=latest
    :target: https://pyslurmtq.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://img.shields.io/pypi/v/pyslurmtq
   :alt: PyPI - Version

.. image:: https://img.shields.io/github/last-commit/cdelcastillo21/pyslurmtq/master?logo=Git
   :alt: GitHub last commit (branch)

.. image:: https://codecov.io/gh/cdelcastillo21/pyslurmtq/graph/badge.svg?token=Y6YmyncCKI
 :target: https://codecov.io/gh/cdelcastillo21/pyslurmtq

.. image:: https://coveralls.io/repos/github/cdelcastillo21/pyslurmtq/badge.svg?branch=ci-cd
  :target: https://coveralls.io/github/cdelcastillo21/pyslurmtq?branch=ci-cd



=========
pyslurmtq
=========


    A Python SLURM Task Queue for batch job submission.


`pyslurmtq` is a Python library for managing a queue of tasks to be executed on an HPC cluster using a `SLURM <https://slurm.schedmd.com>`_ schedule manager.
It provides a python class for managing a task queue, as well as a command-line interface (CLI) for running a task queue from a JSON file containing task configurations.
It is designed to be flexible and easy to use, and to provide a simple interface for running a batch of tasks on a SLURM managed HPC cluster.

This project was originally motiviated by the `pylauncher <https://github.com/TACC/pylauncher>`_ project, which provides a similar interface for running a batch of tasks on a SLURM managed HPC cluster.

Installation
------------

To install `pyslurmtq`, you can use `pip`:

.. code-block:: bash

    pip install pyslurmtq

Usage
-----

To use `pyslurmtq`, you can create a JSON file containing the task configurations, and then run the `pyslurmtq` command-line interface (CLI) with the path to the JSON file as an argument. For example:

.. code-block:: bash

    pyslurmtq run --tasks tasks.json

This will run the tasks specified in the `tasks.json` file on the SLURM cluster.


For example, to configure a batch of three jobs, the first and third with 8 cores and the second with 4 cores, you can create a JSON file with the following contents:

.. code-block:: json

    [
        {
            "workdir": "output_1",
            "cmnd": "parallel_main_entry > output.txt",
            "cores": 8,
        },
        {
            "cmnd": "parallel_main_entry > output.txt",
            "cores": 4,
            "pre": "mkdir -p output_2",
            "post": "mv output.txt output_2/output.txt",
        },
        {
            "cmnd": "parallel_main_entry > output.txt",
            "cores": 8,
            "cdir": "output_3",
            "pre": "mkdir -p output_3",
        }
    ]

Note how the first task uses the workdir argument to have the task queue create a directory for the task to run in, which is relative to where the queue was invoked in.
The second task creates an output directory before running the command, and moves the output to that directory after the command finishes.
Finall the third task creates the output directory before running the command, but does not move the output after the command finishes, since the 'cdir' field ensures the main command is executed in the output directory created by the pre process command.
This demonstrates how the task list is flexible enough to handle a variety of use cases.

The full list of configurable options for task fields is shown below:

.. list-table:: Task File Fields
   :widths: 25 25 50
   :header-rows: 1

   * - Field
     - Description
     - Optional?
   * - cmd
     - The main command to be executed in parallel.
     - No
   * - cores
     - The number of CPU cores to allocate for the task.
     - Yes
   * - pre
     - Pre-process command to be executed in serial before the main parallel command.
     - Yes
   * - post
     - Post-process command to be executed in serial after the main parallel command.
     - Yes
   * - cdir
     - The directory to change to before executing the main parallel command.
     - Yes
   * - workdir
     - Directory to use as the working directory for the task. Will be created if it does not exist.
     - Yes

Upon completion, two summaries are printed.
The first is a summary by task, indicated how long each task took to run, for example:

.. code-block:: bash

    +-----------+---------+--------------------+-------+-----------+
    |   status  | task_id |    running_time    | cores |  command  |
    +-----------+---------+--------------------+-------+-----------+
    | completed |    0    | 1.0178141593933105 |   1   | echo main |
    | completed |    1    | 1.0130047798156738 |   1   | echo main |
    | completed |    2    | 1.008800983428955  |   1   | echo main |
    |  errored  |    4    | 1.0217607021331787 |   4   | echo main |
    |  errored  |    3    | 1.0207343101501465 |   2   | echo main |
    +-----------+---------+--------------------+-------+-----------+

The second is a summary by compute slot available, along with how many tasks were executed on it, which tasks were executed on it, and the total free time and busy time for the node, for example:

.. code-block:: bash

    +-----+----------+--------+-----------+----------+----------------------+--------------------+
    | idx |   host   | status | num_tasks | task_ids |      free_time       |     busy_time      |
    +-----+----------+--------+-----------+----------+----------------------+--------------------+
    |  0  | c302-005 |  FREE  |     1     |   [4]    | 0.010022163391113281 | 1.020758867263794  |
    |  1  | c302-005 |  FREE  |     1     |   [4]    | 0.01005411148071289  | 1.0207319259643555 |
    |  2  | c302-005 |  FREE  |     1     |   [4]    | 0.01006174087524414  | 1.0207273960113525 |
    |  3  | c302-005 |  FREE  |     1     |   [4]    | 0.010066509246826172 | 1.0207266807556152 |
    |  4  | c302-005 |  FREE  |     1     |   [3]    | 0.014362573623657227 | 1.0178310871124268 |
    |  5  | c302-005 |  FREE  |     1     |   [3]    |  0.0143890380859375  | 1.0178096294403076 |
    |  6  | c302-005 |  FREE  |     1     |   [0]    | 0.019230127334594727 | 1.013009786605835  |
    |  7  | c302-005 |  FREE  |     1     |   [1]    | 0.023468732833862305 | 1.0088229179382324 |
    |  8  | c302-005 |  FREE  |     1     |   [2]    | 0.027381420135498047 | 1.0049347877502441 |
    |  9  | c302-005 |  FREE  |     0     |    []    |         0.0          |        0.0         |
    |  10 | c302-005 |  FREE  |     0     |    []    |         0.0          |        0.0         |
    |  11 | c302-005 |  FREE  |     0     |    []    |         0.0          |        0.0         |
    +-----+----------+--------+-----------+----------+----------------------+--------------------+

The CLI currently provides only an entrypoint to launch a task queue for a given task file.
For more advanced usage, the `pyslurmtq` library can be imported and used directly in Python code using the `SLURMTaskQueue` class.


Contact Info and Open Bugs/Issues/Feature Requests in GitHub
------------------------------------------------------------

If you have any questions or issues with `pyslurmtq`, please feel free to contact us at `cdelcastilloew@gmail.com`. You can also report bugs, issues, or feature requests on our GitHub page: https://github.com/pyslurmtq/pyslurmtq/issues.
