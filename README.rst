.. image:: https://img.shields.io/coveralls/<USER>/pyslurmtq.svg
        :alt: Coveralls
        :target: https://coveralls.io/r/<USER>/pyslurmtq
.. image:: https://img.shields.io/pypi/v/pyslurmtq.svg
        :alt: PyPI-Server
        :target: https://pypi.org/project/pyslurmtq/


=========
pyslurmtq
=========


    A Python SLURM Task Queue for batch job submission.


`pyslurmtq` is a Python library for managing a queue of tasks to be executed on a SLURM cluster. It provides a simple interface for enqueuing tasks from a JSON file, running the tasks on the cluster, and monitoring the status of the tasks.

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

Task Files
----------

The task file contains the list of tasks to be executed.

# .rst table explaining each task json field
Task File Fields
----------------

The task file contains a list of tasks to be executed. Each task is a dictionary with the following fieldsTask File Fields
----------------

The task file contains a list of tasks to be executed. Each task is a dictionary with the following fields:

.. list-table:: Task File Fields
   :widths: 25 25 50
   :header-rows: 1

   * - Field
       - Description
       - Optional?
   * - command
       - The command to be executed.
       - No
   * - cores
       - The number of CPU cores to allocate for the task.
       - Yes
   * - pre
       - The command to be executed in serial before the main parallel command.
       - Yes
   * - post
       - The command to be executed in serial after the main parallel command.
       - Yes
   * - cdir
       - The directory to change to before executing the main parallel command.
       - Yes


.. code-block:: json

    [
        {
            "command": "parallel_main_entry",
            "cores": 4,
            "pre": "mkdir -p /path/to/workdir/job_task/",
            "post": "mv /path/to/output /path/to/workdir/job_task/",
        },
        {
            "command": "parallel_main_entry",
            "cores": 8,
            "pre": "mkdir -p /path/to/workdir/job_task/",
            "post": "",
        }
    ]
    

This file contains two tasks, one that runs the `echo` command and one that runs a Python script. To run these tasks using `pyslurmtq`, you can save the file as `tasks.json` and run the following command:

.. code-block:: bash

    pyslurmtq run --tasks tasks.json

This will enqueue the tasks and run them on the SLURM cluster.

Contact Info and Open Bugs/Issues/Feature Requests in GitHub
------------------------------------------------------------

If you have any questions or issues with `pyslurmtq`, please feel free to contact us at `cdelcastilloew@gmail.com`. You can also report bugs, issues, or feature requests on our GitHub page: https://github.com/pyslurmtq/pyslurmtq/issues.


.. _pyscaffold-notes:

Note
====

This project has been set up using PyScaffold 4.3. For details and usage
information on PyScaffold see https://pyscaffold.org/.
cdelcastillo21: Break-up the TACCSimulation setup() method into smaller chunks
