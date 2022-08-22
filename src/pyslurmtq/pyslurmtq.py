"""
This is a skeleton file that can serve as a starting point for a Python
console script. To run this script uncomment the following lines in the
``[options.entry_points]`` section in ``setup.cfg``::

    console_scripts =
         fibonacci = pyslurmqt.skeleton:run

Then run ``pip install .`` (or ``pip install -e .`` for editable mode)
which will install the command ``fibonacci`` inside your current environment.

Besides console scripts, the header (i.e. until ``_logger``...) of this file can
also be used as template for Python modules.

Note:
    This file can be renamed depending on your needs or safely removed if not needed.

References:
    - https://setuptools.pypa.io/en/latest/userguide/entry_point.html
    - https://pip.pypa.io/en/stable/reference/pip_install
"""

import argparse
import logging
import sys
from SLURMTaskQueue import SLURMTaskQueue

from pyslurmqt import __version__

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

_logger = logging.getLogger(__name__)

# ---- CLI ----

def parse_args(args):
    """Parse command line parameters

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--help"]``).

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(description="SLURM Task Queue")
    parser.add_argument(
        "--version",
        action="version",
        version="pyslurmqt {ver}".format(ver=__version__),
    )
    parser.add_argument(dest="infile",
            help="input json file with tasks to execute",
            type=str)
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )
    return parser.parse_args(args)


def setup_logging(loglevel):
    """Setup basic logging

    Parameters
    ----------
    loglevel : int
        minimum loglevel for emitting messages
    """
    # Initialize Logging
    logger = logging.getLogger("pylauncher")
    logformat = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    logging.basicConfig(
        level=loglevel,
        stream=sys.stdout,
        format=logformat,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main(args):
    """Wrapper allowing a :class:`SLURMTaskQueue` to be initialized and run
    from an input json file containing a list of tasks.

    Parameters
    ----------
    args : List[str]
        command line parameters as list of strings (for example
        ``["--verbose", "42"]``).
    """
    args = parse_args(args)

    _logger.info(f"Initializing Task Queue from file {args.infile}")
    tq = SLURMTaskQueue(args.infile)
    _logger.info(f"Running Task Queue")
    tq.run()
    _logger.info(f"Done Running Tasks in Queue")


def run():
    """Calls :func:`main` passing the CLI arguments from :obj:`sys.argv`

    This function is the main entry point for the CLI.
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()

