.. include:: ../CONTRIBUTING.rst

Contributing to PySlurmTQ
-------------------------

Thank you for your interest in contributing to PySlurmTQ! This guide will help you get started with contributing to the project.

Dev Environment Set-Up
-----------------------

To get started with contributing to PySlurmTQ, you will need to set up a development environment on your local machine. We recommend using mamba/conda to create a new environment for development.

1. Clone the PySlurmTQ repository to your local machine:

   ```
   git clone <dir>
   ```

2. Create a new conda environment for development:

   ```
   conda create --name pyslurmtq-dev -y -c conda-forge pip
   ```

3. Activate the new environment:

   ```
   conda activate pyslurmtq-dev
   ```

4. Install PyScaffold and its dependencies:

   ```
   pip install pyscaffold[all]
   ```

Testing
-------

We use pytest for testing PySlurmTQ. To run the tests, simply run the following command from the root directory of the project:

```
pytest
```

Reporting Bugs and Issues
-------------------------

If you encounter any bugs or issues while using PySlurmTQ, please report them on the project's GitHub page:

```
https://github.com/<USER>/pyslurmtq/issues
```

Contributing Code
-----------------

To contribute code to PySlurmTQ, please follow these steps:

1. Fork the PySlurmTQ repository on GitHub.

2. Create a new branch for your changes:

   ```
   git checkout -b my-new-feature
   ```

3. Make your changes and commit them to your branch.

4. Push your changes to your forked repository:

   ```
   git push origin my-new-feature
   ```

5. Create a pull request on the PySlurmTQ repository.

We appreciate all contributions to PySlurmTQ, and we will review your pull request as soon as possible. Thank you for your help in improving the project!