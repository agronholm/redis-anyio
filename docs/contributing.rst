Contributing to Redis-AnyIO
===========================

If you wish to contribute a fix or feature to Redis-AnyIO, please follow these
guidelines.

When you make a pull request against the main AnyIO codebase, GitHub runs the AnyIO test
suite against your modified code. Before making a pull request, you should ensure that
the modified code passes tests locally. To that end, the use of tox_ is recommended. The
default tox run first runs ``pre-commit`` and then the actual test suite.

.. note:: Do not use ``tox -p`` (parallel mode) when running against multiple tox
    environments, as the integration tests cannot be safely run against the same server.

To build the documentation, run ``tox -e docs`` which will generate a directory named
``build`` in which you may view the formatted HTML documentation.

Redis-AnyIO uses pre-commit_ to perform several code style/quality checks. It is
recommended to activate pre-commit_ on your local clone of the repository (using
``pre-commit install``) to ensure that your changes will pass the same checks on GitHub.

.. _tox: https://tox.wiki/en/latest/installation.html
.. _pre-commit: https://pre-commit.com/#installation

Making a pull request on GitHub
-------------------------------

To get your changes merged to the main codebase, you need a GitHub account.

#. Fork the repository (if you don't have your own fork of it yet) by navigating to the
   `main Redis-AnyIO repository`_ and clicking on "Fork" near the top right corner.
#. Clone the forked repository to your local machine with
   ``git clone git@github.com/yourusername/redis-anyio``.
#. Create a branch for your pull request, like ``git checkout -b myfixname``.
#. Make the desired changes to the code base.
#. Commit your changes locally. If your changes close an existing issue, add the text
   ``Fixes XXX.`` or ``Closes XXX.`` to the commit message (where XXX is the issue
   number).
#. Push the changeset(s) to your forked repository (``git push``).
#. Navigate to the pull requests page on the original repository (not your fork) and click
   "New pull request".
#. Click on the text "compare across forks".
#. Select your own fork as the head repository and then select the correct branch name.
#. Click on "Create pull request".

If you have trouble, consult the `pull request making guide`_ on opensource.com.

.. _main Redis-AnyIO repository: https://github.com/agronholm/redis-anyio
.. _pull request making guide:
    https://opensource.com/article/19/7/create-pull-request-github
