======================================
Develop: Including Custom Subcommands
======================================

``pycmor`` is built in a modular way such that it is easy to extend by adding new command line subcommands via Python's `entry_points` mechanism.
You can add your own such subcommands by creating a Python package with a ``Click.Group`` object and registering it as an entry point in your ``setup.py``.

Here is an example of how to create a custom subcommand for ``pycmor``. Let's assume you have a very simple project layout like this:

.. code-block:: bash

    my_project/
    ├── my_project/
    │   ├── __init__.py
    │   └── cli.py
    └── setup.py

In the ``cli.py`` file, you can define a new subcommand like this:

.. code-block:: python

    import click

    @click.command()
    def my_subcommand():
        click.echo('Hello from my subcommand!')

    my_group = click.Group()
    my_group.add_command(my_subcommand)

Then, in your ``setup.py`` file, you can register this subcommand as an entry point like this:

.. code-block:: python

    from setuptools import setup

    setup(
        name='my_project',
        version='0.1',
        packages=['my_project'],
        entry_points={
            'pycmor.subcommands': [
                'my_subcommand = my_project.cli:my_group',
            ],
        },
    )

After installing your package, you should be able to run your subcommand like this::

    $ pycmor my_subcommand
    Hello from my subcommand!

That's it! ``pycmor`` will automatically discover your subcommand and make it available to the user. To list available subcommands, run ``pycmor --help``, or ``pycmor plugins list``.

For more information on how to create custom subcommands, see the `Click documentation <https://click.palletsprojects.com/en/7.x/setuptools/#setuptools-integration>`_.
