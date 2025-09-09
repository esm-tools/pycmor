=============================
Develop: Main Developer Guide
=============================

Thanks for helping develop ``pycmor``! This document will guide you through
the code structure and layout, and provide a few tips on how to contribute.

Getting Started
---------------
To get started, you should clone the repository and install the dependencies. We give
a few extra dependencies for testing and documentation, so you should install these as well::

    git clone https://github.com/esm-tools/pycmor.git
    cd pycmor
    pip install -e ".[dev,doc]"

This will install the package in "editable" mode, so you can make changes to the code. The
``dev`` and ``doc`` extras will install the dependencies needed for testing and documentation,
respectively. Before changing anything, make sure that the tests are passing::

    pytest

Next, you should familiarize yourself with the code layout and the main building blocks. These
are described in the next section.


Code Layout and Main Classes
----------------------------

We use a ``src`` layout, with all files living under ``./src/pycmor``. The code is
generally divided into several building blocks:

* :py:class:`~pycmor.rule.Rule` is the main class that defines a rule for processing a CMOR variable.

* :py:class:`~pycmor.pipeline.Pipeline` (or an object inherited from :py:class:`~pycmor.pipeline.Pipeline`) is a collection
  of actions that can be applied to a set of files described by a :py:class:`~pycmor.rule.Rule`. A few default pipelines are
  provided, and you can also define your own.

* :py:class:`~pycmor.cmorizer.CMORizer` is responsible for reading in the rules, and managing the various
  objects.

:py:class:`~pycmor.rule.Rule` Class
-------------------------------------

This is the main building block for handling a set of files produced by a model. It has the following attributes:

  1. ``input_patterns``: A list of regular expressions that are used to match the
     input file name. Note that this is **regex**, not globbing!
  2. ``cmor_variable``: The ``CMOR`` name of the variable.
  3. ``pipelines``: A list of pipeline names that should be applied to the data.

Any other attributes can be added to the rule, and will appear in the ``rule_spec`` as attributes of the ``Rule`` object. In YAML, a minimal rule
looks like this:

.. code-block:: yaml

    input_patterns: [".*"]
    cmor_variable: tas
    pipelines: [My Pipeline]


:py:class:`~pycmor.pipeline.Pipeline` Class
---------------------------------------------

The :py:class:`~pycmor.pipeline.Pipeline` class is a collection of actions that can be applied to a set of files. It should have a
``name`` attribute that describes the pipeline. If not given during construction, a random one is generated. The actions are stored in a list, and
are applied in order. There are a few ways to construct a pipeline. You can either create one from a list of actions (also called steps)::

    >>> pipeline = pycmor.pipeline.Pipeline([action1, action2], name="My Pipeline")
    >>> # Or use the class method:
    >>> pl = Pipeline.from_list([action1, action2], name="My Pipeline")

where ``action1`` and ``action2`` are functions that follow the pipeline step protocol. See :ref:`the guide on building actions <building-actions-for-pipelines>`
for more information.

Another way to build actions is from a list of qualified names of functions. A class method is provided to do this easily::

    >>> my_pipeline = Pipeline.from_qualnames(["my_module.my_action1", "my_module.my_action2"], name="My Pipeline")



:py:class:`~pycmor.cmorizer.CMORizer` Class
---------------------------------------------

The :py:class:`~pycmor.cmorizer.CMORizer` class is responsible for managing the rules and pipelines. It contains four configuration dictionaries:

1. ``pycmor_cfg``: This is the configuration for the ``pycmor`` package. It should contain a version number, and any other configuration
   that is needed for the package to run. This is used to check that the configuration is correct for the specific version of ``pycmor``. You
   can also specify certain features to be enabled or disabled here, as well as configure the logging.

2. ``global_cfg``: This is the global configuration for the rules and pipelines. This is used for configuration that is common to all rules and pipelines,
   such as the path to the CMOR tables, or the path to the output directory. This is used to set up the environment for the rules and pipelines.

3. ``pipelines``: This is a list of :py:class:`~pycmor.pipeline.Pipeline` objects that are used to process the data.

4. ``rules``: This is a list of :py:class:`~pycmor.rule.Rule` objects that are used to match the data.

.. _building-actions-for-pipelines:

Building Actions for Pipelines
------------------------------

When defining actions for a :py:class:`~pycmor.pipeline.Pipeline`, you should create functions
with the following signature::

    def my_action(data: Any,
                  rule_spec: pycmor.rule.Rule,
                  cmorizer: pycmor.cmorizer.CMORizer,
                  *args, **kwargs) -> Any:
        ...
        return data

The ``data`` argument is the data that is passed from one action to the next. The ``rule_spec`` is the
instance of the :py:class:`~pycmor.rule.Rule` class that is currently being evaluated. The ``cmorizer``
is the instance of the :py:class:`~pycmor.cmorizer.CMORizer` class that is managing the pipeline. You
can pass additional arguments to the action by using ``*args`` and ``**kwargs``, however most arguments or
keyword arguments should be extracted from the ``rule_spec``. The action should return the data that will be
passed to the next action in the pipeline. Note that the data can be any type, but it should be the same type
as what is expected in the next action in the pipeline.

.. note::

   If needed, you can construct "conversion" actions that will convert the data from one type to another and pass
   it to the next step.

When defining actions, you should also add a docstring that describes what the action does. This will be printed
when the user asks for help on the action. Note that whenever possible, you should use the ``rule_spec`` to pass
information into your action, rather than hardcoding it or passing in arguments. You can also use additional arguments
if needed, and these can be fixed to always use the same values for the entire pipeline the action belongs to, or,
alternatively, to the rule that the action is a part of. A few illustrative examples may make this clearer.

* Example 1: A simple action that adds 1 to the data::

      def add_one(data: Any, rule_spec: pycmor.rule.Rule, cmorizer: pycmor.cmorizer.CMORizer) -> Any:
          """Add one to the data."""
          return data + 1

  Using this in a pipeline would look like this in Python code::

      pipeline = pycmor.pipeline.Pipeline([add_one], name="Add One")
      rule_spec = pycmor.rule.Rule(input_patterns=[".*"], cmor_variable="tas", pipelines=["Add One"])
      cmorizer = pycmor.cmorizer.CMORizer(pymor_cfg={"version": "unreleased"}, global_cfg={}, rules=[rule_spec], pipelines=[pipeline])
      initial_data = 1
      data = pipeline.run(initial_data, rule_spec, cmorizer)

  In yaml, the same pipeline and configuration looks like this:

  .. code-block:: yaml

      pycmor:
        version: unreleased

      general:

      pipelines:
        - name: Add One
          actions:
            - add_one
      rules:
        - input_patterns: [".*"]
          cmor_variable: tas
          pipelines: [Add One]

* Example 2: An action that sets an attribute on a :py:class:`xarray.Dataset`, where this is specified in
  the rule specification::

      def set_attribute(data: xr.Dataset, rule_spec: pycmor.rule.Rule, cmorizer: pycmor.cmorizer.CMORizer) -> xr.Dataset:
          """Set an attribute on the dataset."""
          data.attrs[rule_spec.attribute_name] = rule_spec.attribute_value
          return data

  Using this in a pipeline would look like this in yaml:

  .. code-block:: yaml

      pycmor:
        version: unreleased

      general:

      pipelines:
        - name: Set Attribute
          actions:
            - set_attribute
      rules:
        - input_patterns: [".*"]
          cmor_variable: tas
          pipelines: [Set Attribute]
          attribute_name: "my_attribute"
          attribute_value: "my_value"

* Example 3: An action that sets an attribute on a :py:class:`~xarray.Dataset`, where this is specified in the :py:class:`~pycmor.pipeline.Pipeline`.

  It is the responsibility of the action developer to ensure arguments are passed correctly and have sensible values. This is a more complicated example. Here we check
  if the rule has a specific attribute that matches the action's name, with "``_args``" appended. We use those values if that is the case. Otherwise, they can be obtained from
  the pipeline, and default to empty strings. As an action developer, you need to ensure sensible logic here!

  .. code-block::

      def set_attribute(data: xr.Dataset, rule_spec: pycmor.rule.Rule, cmorizer: pycmor.cmorizer.CMORizer, attribute_name: str = "", attribute_value: str = "", *args, **kwargs) -> xr.Dataset:
          """Set an attribute on the dataset."""
          if hasattr(rule_spec, f"{__name__}_args"):
              attribute_name = getattr(rule_spec, f"{__name__}_args").get("attribute_name", my_attribute)
              attribute_value = getattr(rule_spec, f"{__name__}_args").get("attribute_value", my_value)
          data.attrs[attribute_name] = attribute_value
          return data

  Using this in a pipeline would look like this in yaml:

  .. code-block:: yaml

      pycmor:
        version: unreleased

      general:

      pipelines:
        - name: Set Attribute
          actions:
            - set_attribute
          attribute_name: "my_attribute"
          attribute_value: "my_value"
      rules:
        - input_patterns: [".*"]
          cmor_variable: tas
          pipelines: [Set Attribute]

  .. important::

      In the case of passing arguments that are *not* in the rule spec, you need to be careful about where you place the information. The :py:class:`~pycmor.rule.Rule` should win, if
      there are conflicts between the rule and the pipeline. This is because the rule is the most specific, and the pipeline is the most general. So, to have a value specified in
      the rule, you should do:

      .. code-block:: yaml

            pycmor:
              version: unreleased

            general:

            pipelines:
              - name: Set Attribute
                actions:
                  - set_attribute
                attribute_name: "my_attribute"
                attribute_value: "my_value"
            rules:
              - input_patterns: [".*"]
                cmor_variable: tas
                pipelines: [Set Attribute]
                set_attribute_args:
                  attribute_name: "my_other_attribute"
                  attribute_value: "my_other_value"

.. attention::

   If you want more examples in the handbook, please open an issue or a pull request!
