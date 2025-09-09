"""
Pipeline of the data processing steps.
"""

import copy
from datetime import timedelta

import randomname
from prefect import flow
from prefect.cache_policies import INPUTS, TASK_SOURCE
from prefect.tasks import Task
from prefect_dask import DaskTaskRunner

from .caching import generate_cache_key  # noqa: F401
from .cluster import DaskContext
from .logging import add_to_report_log, logger
from .utils import get_callable, get_callable_by_name


class Pipeline:
    def __init__(
        self,
        *args,
        name=None,
        workflow_backend=None,
        cache_policy=None,
        dask_cluster=None,
        cache_expiration=None,
    ):
        self._steps = args
        self.name = name or randomname.get_name()
        self._cluster = dask_cluster
        self._prefect_cache_kwargs = {}
        self._steps_are_prefectized = False
        if workflow_backend is None:
            workflow_backend = "prefect"
        self._workflow_backend = workflow_backend
        if cache_policy is None:
            self._cache_policy = TASK_SOURCE + INPUTS
            self._prefect_cache_kwargs["cache_policy"] = self._cache_policy

        if cache_expiration is None:
            self._cache_expiration = timedelta(days=1)
        else:
            if isinstance(cache_expiration, timedelta):
                self._cache_expiration = cache_expiration
            else:
                raise TypeError("Cache expiration must be a timedelta!")
        self._prefect_cache_kwargs["cache_expiration"] = self._cache_expiration

        if self._workflow_backend == "prefect":
            self._prefectize_steps()

    def __str__(self):
        name_header = f"Pipeline: {self.name}"
        name_uline = "-" * len(name_header)
        step_header = "steps"
        step_uline = "-" * len(step_header)
        r_val = [name_header, name_uline, step_header, step_uline]
        for i, step in enumerate(self.steps):
            r_val.append(f"[{i+1}/{len(self.steps)}] {step.__name__}")
        return "\n".join(r_val)

    def __getstate__(self):
        """Custom pickling of a Pipeline"""
        state = self.__dict__.copy()
        if self._steps_are_prefectized:
            state["_steps"] = self._raw_steps
            del state["_raw_steps"]
            state["_steps_are_prefectized"] = False
        if "_cluster" in state:
            # It makes no sense to pickle the cluster
            del state["_cluster"]

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        if self._workflow_backend == "prefect":
            self._prefectize_steps()
        logger.info("Restoring from pickled state!")
        # logger.info("You may want to assign a cluster to this pipeline")
        try:
            self._cluster = DaskContext.get_cluster()
        except RuntimeError:
            logger.warning("No cluster available to assign to this pipeline")

    def assign_cluster(self, cluster):
        logger.debug("Assigning cluster to this pipeline")
        self._cluster = cluster

    def _prefectize_steps(self):
        # Turn all steps into Prefect tasks:
        raw_steps = copy.deepcopy(self._steps)
        prefect_tasks = []
        for i, step in enumerate(self._steps):
            logger.debug(
                f"[{i+1}/{len(self._steps)}] Converting step {step.__name__} to Prefect task."
            )
            prefect_tasks.append(
                Task(
                    fn=step,
                    **self._prefect_cache_kwargs,
                    # cache_key_fn=generate_cache_key,
                )
            )

        self._steps = prefect_tasks
        self._steps_are_prefectized = True
        self._raw_steps = raw_steps

    @property
    def steps(self):
        return self._steps

    def run(self, data, rule_spec):
        if self._workflow_backend == "native":
            return self._run_native(data, rule_spec)
        elif self._workflow_backend == "prefect":
            return self._run_prefect(data, rule_spec)
        else:
            raise ValueError("Invalid workflow backend!")

    def _run_native(self, data, rule_spec):
        for step in self.steps:
            data = step(data, rule_spec)
        return data

    def _run_prefect(self, data, rule_spec):
        logger.debug("Dynamically creating workflow with DaskTaskRunner...")
        cmor_name = rule_spec.get("cmor_name")
        rule_name = rule_spec.get("name", cmor_name)
        if self._cluster is None:
            logger.warning(
                "No cluster assigned to this pipeline. Using local Dask cluster."
            )
            dask_scheduler_address = None
        else:
            dask_scheduler_address = self._cluster.scheduler.address

        @flow(
            flow_run_name=f"{self.name} - {rule_name}",
            description=f"{rule_spec.get('description', '')}",
            task_runner=DaskTaskRunner(address=dask_scheduler_address),
            on_completion=[self.on_completion],
            on_failure=[self.on_failure],
        )
        def dynamic_flow(data, rule_spec):
            return self._run_native(data, rule_spec)

        return dynamic_flow(data, rule_spec)

    @staticmethod
    @add_to_report_log
    def on_completion(flow, flowrun, state):
        logger.success("Success...\n")
        logger.success(f"{flow=}\n")
        logger.success(f"{flowrun=}\n")
        logger.success(f"{state=}\n")
        logger.success("Good job! :-) \n")

    @staticmethod
    @add_to_report_log
    def on_failure(flow, flowrun, state):
        logger.error("Failure...\n")
        logger.error(f"{flow=}\n")
        logger.error(f"{flowrun=}\n")
        logger.error(f"{state=}\n")
        logger.error("Better luck next time :-( \n")

    @classmethod
    def from_list(cls, steps, name=None, **kwargs):
        return cls(*steps, name=name, **kwargs)

    @classmethod
    def from_qualname_list(cls, qualnames: list, name=None, **kwargs):
        return cls.from_list(
            [get_callable_by_name(name) for name in qualnames], name=name, **kwargs
        )

    @classmethod
    def from_callable_strings(cls, step_strings: list, name=None, **kwargs):
        return cls.from_list(
            [get_callable(name) for name in step_strings], name=name, **kwargs
        )

    @classmethod
    def from_dict(cls, data):
        if "uses" in data and "steps" in data:
            raise ValueError("Cannot have both 'uses' and 'steps' to create a pipeline")
        if "uses" in data:
            # FIXME(PG): This is bad. What if I need to pass arguments to the constructor?
            return get_callable_by_name(data["uses"])(
                name=data.get("name"),
                cache_expiration=data.get("cache_expiration"),
                workflow_backend=data.get("workflow_backend"),
            )
        if "steps" in data:
            return cls.from_callable_strings(
                data["steps"],
                name=data.get("name"),
                cache_expiration=data.get("cache_expiration"),
                workflow_backend=data.get("workflow_backend"),
            )
        raise ValueError("Pipeline data must have 'uses' or 'steps' key")


class FrozenPipeline(Pipeline):
    """
    The FrozenPipeline class is a subclass of the Pipeline class. It is designed to have a fixed set of steps
    that cannot be modified, hence the term "frozen". The specific steps are defined as a class-level constant
    and cannot be customized, only the name of the pipeline can be customized.

    Parameters
    ----------
    *args
        Variable length argument list. Not used in this class, but included for compatibility with parent.
    name : str, optional
        The name of the pipeline. If not provided, it defaults to None.

    Attributes
    ----------
    STEPS : tuple
        A tuple containing the steps of the pipeline. This is a class-level attribute and cannot be modified.
    """

    NAME = "FrozenPipeline"
    STEPS = ()

    @property
    def steps(self):
        return self._steps

    @steps.setter
    def steps(self, value):
        raise AttributeError("Cannot set steps on a FrozenPipeline")

    def __init__(self, name=NAME, **kwargs):
        steps = [get_callable_by_name(name) for name in self.STEPS]
        super().__init__(*steps, name=name, **kwargs)


class DefaultPipeline(FrozenPipeline):
    """
    The DefaultPipeline class is a subclass of the Pipeline class. It is designed to be a general-purpose pipeline
    for data processing. It includes steps for loading data and handling unit conversion. The specific steps are fixed
    and cannot be customized, only the name of the pipeline can be customized.

    Parameters
    ----------
    name : str, optional
        The name of the pipeline. If not provided, it defaults to "pycmor.pipeline.DefaultPipeline".
    """

    # FIXME(PG): This is not so nice. All things should come out of the std_lib,
    #            but it is a good start...
    STEPS = (
        "pycmor.core.gather_inputs.load_mfdataset",
        "pycmor.std_lib.generic.get_variable",
        "pycmor.std_lib.timeaverage.timeavg",
        "pycmor.std_lib.units.handle_unit_conversion",
        "pycmor.std_lib.global_attributes.set_global_attributes",
        "pycmor.std_lib.variable_attributes.set_variable_attributes",
        "pycmor.core.caching.manual_checkpoint",
        "pycmor.std_lib.generic.trigger_compute",
        "pycmor.std_lib.generic.show_data",
        "pycmor.std_lib.files.save_dataset",
    )
    NAME = "pycmor.pipeline.DefaultPipeline"


class TestingPipeline(FrozenPipeline):
    """
    The TestingPipeline class is a subclass of the Pipeline class. It is designed for testing purposes. It includes
    steps for loading data fake data, performing a logic step, and saving data. The specific steps are fixed and
    cannot be customized, only the name of the pipeline can be customized.

    Parameters
    ----------
    name : str, optional
        The name of the pipeline. If not provided, it defaults to "pycmor.pipeline.TestingPipeline".

    Warning
    -------
    An internet connection is required to run this pipeline, as the load_data step fetches data from the internet.
    """

    __test__ = False  # Prevent pytest from thinking this is a test, as the class name starts with test.

    STEPS = (
        "pycmor.std_lib.generic.dummy_load_data",
        "pycmor.std_lib.generic.dummy_logic_step",
        "pycmor.std_lib.generic.dummy_save_data",
    )
    NAME = "pycmor.pipeline.TestingPipeline"
