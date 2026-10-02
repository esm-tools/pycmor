"""
Pipeline of the data processing steps.
"""

import copy
import os
import time
from datetime import timedelta

import randomname
from prefect.cache_policies import INPUTS, NO_CACHE, TASK_SOURCE
from prefect.tasks import Task

from .skip import RuleSkipped
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
        collapse_steps=None,
        throttle_group=None,
    ):
        self._steps = args
        self.name = name or randomname.get_name()
        self._cluster = dask_cluster
        self._prefect_cache_kwargs = {}
        self._steps_are_prefectized = False
        if workflow_backend is None:
            workflow_backend = "prefect"
        self._workflow_backend = workflow_backend
        # Throttle group: pipelines sharing the same key compete for a
        # bounded slot count in ``cmorizer._parallel_process_prefect``.
        # Used to cap driver-process concurrency for memory-heavy rule
        # families (lrcs_seaice's OIFS-regrid family explodes driver RSS
        # past 80 GiB when 4 run concurrently — see
        # FORENSIC_lrcs_seaice_failure.md §"Why ONLY lrcs_seaice"). Default
        # None means unthrottled; caps live in
        # ``PYCMOR_THROTTLE_CAPS=group:N,...`` env or rule yaml inherit
        # ``throttle_caps:`` map. Default per-group cap (when not
        # configured) is 2 — small enough to prevent driver pileup, big
        # enough to keep some throughput.
        self.throttle_group = throttle_group
        # Round-2 perf knob: if set, collapse all pipeline steps into a
        # single Prefect task. Trades per-step task caching for ~13×
        # less Prefect orchestration overhead per rule (Prefect 3.x:
        # ~2.4 s/task scheduler latency × 13 steps × N rules adds up).
        # Default off; can be set per-pipeline via yaml ``collapse_steps``
        # or globally via env var ``PYCMOR_PREFECT_COLLAPSE=1``.
        if collapse_steps is None:
            collapse_steps = os.environ.get("PYCMOR_PREFECT_COLLAPSE", "1") in ("1", "true", "True", "yes")
        self._collapse_steps = bool(collapse_steps)
        if cache_policy is None:
            # ``script://`` step loader creates a synthetic module called
            # "script" (see core.utils.get_function_from_script). Anything
            # the script defines, including ``@functools.lru_cache``
            # wrappers used as helpers, carries ``__module__ == "script"``.
            # Prefect's INPUTS cache policy pickles every task argument
            # (the Rule + bound steps), and pickle fails to serialise
            # those wrappers because "script" isn't a re-importable module
            # name. Detect script-loaded steps and downgrade to NO_CACHE
            # for the whole pipeline; INPUTS hashing is the only thing
            # that touches the wrapper, and disabling it costs nothing
            # because pycmor runs each pipeline once per process.
            _has_script_step = any(getattr(s, "__module__", "") == "script" for s in self._steps)
            if _has_script_step:
                self._cache_policy = NO_CACHE
            else:
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

        if self._collapse_steps and self._steps:
            # Collapse all pipeline steps into a single Prefect task to
            # eliminate per-step orchestration overhead. Step bodies still
            # execute in order; only the Task wrapping is consolidated.
            #
            # Some steps (e.g. ``pycmor.core.caching.manual_checkpoint``)
            # return a Prefect ``State`` object when the workflow backend
            # is "prefect", relying on the per-step Prefect Task chain
            # to unwrap it. With all steps in one Task, we have to do
            # the unwrapping ourselves.
            steps_to_run = list(self._steps)

            def _run_collapsed_pipeline(data, rule_spec):
                from prefect.states import State

                for step in steps_to_run:
                    result = step(data, rule_spec)
                    if isinstance(result, State):
                        try:
                            result = result.result(raise_on_failure=True)
                        except Exception:
                            # Step intentionally returned a state without a
                            # data payload; pass the prior data through.
                            result = data
                    data = result
                    if isinstance(data, RuleSkipped):
                        return data
                return data

            _run_collapsed_pipeline.__name__ = f"{self.name}_collapsed"
            logger.debug(
                f"Collapsing {len(self._steps)} steps into one Prefect task " f"({_run_collapsed_pipeline.__name__})."
            )
            prefect_tasks = [
                Task(
                    fn=_run_collapsed_pipeline,
                    **self._prefect_cache_kwargs,
                )
            ]
        else:
            prefect_tasks = []
            for i, step in enumerate(self._steps):
                logger.debug(f"[{i+1}/{len(self._steps)}] Converting step {step.__name__} to Prefect task.")
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
            if isinstance(data, RuleSkipped):
                return data
        return data

    def _run_prefect(self, data, rule_spec):
        # Run the pipeline's prefectised steps synchronously in the calling
        # thread. Earlier versions wrapped this in a per-rule ``@flow`` whose
        # ``DaskTaskRunner`` shared the parent task's pool; that nested
        # submission caused a parent×child resource-allocation deadlock at
        # production scale. See DESIGN_PROPOSAL_subflow_deadlock.md §3-§4.
        cmor_name = rule_spec.get("cmor_name")
        rule_name = rule_spec.get("name", cmor_name)
        logger.info(f"Pipeline '{self.name}' running for rule '{rule_name}'")
        t0 = time.monotonic()
        try:
            result = self._run_native(data, rule_spec)
        except BaseException as exc:
            elapsed = time.monotonic() - t0
            try:
                self.on_failure_native(
                    rule_name=rule_name,
                    pipeline_name=self.name,
                    elapsed_s=elapsed,
                    exception=exc,
                )
            except Exception as cb_exc:
                logger.warning(f"on_failure_native callback raised: {cb_exc}")
            raise
        elapsed = time.monotonic() - t0
        try:
            self.on_completion_native(
                rule_name=rule_name,
                pipeline_name=self.name,
                elapsed_s=elapsed,
            )
        except Exception as cb_exc:
            logger.warning(f"on_completion_native callback raised: {cb_exc}")
        return result

    @staticmethod
    @add_to_report_log
    def on_completion_native(rule_name, pipeline_name, elapsed_s):
        logger.success(f"Pipeline '{pipeline_name}' completed for rule " f"'{rule_name}' in {elapsed_s:.1f}s")

    @staticmethod
    @add_to_report_log
    def on_failure_native(rule_name, pipeline_name, elapsed_s, exception):
        logger.error(
            f"Pipeline '{pipeline_name}' FAILED for rule '{rule_name}' "
            f"after {elapsed_s:.1f}s: "
            f"{type(exception).__name__}: {exception}"
        )

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
        return cls.from_list([get_callable_by_name(name) for name in qualnames], name=name, **kwargs)

    @classmethod
    def from_callable_strings(cls, step_strings: list, name=None, **kwargs):
        return cls.from_list([get_callable(name) for name in step_strings], name=name, **kwargs)

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
                collapse_steps=data.get("collapse_steps"),
                throttle_group=data.get("throttle_group"),
            )
        if "steps" in data:
            return cls.from_callable_strings(
                data["steps"],
                name=data.get("name"),
                cache_expiration=data.get("cache_expiration"),
                workflow_backend=data.get("workflow_backend"),
                collapse_steps=data.get("collapse_steps"),
                throttle_group=data.get("throttle_group"),
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


class DefaultOpenDataPipeline(FrozenPipeline):
    """
    Pipeline for opening and loading data.

    This pipeline handles the initial data loading step.

    Parameters
    ----------
    name : str, optional
        The name of the pipeline.
    """

    STEPS = (
        "pycmor.core.gather_inputs.load_mfdataset",
        "pycmor.std_lib.generic.get_variable",
    )
    NAME = "pycmor.pipeline.DefaultOpenDataPipeline"


class DefaultCorePipeline(FrozenPipeline):
    """
    Core processing pipeline without I/O operations.

    This pipeline handles all data transformations and processing but does not
    load or save data. Useful for testing and when working with data already
    in memory.

    Parameters
    ----------
    name : str, optional
        The name of the pipeline.
    """

    STEPS = (
        "pycmor.std_lib.add_vertical_bounds",
        "pycmor.std_lib.timeaverage.timeavg",
        "pycmor.std_lib.units.handle_unit_conversion",
        "pycmor.std_lib.global_attributes.set_global_attributes",
        "pycmor.std_lib.variable_attributes.set_variable_attributes",
    )
    NAME = "pycmor.pipeline.DefaultCorePipeline"


class DefaultSaveDataPipeline(FrozenPipeline):
    """
    Pipeline for finalizing and saving processed data.

    This pipeline handles caching, computation triggering, and file output.

    Parameters
    ----------
    name : str, optional
        The name of the pipeline.
    """

    STEPS = (
        "pycmor.core.caching.manual_checkpoint",
        "pycmor.std_lib.generic.trigger_compute",
        "pycmor.std_lib.generic.show_data",
        "pycmor.std_lib.files.save_dataset",
    )
    NAME = "pycmor.pipeline.DefaultSaveDataPipeline"


class DefaultPipeline(FrozenPipeline):
    """
    Complete default pipeline combining open, process, and save operations.

    This pipeline includes steps for loading data, adding vertical bounds, handling unit conversion,
    and setting CMIP-compliant attributes, then saving the output. The specific steps are fixed
    and cannot be customized, only the name of the pipeline can be customized.

    This combines: DefaultOpenDataPipeline + DefaultCorePipeline + DefaultSaveDataPipeline

    Parameters
    ----------
    name : str, optional
        The name of the pipeline. If not provided, it defaults to "pycmor.pipeline.DefaultPipeline".

    Notes
    -----
    The pipeline includes automatic vertical bounds calculation for datasets with vertical coordinates
    (pressure levels, depth, height), ensuring CMIP compliance.
    """

    # FIXME(PG): This is not so nice. All things should come out of the std_lib,
    #            but it is a good start...
    STEPS = (
        "pycmor.core.gather_inputs.load_mfdataset",
        "pycmor.std_lib.generic.get_variable",
        "pycmor.std_lib.add_vertical_bounds",
        "pycmor.std_lib.timeaverage.timeavg",
        # CF/CMIP and wcrp TIME001 require time == midpoint(time_bnds). The
        # step is a no-op when bnds already exist; otherwise it builds the
        # canonical month-start bounds and realigns the time coord. Uses
        # the std_lib wrapper so DataArray-typed pipeline payloads still
        # round-trip through the Dataset-only inner function.
        "pycmor.std_lib.set_time_bounds",
        "pycmor.std_lib.units.handle_unit_conversion",
        "pycmor.std_lib.attributes.set_global",
        "pycmor.std_lib.attributes.set_variable",
        "pycmor.std_lib.attributes.set_coordinates",
        "pycmor.std_lib.dimensions.map_dimensions",
        "pycmor.core.caching.manual_checkpoint",
        "pycmor.std_lib.generic.trigger_compute",
        "pycmor.std_lib.generic.show_data",
        "pycmor.std_lib.files.save_dataset",
        # Opt-in per-shard QC. No-op unless ``qc_enabled: true`` is set
        # on the rule (or in ``inherit:``); see pycmor.std_lib.qc.
        "pycmor.std_lib.qc.run_compliance_checker",
    )
    NAME = "pycmor.pipeline.DefaultPipeline"


class AreacelloFxPipeline(FrozenPipeline):
    """Fixed pipeline producing ``areacello`` from an unstructured ocean mesh.

    Reads ``rule.grid_file`` for ``cell_area`` and writes a CMIP7 fx
    file. Configs need only set ``compound_name``, ``model_variable``,
    and ``inputs`` (the mesh file), then reference this pipeline via
    ``uses: pycmor.pipeline.AreacelloFxPipeline``.
    """

    STEPS = (
        "pycmor.std_lib.cell_measures.load_gridfile",
        "pycmor.std_lib.cell_measures.compute_areacello",
        "pycmor.std_lib.attributes.set_global",
        "pycmor.std_lib.attributes.set_variable",
        "pycmor.std_lib.attributes.set_coordinates",
        "pycmor.std_lib.dimensions.map_dimensions",
        "pycmor.std_lib.files.save_dataset",
        "pycmor.std_lib.qc.run_compliance_checker",
    )
    NAME = "pycmor.pipeline.AreacelloFxPipeline"


class AreacellaFxPipeline(FrozenPipeline):
    """Fixed pipeline producing ``areacella`` from lat/lon on a regular grid.

    Loads any model output file, picks a field, and applies the
    spherical-Earth cell-area formula on the field's lat/lon coords.
    Reference via ``uses: pycmor.core.pipeline.AreacellaFxPipeline``.
    """

    STEPS = (
        "pycmor.core.gather_inputs.load_mfdataset",
        "pycmor.std_lib.cell_measures.compute_areacella",
        "pycmor.std_lib.attributes.set_global",
        "pycmor.std_lib.attributes.set_variable",
        "pycmor.std_lib.attributes.set_coordinates",
        "pycmor.std_lib.dimensions.map_dimensions",
        "pycmor.std_lib.files.save_dataset",
        "pycmor.std_lib.qc.run_compliance_checker",
    )
    NAME = "pycmor.pipeline.AreacellaFxPipeline"


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
