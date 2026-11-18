AWI-ESM3 LR CMIP7 CMORization
============================

Use the single full workflow in ``full/``. It covers OpenIFS, LPJ-GUESS,
FESOM ocean/sea ice, REcoM, and supported atmospheric CO2 diagnostics.

Installation, external site configuration, validation and reproduction are
in ``full/README.rst``. From this directory::

    ./full/cmorize --config /outside/repo/config.json \
      /path/to/experiment --leg latest --check

``--check`` writes a frozen plan and reports without converting or submitting
jobs. Use the representative-test gate before releasing the full conversion::

    python full/scripts/retest_all_products.py prepare /printed/campaign/path
    python full/scripts/retest_all_products.py submit /printed/campaign/path \
      --full-after-pass

The planner selects a verified completed/postprocessed leg. Missing mappings,
missing inputs and inapplicable requests remain explicitly reported. READY is
eligible to attempt; PASS records only the checks actually performed. Test
metadata and QC findings do not establish publication readiness.

Files that remain necessary
--------------------------

* All 17 ``*/cmip7xml_awiesm3-lr*.yaml`` tiers provide the full catalogue.
* ``profile.yaml`` and ``*/cmip7_awiesm3-lr*.yaml`` provide raw fallback mappings.
* ``full/rules/`` supplies reviewed REcoM, CO2, iceberg and land adapters.
* ``full/scripts/`` supplies planning, execution, reporting and scientific checks.
* ``full/tests/`` provides regressions; run ``bash full/tests/run-tests``.
* Shared ``src/pycmor`` and ``examples`` helpers remain engine dependencies.
  Some HR-named catalogues supply the reviewed rotation-aware pipelines used
  by LR and must remain available.
* The requested-variable CSVs retain the scope and evidence for unsupported
  diagnostics. A model field that was never saved cannot be manufactured.

The retired independent raw-monthly controller, its wrapper scripts, private
example campaign and controller-only tests have been removed. There is one
operational workflow. Keep actual model output, environments, site settings,
metadata downloads and generated results outside this repository.
