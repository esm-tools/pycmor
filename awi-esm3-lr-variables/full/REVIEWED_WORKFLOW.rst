Reviewed LR products and request coverage
========================================

An official experiment selection is independent of the converter catalogue.
Export it with CMIP7-data-request-api and set ``data_request_file`` to the
external JSON file, with ``data_request_experiment`` (for example piControl).
The driver inventories every selected compound product and writes
``report/data_request_coverage.csv`` and ``.json``. A product is WRITTEN only
after every required task passes. An explicitly permitted regional duplicate
is covered only after its exact global equivalent has been written. Such
coverage is not an additional output file or a scientific approval.

The optional ``scientific_profile: awiesm3-v3.5.1-cc-4e03ffb1`` applies the
run-specific review in ``config/scientific_review.json``. It requires the
reviewed FESOM binary hash. It corrects verified snow, ice and Boussinesq mass
factors, overturning mass conversion and the ice-drag domain. Daily/monthly
wind speed is calculated from the native hourly vectors before averaging;
daily/monthly relative humidity is calculated from hourly temperature and
dewpoint before temporal averaging, rather than from daily/monthly means;
sub-hour variability remains an approximation. Confirmed invalid mappings
without a verified replacement are BLOCKED with their reason. This includes
convection from ice cover, groundwater from deep unsaturated soil, lateral
viscosity from vertical viscosity, and unverified pressure, salt-flux,
volume, column-integral and root-depth proxies. The complete matrix retains
other unresolved review/approximation findings. Products outside this matrix
have not thereby been certified.

All full-workflow recipes preserve small native positive and signed values.
The dewpoint humidity calculation uses water saturation in the numerator,
as required by the OpenIFS dewpoint definition, and retains ice
supersaturation. Annual reductions of consecutive monthly means use month
lengths, including leap years. These repairs do not recover variability
already lost through source averaging.

Seven LPJG products use native ``Year``/``Day`` records: daily total and
liquid soil water, soil temperature, leaf area index and potential
evapotranspiration, plus daily/monthly frozen soil water. They
validate Gregorian days, unique and complete cell/day coverage, the actual
monthly modeled-cell footprint, text-precision coordinate agreement and
the fifteen 0.1 m soil layers. They do not reuse the corrupted dates in
older remapped soil files. Layer labels in the native tables are bottoms;
the CMOR axis uses centers and explicit layer bounds.

Frozen soil water is the difference between total and liquid soil water.
The reviewed LPJG ``CMIPoutput.cpp`` includes below-wilting-point liquid
water in both totals, so that difference isolates ice. Both native streams
must have identical complete cell/day/layer coordinates. Monthly means
include every original daily record. Potential evaporation is already
written as kg m-2 s-1 by the model; do not divide it by 86400 again.
These land diagnostics describe LPJG's 1.5 m soil system; they cannot be
combined into a vertically closed budget with OpenIFS's 2.89 m soil system.
The received OpenIFS soil-water drivers are retained separately. Layered
temperature output uses LPJG's own ``T_soil`` state, but the checked source
can still use received OpenIFS 25 cm temperature for the respiration
temperature response when ``useifssoiltemp`` is enabled. Do not infer that
every received soil-temperature field is unused just because the layered
soil temperature is calculated internally.

``hfgeou`` is disabled without an explicit ``geothermal_context`` containing
``kind: zero_bottom_geothermal_heat_flux``, ``binary_sha256`` and an
``evidence_file``. The evidence must document the model-applied zero bottom
boundary for that exact archived binary and retain hashes of reviewed source
copies. An absence of an output variable alone does not establish zero. The
archived review has a provenance limitation: the source revision comes from
a contemporaneous audit rather than an embedded compiler manifest.

For cleaned-up completed legs, ``archived_contexts`` maps a leg name to an
external context JSON. The context retains the original plan and hash,
experiment, period, original identity, and named geometry/configuration
files with hashes. A changed file or different leg is rejected. Do not
substitute geometry from a newer run.

The local checker launcher narrowly adapts multidimensional CF region
labels for compliance-checker versions that otherwise crash on them.
Every decoded label still reaches the original vocabulary check. Checker
exceptions fail execution; ordinary CF/WCRP/AICC findings remain visible.
The LR source registration and documented CF/CMIP table conflicts remain
separate from successful conversion.

Field capacity requires an external ``field_capacity_context`` for the exact
archived leg. It binds an unchanged model-source copy and an actual run-log
excerpt (verified against its original log). Only the verified ``LEVGEN=T``
and four-soil-layer configuration is accepted. The converter reconstructs
the Van Genuchten field-capacity values and layer thicknesses from that
source. It rejects interpolated or unknown categorical soil types; it does
not round them into a guessed soil class. Context and source/excerpt hashes
are retained, and the original log is tracked as an unchanged input.

Use a separate result root for validation and freeze the source before
submitting jobs. The existing completed campaign should remain available
until any replacement has passed its conversion and source-value gates.
