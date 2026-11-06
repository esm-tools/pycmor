#!/usr/bin/env python3
"""Generate an operational PDF from external settings; never run conversions."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import sys
import textwrap


FULL = Path(__file__).resolve().parent.parent
REPO = FULL.parent.parent


def page(pdf, plt, title, blocks, number):
    """Render deliberately short pages, refusing silently clipped content."""
    fig = plt.figure(figsize=(8.27, 11.69), facecolor="white")
    fig.text(.08, .94, title, fontsize=18, weight="bold", color="#14394f", va="top")
    y = .892
    for kind, text in blocks:
        if kind == "h":
            y -= .012
            fig.text(.08, y, text, fontsize=12, weight="bold", va="top", color="#14394f")
            y -= .029
            continue
        width = 85 if kind == "code" else 89
        lines = []
        for line in text.splitlines():
            lines.extend(textwrap.wrap(line, width=width, break_long_words=True,
                                       break_on_hyphens=False, replace_whitespace=False) or [""])
        fig.text(.08, y, "\n".join(lines), fontsize=8.8 if kind == "code" else 10.3,
                 family="monospace" if kind == "code" else "sans-serif", va="top",
                 linespacing=1.4, parse_math=False,
                 bbox=dict(facecolor="#f1f5f8", edgecolor="none", pad=6) if kind == "code" else None)
        y -= len(lines) * (.0148 if kind == "code" else .0175) + .015
    if y < .07:
        plt.close(fig)
        raise RuntimeError(f"Guide page {number} overflows; shorten configured paths ({y:.3f})")
    fig.text(.08, .038, "Full-catalog LR workflow | Test products, not publication-approved",
             fontsize=8, color="#52616b")
    fig.text(.92, .038, str(number), fontsize=8, ha="right", color="#52616b")
    pdf.savefig(fig)
    plt.close(fig)


def guide_pages(config, config_path):
    python = shlex.quote(str(config["python"]))
    settings = shlex.quote(str(config_path))
    experiment = str(config.get("experiment", "/absolute/path/to/experiment"))
    repo = Path(config.get("repo", REPO))
    workspace = repo.parent
    local_launcher = (workspace / "cmorize").is_file()
    location = workspace if local_launcher else repo / "awi-esm3-lr-variables"
    command = ('./cmorize' if local_launcher else './full/cmorize') + ' --config "$CMOR_CONFIG" "$EXP"'
    # Keep long experiment paths as valid copyable shell, not soft-wrapped code.
    parts, chunk = [], ""
    for segment in experiment.split("/"):
        token = segment + "/"
        if chunk and len(chunk + token) > 72:
            parts.append(chunk)
            chunk = ""
        chunk += token
    parts.append(chunk.rstrip("/"))
    experiment_assignment = "EXP=" + "\\\n".join(shlex.quote(part) for part in parts)
    return [
        ("CMORization on ALBEDO: run a leg", [
            ("h", "1. Set the experiment and configuration"),
            ("code", f"cd {shlex.quote(str(location))}\n"
                     f"export PYCMOR_PYTHON={python}\n"
                     f"CMOR_CONFIG={settings}\n{experiment_assignment}"),
            ("p", "Replace EXP with an experiment root, not outdata or run_.... "
                  "The workspace launcher forwards to the Git checkout; it is not another "
                  "implementation. No model data or running model jobs are changed."),
            ("h", "2. Check the latest completed leg first"),
            ("code", command + " --leg latest --check"),
            ("p", "Checks compute and merge completion, then writes an inventory. No conversion "
                  "or job submission. Missing streams and rules are reported, not reconstructed. "
                  "If the latest compute leg is not merged, it stops rather than choosing an older leg."),
            ("h", "3. Run the validation gate before a full conversion"),
            ("code", 'LEG=26100101-26141231\n'
                     'FULL=pycmor/awi-esm3-lr-variables/full\n'
                     '"$PYCMOR_PYTHON" "$FULL/scripts/start_verified_leg.py" "$EXP" \\\n  --leg "$LEG" --config "$CMOR_CONFIG"'),
            ("p", "Run step 3 inside a postprocessing Slurm allocation. The example leg was "
                  "verified on 5 November 2026; recheck before a new campaign. This performs a "
                  "fresh source audit, freezes a plan and tests every READY representative. "
                  "A failed audit or test blocks the remaining product-years. "
                  "Ordinary cmorize submission does not recreate this gate."),
        ]),
        ("Configuration, rules and results", [
            ("h", "4. One source tree, local settings outside Git"),
            ("code", str(workspace) + "/\n"
                     "  cmorize                       forwarding launcher\n"
                     "  config/full_cmor_config.json  local settings\n"
                     "  pycmor/                       Git repository\n"
                     "    awi-esm3-lr-variables/full/\n"
                     "      scripts/                  workflow and tests helpers\n"
                     "      rules/recom.yaml          extra REcoM mappings\n"
                     "      rules/co2.yaml            extra CO2 mappings\n"
                     "      tests/                    regression tests"),
            ("p", "The full CMIP7 catalog also uses the existing tracked rules in "
                  "awi-esm3-veg-hr-variables and the LR raw-monthly catalog. Edit reusable code/rules "
                  "inside pycmor, not in a separate workspace scripts/ or rules/ directory."),
            ("p", "The JSON sets results, metadata/CVs, mesh/forcing paths, Python and Slurm "
                  "resources. Optional experiment and leg keys provide defaults; CLI arguments "
                  "override them. Another experiment needs its metadata and carbon conventions "
                  "reviewed, especially if an external emissions reader is enabled."),
            ("h", "5. Find products and diagnose missing variables"),
            ("code", str(config["results"]) + "/\n"
                     "  <experiment>__<path-hash>/<leg>/\n"
                     "    NOT_FOR_PUBLICATION/        products by attempt\n"
                     "    recipes/                    generated conversion rules\n"
                     "    report/products.csv         every product-year status\n"
                     "    report/variable_inventory.csv\n"
                     "    report/summary.json         overall status\n"
                     "    reproduce/                  code/settings/dependencies\n"
                     "    logs/                       scheduler output\n"
                     "    submission.json             submitted job IDs"),
            ("p", "Check report/products.csv for each product-year and its reason if blocked "
                  "or failed. Planner logs are in results/submission_logs. A job ID is not proof "
                  "that conversion finished; inspect the product report and scheduler exit status."),
        ]),
        ("Repeat runs and share with colleagues", [
            ("h", "6. Whole experiment: currently one leg at a time"),
            ("p", "There is no --all option. Repeat the explicit-leg command for the completed, "
                  "merged periods you need; do not infer completion from directory names. Each "
                  "leg gets its own results directory. Independent submissions may run together; "
                  "the configured concurrency limit applies per campaign, not across all legs."),
            ("h", "7. Changed code or settings require a fresh plan"),
            ("code", command + ' --leg "$LEG" \\\n  --output-root "' + str(config["results"]) + '/reviewed_rerun" --check'),
            ("p", "Choose a new output root for a changed implementation/configuration. Do not "
                  "edit code while its workers run: source fingerprints deliberately reject that. "
                  "Unchanged campaigns can reuse their frozen plan. allow_dirty:true is for "
                  "reviewed development only; normal shared use should use a committed checkout."),
            ("h", "8. Future per-leg integration"),
            ("p", "Submit start_verified_leg.py with the exact leg and Slurm dependency "
                  "afterok:MERGE_JOB_ID. No automatic model hook "
                  "is installed by this workflow. Verify generated output XML before expecting "
                  "new variables: existing prepared legs may still have reduced output."),
            ("h", "9. Share the Git code, not the local environment"),
            ("p", "A colleague needs the commit containing awi-esm3-lr-variables/full, an "
                  "installed environment and their own external config. Uncommitted/unpushed "
                  "changes are not obtained by git pull. Installation instructions and an example "
                  "config are in full/README.rst and full/config/example.json."),
            ("code", "cd /path/to/pycmor/awi-esm3-lr-variables\n"
                     "export PYCMOR_PYTHON=/path/to/venv/bin/python\n"
                     "./full/cmorize --config /path/to/config.json \\\n  /path/to/experiment --leg latest --check"),
        ]),
        ("Validation: what success means", [
            ("h", "Test without submitting model jobs"),
            ("code", f'cd {shlex.quote(str(repo / "awi-esm3-lr-variables"))}\n'
                     'bash full/tests/run-tests\n'
                     '"$PYCMOR_PYTHON" full/scripts/retest_all_products.py --help'),
            ("p", "The regression suite submits no jobs. A smoke test converts a small selected "
                  "set using real output in Slurm. Representative retesting selects one READY "
                  "year per distinct product from a frozen plan. Neither is an all-years audit."),
            ("h", "Read status precisely"),
            ("p", "Product-years, distinct compounds, variable names and files are different counts. "
                  "READY means suitable for an attempted conversion, not already validated. "
                  "BLOCKED and FAILED are not success. PASS covers the stated conversion/data "
                  "checks; formal CF/WCRP/AICC findings and scientific validation remain separate."),
            ("p", "Monthly means cannot provide daily/subdaily diagnostics, and a five-year leg "
                  "alone cannot supply a decadal product; a verified ten-year archive window can. Additional diagnostics may need a newer "
                  "model build. Read the inventory for missing rules and inputs; requesting the "
                  "full catalog does not mean every requested product is convertible."),
            ("h", "Current scientific limits"),
            ("p", "The shared raw-monthly mappings provide fallback recipes within the full "
                  "workflow. The full scope includes REcoM and atmospheric CO2. "
                  "Carbon mappings require an explicitly reviewed profile and source conventions. "
                  "Official metadata/CVs remain external; no official tables are created. "
                  "Do not publish test products or delete raw data based on conversion success."),
        ]),
    ]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True, help="External full-workflow JSON settings")
    parser.add_argument("--output", type=Path, required=True, help="New PDF outside the source checkout")
    parser.add_argument("--experiment", type=Path, help="Experiment path for the guide examples only; does not change settings")
    args = parser.parse_args(argv)
    config_path = args.config.expanduser().resolve(strict=True)
    config = json.loads(config_path.read_text())
    config.setdefault("python", sys.executable)
    config.setdefault("repo", str(REPO))
    if args.experiment:
        config["experiment"] = str(args.experiment)
    for key in ("python", "results"):
        if not isinstance(config.get(key), str) or not config[key]:
            parser.error(f"Configuration requires a nonempty {key!r} path")
    for key in ("python", "results", "experiment", "repo"):
        if config.get(key):
            value = Path(os.path.expandvars(config[key])).expanduser()
            # Resolving a venv executable symlink would select the system Python.
            config[key] = (os.path.abspath(config_path.parent / value) if key == "python"
                           else str((config_path.parent / value).resolve()))
    output = args.output.expanduser().resolve()
    if output == REPO or REPO in output.parents:
        parser.error("Write the generated PDF outside the source checkout")
    if output.suffix.lower() != ".pdf":
        parser.error("--output must name a .pdf file")
    if output.exists():
        parser.error("Output already exists; choose a new file")
    # Late imports keep --help usable before the optional reporting dependency is installed.
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    output.parent.mkdir(parents=True, exist_ok=True)
    with PdfPages(output) as pdf:
        for number, (title, blocks) in enumerate(guide_pages(config, config_path), 1):
            page(pdf, plt, title, blocks, number)
    print(output)


if __name__ == "__main__":
    main()
