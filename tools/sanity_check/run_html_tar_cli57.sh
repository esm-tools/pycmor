#!/bin/bash
#SBATCH --job-name=pycmor-html-tar
#SBATCH --partition=shared
#SBATCH --account=ab0246
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=00:30:00
#SBATCH --output=pycmor_html_tar_%j.log
#SBATCH --error=pycmor_html_tar_%j.log

set -e

source ~/loadconda.sh
conda activate pycmor_py312

cd /work/ab0246/a270092/software/pycmor

python tools/sanity_check/build_html_report.py \
    --jsonl tools/sanity_check/reports/cli57_picontrol_y1851_full.jsonl \
    --out-dir tools/sanity_check/reports/cli57_picontrol_y1851_full_html

cd tools/sanity_check/reports
tar -czf cli57_picontrol_y1851_full_html.tar.gz cli57_picontrol_y1851_full_html
ls -lh cli57_picontrol_y1851_full_html.tar.gz
