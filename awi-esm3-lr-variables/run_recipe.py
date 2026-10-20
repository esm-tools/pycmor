"""Run a pinned recipe and record inputs, exit status and actual generated files."""
import hashlib
import copy
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import yaml

ROOT=Path(os.environ.get('PYCMOR_LR_ROOT',Path(__file__).resolve().parents[2]))
REPO=Path(__file__).resolve().parents[1]

def sha(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for block in iter(lambda:f.read(8*1024**2),b''):h.update(block)
    return h.hexdigest()

def main():
    (ROOT/'provenance').mkdir(parents=True,exist_ok=True)
    (ROOT/'inventory').mkdir(parents=True,exist_ok=True)
    recipe=Path(sys.argv[1]).resolve();cfg=yaml.safe_load(recipe.read_text())
    out=Path(cfg['inherit']['output_directory'])
    if any(out.rglob('*.nc')):
        raise RuntimeError(f'Refusing to overwrite existing results: {out}; use a new attempt directory')
    label=recipe.stem;run=ROOT/'provenance'/f'{label}_{os.environ.get("SLURM_JOB_ID",str(int(time.time())))}'
    run.mkdir(parents=True,exist_ok=False)
    # Retain the actual dirty-tree implementation as well as the base revision.
    snapshot=run/'implementation';snapshot.mkdir()
    shutil.copytree(REPO/'src/pycmor',snapshot/'src/pycmor',ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
    paths=[REPO/'src/pycmor/std_lib/files.py',REPO/'src/pycmor/awiesm3_lr.py']
    paths+=list(Path(__file__).parent.glob('*.py'))+list(Path(__file__).parent.glob('*.sh'))
    for path in paths:
        dest=snapshot/path.relative_to(REPO);dest.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(path,dest)
    git={key:subprocess.check_output(['git','-C',str(REPO),*args],text=True) for key,args in
         [('commit',['rev-parse','HEAD']),('branch',['branch','--show-current']),('status',['status','--short','--untracked-files=all']),('diff',['diff','--binary'])]} if (REPO/'.git').exists() else {'pinned_code':str(REPO)}
    (run/'git.json').write_text(json.dumps(git,indent=2))
    (run/'implementation.sha256.json').write_text(json.dumps({str(p.relative_to(REPO)):sha(p) for p in paths},indent=2))
    inputs=set()
    for rule in cfg['rules']:
        inputs.update(Path(p) for p in rule.get('lr_source_files',[]))
        for inp in rule.get('inputs',[]):
            base=Path(inp['path']);pattern=inp['pattern']
            inputs.update(p for p in base.iterdir() if p.is_file() and (p.name==pattern or re.fullmatch(pattern,p.name)))
        for term in rule.get('lr_terms',[]):
            if term.get('files'):
                inputs.update(Path(p) for p in term['files'])
                continue
            for year in range(cfg['inherit']['year_start'],cfg['inherit']['year_end']+1):
                inputs.add(Path(rule['data_path'])/term['file'].format(year=year))
        if rule.get('grid_file'):inputs.add(Path(rule['grid_file']))
    before={str(p):sha(p) for p in sorted(inputs)}
    (run/'recipe.yaml').write_text(recipe.read_text())
    (run/'inputs.sha256.json').write_text(json.dumps(before,indent=2))
    executable=os.environ.get('PYCMOR_LR_EXECUTABLE',str(ROOT/'venv/bin/pycmor'))
    command=[executable,'process',str(run/'recipe.yaml')]
    print('Running',command,flush=True)
    start=time.time()
    with (run/'pycmor.log').open('w') as log:
        env=dict(os.environ,PYTHONPATH=str(snapshot/'src'))
        result=subprocess.run(command,cwd=run,env=env,stdout=log,stderr=subprocess.STDOUT)
    attempts=[{'command':command,'exit_code':result.returncode,'log':str(run/'pycmor.log')}]
    startup_timeout='Timed out while attempting to connect to ephemeral Prefect API server'
    # One bounded infrastructure-only fallback. Native execution uses exactly
    # the same scientific steps and checks, sequentially within this Slurm task.
    # Never retry over existing NetCDF files or mask a scientific rule failure.
    if result.returncode and not any(out.rglob('*.nc')) and startup_timeout in ' '.join((run/'pycmor.log').read_text().split()):
        native=copy.deepcopy(cfg)
        native['pycmor'].update(parallel=False,pipeline_workflow_orchestrator='native')
        for pipeline in native['pipelines']:pipeline['workflow_backend']='native'
        native_path=run/'recipe.native.yaml'
        native_path.write_text(yaml.safe_dump(native,sort_keys=False))
        command=[executable,'process',str(native_path)]
        print('Prefect startup failed before any output; retrying once with the native pipeline backend',flush=True)
        with (run/'pycmor.native.log').open('w') as log:
            result=subprocess.run(command,cwd=run,env=env,stdout=log,stderr=subprocess.STDOUT)
        attempts.append({'command':command,'exit_code':result.returncode,'log':str(run/'pycmor.native.log')})
    unchanged=all(sha(Path(p))==digest for p,digest in before.items())
    out=Path(cfg['inherit']['output_directory'])
    status={'command':command,'attempts':attempts,'recipe':str(recipe),'pycmor_exit_code':result.returncode,'sources_unchanged':unchanged,
            'elapsed_seconds':time.time()-start,'expected_rules':[r['name'] for r in cfg['rules']],
            'files':[str(p) for p in sorted(out.rglob('*.nc'))], 'publication_ready':False}
    if status['files']:
        validator=snapshot/Path(__file__).parent.relative_to(REPO)/'validate_outputs.py'
        with (run/'validation.log').open('w') as log:
            validation=subprocess.run([sys.executable,str(validator),str(recipe),'--snapshot',str(run/'recipe.yaml')],
                                      env=dict(os.environ,PYCMOR_LR_ROOT=str(ROOT)),stdout=log,stderr=subprocess.STDOUT)
        status['independent_validation_exit_code']=validation.returncode
        report_path=ROOT/'inventory'/f'validation_{recipe.stem}.json'
        if report_path.is_file():
            report=json.loads(report_path.read_text())
            shutil.copyfile(report_path,run/'validation.json')
            if report.get('recipe')!=str(recipe):
                raise RuntimeError('Validation report belongs to another recipe')
            accepted={p['output'] for p in report['products'] if p['status']=='PASS'} if unchanged else set()
            moved={}
            for filename in status['files']:
                if filename in accepted:continue
                path=Path(filename)
                rejected=ROOT/'rejected_outputs'/run.name/path.relative_to(out)
                rejected.parent.mkdir(parents=True,exist_ok=True)
                if rejected.exists():raise FileExistsError(rejected)
                shutil.move(str(path),str(rejected));moved[filename]=str(rejected)
            status['rejected_files']=moved
            status['files']=[p for p in status['files'] if p not in moved]
    (run/'status.json').write_text(json.dumps(status,indent=2));print(json.dumps(status,indent=2),flush=True)
    sys.exit(result.returncode or int(not unchanged) or int(not status['files']) or status.get('independent_validation_exit_code',0))

if __name__=='__main__':main()
