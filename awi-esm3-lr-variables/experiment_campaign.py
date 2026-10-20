"""Restartable, configuration-driven LR campaigns; archive legs are not jobs."""
import argparse
import contextlib
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

import yaml

from discovery import check_profile,digest,load_config,plan,signature,source_paths,SourceIndex

HERE=Path(__file__).resolve().parent
REPO=HERE.parent


def atomic_json(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w',dir=path.parent,prefix=path.name+'.',delete=False) as f:
        json.dump(value,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
        temporary=f.name
    os.replace(temporary,path)


def read_json(path,default=None):
    return json.loads(Path(path).read_text()) if Path(path).exists() else default


@contextlib.contextmanager
def lock(path):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a') as f:
        try:
            fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(f'Another campaign process holds {path}') from None
        yield


def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(8*1024**2),b''):
            h.update(block)
    return h.hexdigest()


def profile():
    result=yaml.safe_load((HERE/'profile.yaml').read_text())
    groups={key:[] for key in result.pop('job_groups')}
    seen=set()
    for filename in result['tier_files']:
        tier=yaml.safe_load((HERE/filename).read_text())
        for original in tier['rules']:
            rule=copy.deepcopy(original)
            group=rule.pop('lr_group')
            identity=(rule['compound_name'],rule.get('grid_label',result['defaults']['grid_label']))
            if identity in seen:
                raise ValueError(f'Duplicate product in LR tiers: {identity}')
            seen.add(identity)
            groups[group].append(rule)
    result['groups']=groups
    return result


def code_hashes(root):
    paths=[p for p in (root/'src/pycmor').rglob('*') if p.is_file() and '__pycache__' not in p.parts and p.suffix!='.pyc']
    paths+=list((root/'awi-esm3-lr-variables').glob('*.py'))
    paths+=list((root/'awi-esm3-lr-variables').glob('*.sh'))
    base=root/'awi-esm3-lr-variables'
    paths+=[base/'profile.yaml']
    definition=yaml.safe_load((base/'profile.yaml').read_text())
    paths += [base/p for p in definition['tier_files']]
    return {str(p.relative_to(root)):sha(p) for p in sorted(paths)}


def setup(cfg):
    state=Path(cfg['state_dir'])
    state.mkdir(parents=True,exist_ok=True)
    pin=state/'pin.json'
    with lock(state/'setup.lock'):
        if pin.exists():
            verify(cfg)
            return
        if (state/'code').exists():
            raise RuntimeError('Incomplete setup: code directory exists without pin.json; inspect before retrying')
        check_profile(cfg,profile())
        code=state/'code'
        shutil.copytree(REPO/'src/pycmor',code/'src/pycmor',
                        ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
        dest=code/HERE.name;dest.mkdir(parents=True)
        for path in HERE.iterdir():
            if path.is_file() and path.suffix in ['.py','.sh','.json']:
                shutil.copy2(path,dest/path.name)
        shutil.copy2(HERE/'profile.yaml',dest/'profile.yaml')
        for filename in profile()['tier_files']:
            target=dest/filename;target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copy2(HERE/filename,target)
        resource_hashes={p:sha(p) for p in [*cfg['grids'].values(),cfg['metadata']['dreq']]}
        # CV files and the environment are retained as provenance too.
        resource_hashes.update({str(p):sha(p) for p in Path(cfg['metadata']['cv']).rglob('*.json')})
        (state/'campaign.yaml').write_text(yaml.safe_dump(cfg,sort_keys=False))
        freeze=subprocess.check_output([str(Path(cfg['venv'])/'bin/python'),'-m','pip','freeze'],text=True)
        (state/'environment.txt').write_text(freeze)
        for directory in ['logs','tasks','submissions','provenance','inventory','attempts','done']:
            (state/directory).mkdir(exist_ok=True)
        atomic_json(pin,{'config_digest':digest(cfg),'code':code_hashes(code),
                         'resources':resource_hashes,'created':time.strftime('%Y-%m-%dT%H:%M:%S%z')})


def verify(cfg):
    state=Path(cfg['state_dir']);pin=read_json(state/'pin.json')
    if not pin or pin['config_digest']!=digest(cfg):
        raise RuntimeError('Campaign settings differ from the pinned configuration; use a new state/output location')
    if code_hashes(state/'code')!=pin['code']:
        raise RuntimeError('Pinned campaign code changed')
    for path,wanted in pin['resources'].items():
        if sha(path)!=wanted:
            raise RuntimeError(f'Pinned grid/metadata changed: {path}')


def discover(cfg):
    prof=profile();check_profile(cfg,prof)
    state=Path(cfg['state_dir'])
    cached=read_json(state/'inventory/discovery_cache.json',{})
    index=SourceIndex(cfg,cached)
    result=plan(cfg,prof,index)
    atomic_json(state/'inventory/discovery_cache.json',index.cache)
    atomic_json(state/'inventory/coverage.json',{k:v for k,v in result.items() if k!='tasks'})
    return result


def file_records(paths):
    return {str(p):signature(p) for p in paths}


def done_ok(cfg,task):
    state=Path(cfg['state_dir'])
    record=read_json(state/'done'/f'{task}.json')
    if record is None:
        return False
    for key in ['sources','files']:
        for path,expected in record[key].items():
            if not Path(path).exists() or signature(path)!=expected:
                raise RuntimeError(f'Previously validated {task} changed or disappeared: {path}')
    actual=set(map(str,Path(record['output_directory']).rglob('*.nc')))
    if actual!=set(record['files']):
        raise RuntimeError(f'Unexpected files in completed task {task}')
    return True


def prepare(cfg):
    result=discover(cfg);state=Path(cfg['state_dir'])
    for item in result['tasks']:
        from pycmor.core.validate import GENERAL_VALIDATOR,PIPELINES_VALIDATOR,RULES_VALIDATOR
        recipe=item['recipe']
        expanded=[dict(recipe['inherit'],**r) for r in recipe['rules']]
        for name,validator,value in [('general',GENERAL_VALIDATOR,recipe['general']),
                                     ('pipelines',PIPELINES_VALIDATOR,recipe['pipelines']),
                                     ('rules',RULES_VALIDATOR,expanded)]:
            if not validator.validate({name:value}):
                raise ValueError(f'{item["id"]}: invalid {name}: {validator.errors}')
        target=state/'tasks'/f'{item["id"]}.json'
        item['sources']=file_records(source_paths(item['recipe']))
        existing=read_json(target)
        if existing and existing!=item:
            raise RuntimeError(f'Frozen recipe/input changed: {target}. Start a new campaign; no outputs overwritten.')
        if not existing:
            atomic_json(target,item)
    atomic_json(state/'inventory/plan.json',
                {**{k:v for k,v in result.items() if k!='tasks'},'tasks':[t['id'] for t in result['tasks']]})
    return result


def active_submissions(cfg):
    state=Path(cfg['state_dir']);active=set()
    for path in (state/'submissions').glob('*.json'):
        record=read_json(path)
        if record['state']=='submitting':
            raise RuntimeError(f'Uncertain Slurm submission in {path}; reconcile it before resubmitting')
        if record['state']!='submitted':
            continue
        # sacct, unlike squeue alone, distinguishes a finished job from missing accounting.
        result=subprocess.run(['sacct','-X','-n','-P','-j',record['job'],'--format=JobIDRaw,State'],
                              capture_output=True,text=True,check=True)
        states=[line.split('|')[1].split()[0].rstrip('+') for line in result.stdout.splitlines() if '|' in line]
        terminal={'COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE'}
        if not states or any(s not in terminal for s in states):
            active.update(record['tasks'])
        else:
            record['state']='finished';record['slurm_states']=states;atomic_json(path,record)
    return active


def submit(cfg,only=None,afterok=None):
    state=Path(cfg['state_dir'])
    with lock(state/'submission.lock'):
        result=prepare(cfg)
        known={t['id'] for t in result['tasks']}
        if only and not set(only)<=known:
            raise ValueError(f'Requested tasks are not ready or do not exist: {sorted(set(only)-known)}')
        if afterok and not str(afterok).isdigit():
            raise ValueError('afterok requires a numeric job ID')
        active=active_submissions(cfg)
        pending=[]
        for item in result['tasks']:
            task=item['id']
            if only and task not in only:
                continue
            if done_ok(cfg,task) or task in active:
                continue
            previous=list((state/'attempts'/task).glob('a[0-9][0-9][0-9]'))
            # Reservations count as attempts even if a worker never started.
            reservations=[r for p in (state/'submissions').glob('*.json')
                          if task in (r:=read_json(p)).get('tasks',[]) and r['state']!='submission_failed']
            attempt=max(len(previous),len(reservations))+1
            if attempt>cfg['max_attempts']:
                continue
            pending.append({'task':task,'attempt':attempt})
        if not pending:
            return None
        token=uuid.uuid4().hex
        # Store manifests separately from submission records to keep reconciliation unambiguous.
        manifest=state/'inventory'/f'submission_{token}.json'
        atomic_json(manifest,pending)
        record_path=state/'submissions'/f'{token}.json'
        record={'state':'submitting','token':token,'tasks':[p['task'] for p in pending],
                'manifest':str(manifest),'created':time.time()}
        atomic_json(record_path,record)
        slurm=cfg['slurm'];code=state/'code'/HERE.name
        command=['sbatch','--parsable',f'--array=0-{len(pending)-1}%{slurm["concurrency"]}',
                 f'--account={slurm["account"]}',f'--partition={slurm["partition"]}',
                 f'--cpus-per-task={slurm["cpus"]}',f'--mem={slurm["memory"]}',f'--time={slurm["time"]}',
                 f'--job-name=lr-{cfg["name"]}',f'--comment=lr-campaign:{token}',
                 f'--output={state}/logs/tasks-%A_%a.log']
        if afterok:
            command.append(f'--dependency=afterok:{afterok}')
        command += [str(code/'run_experiment_task.sh'),str(Path(cfg['venv'])/'bin/python'),
                    str(code/'campaign.py'),str(state/'campaign.yaml'),str(manifest)]
        try:
            answer=subprocess.check_output(command,text=True).strip()
        except subprocess.CalledProcessError:
            record['state']='submission_failed';atomic_json(record_path,record);raise
        job=answer.split(';')[0]
        if not job.isdigit():
            raise RuntimeError(f'Ambiguous sbatch response {answer!r}; inspect {record_path}')
        record.update(state='submitted',job=job,command=command)
        atomic_json(record_path,record)
        print(f'Submitted {job}: {len(pending)} tasks',flush=True)
        return job


def publish(cfg,item,attempt,out,status):
    state=Path(cfg['state_dir']);target=Path(item['recipe']['inherit']['output_directory'])
    journal=attempt/'publish.json'
    record=read_json(journal)
    if record is None:
        files=list(out.rglob('*.nc'))
        if len(files)!=len(item['recipe']['rules']):
            raise RuntimeError('Output count changed after validation')
        record={'task':item['id'],'output_directory':str(target),'sources':item['sources'],
                'relative_hashes':{str(p.relative_to(out)):sha(p) for p in files},
                'validation_status':str(status),'publication_ready':False}
        atomic_json(journal,record)
    if not target.exists():
        target.parent.mkdir(parents=True,exist_ok=True)
        # Refuse cross-filesystem copy-and-delete. State and output belong on one filesystem.
        os.rename(out,target)
    files=list(target.rglob('*.nc'))
    actual={str(p.relative_to(target)):sha(p) for p in files}
    if actual!=record['relative_hashes']:
        raise RuntimeError(f'Publication recovery conflict: {target}; nothing overwritten')
    for path,expected in record['sources'].items():
        if signature(path)!=expected:
            raise RuntimeError(f'Source changed before finalizing: {path}')
    record['files']=file_records(files)
    atomic_json(state/'done'/f'{item["id"]}.json',record)


def run_task(cfg,manifest,index):
    verify(cfg)
    state=Path(cfg['state_dir'])
    selected=read_json(manifest)[index]
    task=selected['task'];number=int(selected['attempt'])
    with lock(state/'tasks'/f'{task}.lock'):
        if done_ok(cfg,task):
            print(f'{task}: already validated; skipping');return 0
        item=read_json(state/'tasks'/f'{task}.json')
        attempt=state/'attempts'/task/f'a{number:03}'
        out=attempt/'output'
        if (attempt/'publish.json').exists():
            publish(cfg,item,attempt,out,None)
            return 0
        if attempt.exists():
            raise RuntimeError(f'Attempt already exists: {attempt}; reserve a new attempt')
        for path,expected in item['sources'].items():
            if signature(path)!=expected:
                raise RuntimeError(f'Source changed since discovery: {path}')
        attempt.mkdir(parents=True)
        recipe=copy.deepcopy(item['recipe'])
        recipe['inherit']['output_directory']=str(out)
        recipe_path=attempt/f'{task}_a{number:03}.yaml'
        recipe_path.write_text(yaml.safe_dump(recipe,sort_keys=False))
        code=state/'code'
        env=dict(os.environ,PYCMOR_LR_ROOT=str(state),PYCMOR_LR_REPO=str(code),
                 PYCMOR_LR_VENV=cfg['venv'],PYCMOR_LR_CACHE=cfg['cache_root'])
        result=subprocess.run(['bash',str(code/HERE.name/'run_recipe.sh'),str(recipe_path)],env=env)
        statuses=list((state/'provenance').glob(f'{recipe_path.stem}_*/status.json'))
        if len(statuses)!=1:
            raise RuntimeError(f'Expected one provenance status for {recipe_path.stem}')
        status=read_json(statuses[0])
        success=(result.returncode==0 and status['pycmor_exit_code']==0
                 and status.get('independent_validation_exit_code')==0
                 and status['sources_unchanged'] and not status.get('rejected_files'))
        if success:
            publish(cfg,item,attempt,out,statuses[0])
        atomic_json(attempt/'result.json',{'success':success,'status':str(statuses[0]),
                                         'returncode':result.returncode})
        return 0 if success else 1


def report(cfg):
    state=Path(cfg['state_dir'])
    coverage=read_json(state/'inventory/plan.json',read_json(state/'inventory/coverage.json',{}))
    wanted=[f'{g}_{y}' for y in range(cfg['first_year'],cfg['last_year']+1)
            for g in cfg['groups'] if not g.startswith('fx_')]
    wanted += [g for g in cfg['groups'] if g.startswith('fx_')]
    completed=[task for task in wanted if done_ok(cfg,task)]
    files=sum(len(read_json(state/'done'/f'{task}.json')['files']) for task in completed)
    return {'name':cfg['name'],'requested_years':[cfg['first_year'],cfg['last_year']],
            'ready_years':coverage.get('ready_years',0),'expected_tasks':len(wanted),
            'completed_tasks':len(completed),'validated_files':files,
            'expected_products':coverage.get('expected_products'),'complete':len(completed)==len(wanted),
            'incomplete_tasks':[t for t in wanted if t not in completed],
            'publication_ready':False}


def start(cfg):
    setup(cfg);state=Path(cfg['state_dir'])
    controller_renewal_seconds(cfg['slurm']['driver_time'])
    with lock(state/'start.lock'):
        ledger=state/'controller.json'
        old=read_json(ledger)
        if old:
            if old['state']=='submitting':
                raise RuntimeError('Uncertain controller submission; reconcile controller.json')
            if old.get('job'):
                active=subprocess.check_output(['squeue','-h','-u',os.environ.get('USER','a270270'),'-o','%i'],text=True).split()
                active=old['job'] in active
                if active:
                    print(f'Controller already active: {old["job"]}');return old['job']
        if report(cfg)['complete']:
            print('Campaign is already complete');return None
        token=uuid.uuid4().hex
        atomic_json(ledger,{'state':'submitting','token':token})
        s=cfg['slurm'];code=state/'code'/HERE.name
        command=['sbatch','--parsable',f'--account={s["account"]}',f'--partition={s["partition"]}',
                 '--cpus-per-task=1','--mem=4G',f'--time={s["driver_time"]}',
                 f'--job-name=lr-driver-{cfg["name"]}',f'--output={state}/logs/driver-%j.log',
                 str(code/'run_experiment_driver.sh'),str(Path(cfg['venv'])/'bin/python'),
                 str(code/'campaign.py'),str(state/'campaign.yaml')]
        try:
            answer=subprocess.check_output(command,text=True).strip()
        except subprocess.CalledProcessError:
            atomic_json(ledger,{'state':'submission_failed','token':token});raise
        job=answer.split(';')[0]
        if not job.isdigit():
            raise RuntimeError(f'Ambiguous sbatch response: {answer}')
        atomic_json(ledger,{'state':'submitted','job':job,'token':token,'command':command})
        print(f'Controller {job}; logs: {state}/logs; output: {cfg["output_root"]}')
        return job


def drive(cfg):
    verify(cfg);state=Path(cfg['state_dir']);started=time.monotonic()
    with lock(state/'driver.lock'):
        while True:
            submit(cfg)
            result=report(cfg)
            atomic_json(state/'inventory/status.json',result)
            print(json.dumps(result),flush=True)
            if result['complete']:
                return 0
            active=active_submissions(cfg)
            exhausted=[]
            for task in result['incomplete_tasks']:
                attempts=sum(task in read_json(p).get('tasks',[]) and read_json(p)['state']!='submission_failed'
                             for p in (state/'submissions').glob('*.json'))
                if attempts>=cfg['max_attempts'] and task not in active:
                    exhausted.append(task)
            if exhausted:
                print(f'Attempt limit reached: {exhausted}; retained all diagnostics',flush=True)
                return 1
            if not active and not cfg['follow']:
                print('Available work finished; requested years still missing. Restart when archives arrive.',flush=True)
                return 2
            # Bounded controller lifetime; automatic successor retains the same pinned state.
            # The default allocation is three days, and renewal occurs after two.
            if time.monotonic()-started>controller_renewal_seconds(cfg['slurm']['driver_time']):
                s=cfg['slurm'];code=state/'code'/HERE.name
                command=['sbatch','--parsable',f'--account={s["account"]}',f'--partition={s["partition"]}',
                         '--cpus-per-task=1','--mem=4G',f'--time={s["driver_time"]}',
                         f'--dependency=afterany:{os.environ["SLURM_JOB_ID"]}',
                         f'--job-name=lr-driver-{cfg["name"]}',f'--output={state}/logs/driver-%j.log',
                         str(code/'run_experiment_driver.sh'),str(Path(cfg['venv'])/'bin/python'),
                         str(code/'campaign.py'),str(state/'campaign.yaml')]
                job=subprocess.check_output(command,text=True).strip().split(';')[0]
                if not job.isdigit():
                    raise RuntimeError(f'Ambiguous controller renewal response: {job!r}')
                atomic_json(state/'controller.json',{'state':'submitted','job':job,'command':command})
                return 0
            time.sleep(cfg['poll_seconds'])


def controller_renewal_seconds(value):
    days,clock=(value.split('-',1) if '-' in value else ('0',value))
    parts=list(map(int,clock.split(':')))
    if len(parts)==3:
        h,m,s=parts
    elif len(parts)==2:
        h=0;m,s=parts
    else:
        h=0;m=parts[0];s=0
    duration=int(days)*86400+h*3600+m*60+s
    if duration<900:
        raise ValueError('Controller allocation must be at least 15 minutes')
    return duration-600


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['inspect','prepare','submit','start','report','drive','task'])
    parser.add_argument('config')
    parser.add_argument('--only',nargs='+',help='Explicit task IDs for a bounded validation submission')
    parser.add_argument('--afterok')
    parser.add_argument('--manifest')
    parser.add_argument('--index',type=int)
    args=parser.parse_args(argv)
    cfg=load_config(args.config)
    if args.action=='inspect':
        result=discover(cfg)
        print(json.dumps({'requested_years':result['requested_years'],
                          'ready_years':[r['year'] for r in result['years'] if r['ready']],
                          'waiting_years':[r['year'] for r in result['years'] if not r['ready']],
                          'ready_tasks':len(result['tasks']),'expected_products':result['expected_products'],
                          'details':str(Path(cfg['state_dir'])/'inventory/coverage.json')},indent=2))
        return 0
    if args.action=='report':
        print(json.dumps(report(cfg),indent=2));return 0
    if args.action=='start':
        start(cfg);return 0
    if args.action in ['prepare','submit']:
        setup(cfg)
        # Always prepare/submit with the campaign's pinned implementation.
        frozen=Path(cfg['state_dir'])/'code'/HERE.name/'campaign.py'
        if HERE.parent!=frozen.parents[1]:
            command=[sys.executable,str(frozen),*(argv or sys.argv[1:])]
            return subprocess.call(command)
        if args.action=='submit':
            submit(cfg,args.only,args.afterok)
        else:
            result=prepare(cfg)
            print(json.dumps({'ready_years':result['ready_years'],'tasks':len(result['tasks'])}))
        return 0
    if args.action=='drive':
        return drive(cfg)
    if args.manifest is None or args.index is None:
        parser.error('task requires --manifest and --index')
    return run_task(cfg,args.manifest,args.index)
