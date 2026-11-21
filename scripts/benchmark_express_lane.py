#!/usr/bin/env python3
"""Linux-only whole-process comparison of trusted Rust and imported spin variants."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import tempfile
import time
import math

import duckdb


def compare_outputs(left, right):
    with duckdb.connect() as connection:
        connection.execute("SET memory_limit = '512MB'")
        connection.execute('SET threads = 2')
        for table in ('user_daily','campaign_daily'):
            paths=[str(root/table/'**/*.parquet') for root in (left,right)]
            count=connection.execute('WITH a AS (SELECT * FROM read_parquet(?)), b AS (SELECT * FROM read_parquet(?))\n                SELECT count(*) FROM ((SELECT * FROM a EXCEPT ALL SELECT * FROM b)\n                UNION ALL (SELECT * FROM b EXCEPT ALL SELECT * FROM a))',paths).fetchone()[0]
            if count: raise RuntimeError(f'{table}: {count} differing rows')


def measure(binary, data, output, workers, spin):
    env = {k:v for k,v in os.environ.items() if not k.startswith('XL_')}
    env.update(NCPU=str(workers))
    env.pop('NATIVE_PROFILE',None)
    if spin is not None: env['XL_SPIN']=str(spin)
    with tempfile.TemporaryFile(mode='w+') as log:
        started=time.perf_counter()
        child=subprocess.Popen([str(binary),str(data),str(output)],env=env,stdout=subprocess.DEVNULL,stderr=log)
        _,status,usage=os.wait4(child.pid,0)
        child.returncode=os.waitstatus_to_exitcode(status)
        wall=time.perf_counter()-started
        log.seek(0); stderr=log.read()
    if child.returncode: raise RuntimeError(f'{binary} exited {child.returncode}: {stderr}')
    return dict(wall_seconds=wall,cpu_seconds=usage.ru_utime+usage.ru_stime,
                peak_rss_bytes=usage.ru_maxrss*1024,
                fallback=bool(re.search(r'FALLBACK|\[generic\]',stderr)),stderr=stderr)


def recommend(reports):
    if len(reports)<2 or not all(r['exact_outputs'] for r in reports):
        return {'selected':None,'reason':'Two exact seed comparisons are required'}
    eligible=[]
    for name in ('stock','spin0','spin50'):
        if not all(r['fast_path'][name] for r in reports): continue
        def better(r):
            candidate=r['summary'][name]
            controls=['trusted']+(['stock'] if name!='stock' else [])
            return all(candidate['wall_seconds']<=r['summary'][c]['wall_seconds']*.95
                       and candidate['cpu_seconds']<r['summary'][c]['cpu_seconds'] for c in controls)
        if all(better(r) for r in reports): eligible.append(name)
    selected=min(eligible,key=lambda n:sum(math.log(r['summary'][n]['wall_seconds']) for r in reports)) if eligible else None
    return {'selected':selected,'eligible':eligible,'reason':'Require >=5% wall improvement and lower CPU on both seeds; tuned variants must also beat stock'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('trusted',type=Path)
    parser.add_argument('candidate',type=Path)
    parser.add_argument('data',type=Path)
    parser.add_argument('report',type=Path)
    parser.add_argument('--workers',type=int,default=2)
    args=parser.parse_args()
    if platform.system()!='Linux' or args.workers<1: parser.error('Linux and positive workers required')
    binaries={'trusted':args.trusted.resolve(),'candidate':args.candidate.resolve()}
    hashes={k:hashlib.sha256(v.read_bytes()).hexdigest() for k,v in binaries.items()}
    variants={'trusted':None,'stock':None,'spin0':0,'spin50':50}
    runs=[]
    with tempfile.TemporaryDirectory(prefix='express-eval-') as tmp:
        root=Path(tmp)
        for round_number in range(3):
            names=list(variants)
            names=names[round_number:]+names[:round_number]
            for name in names:
                binary=binaries['trusted' if name=='trusted' else 'candidate']
                result=measure(binary,args.data.resolve(),root/f'{round_number}-{name}',args.workers,variants[name])
                runs.append(dict(variant=name,round=round_number,**result))
            for name in variants:
                if name!='trusted': compare_outputs(root/f'{round_number}-trusted',root/f'{round_number}-{name}')
    for name,binary in binaries.items():
        if hashlib.sha256(binary.read_bytes()).hexdigest()!=hashes[name]: raise RuntimeError('Executable changed during benchmark')
    summary={name:{key:statistics.median(r[key] for r in runs if r['variant']==name)
                   for key in ('wall_seconds','cpu_seconds','peak_rss_bytes')} for name in variants}
    fast_path={name:all(not r['fallback'] for r in runs if r['variant']==name) for name in variants if name!='trusted'}
    report=dict(platform=platform.platform(),workers=args.workers,binary_sha256=hashes,
                exact_outputs=True,fast_path=fast_path,summary=summary,runs=runs)
    args.report.parent.mkdir(parents=True,exist_ok=True)
    with args.report.open('x') as f: json.dump(report,f,indent=2)
    print(json.dumps({k:v for k,v in report.items() if k!='runs'},indent=2))


if __name__=='__main__': main()
