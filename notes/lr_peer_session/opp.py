import json, os, re, collections
d=json.load(open(os.path.expanduser('~/.CMIP7_data_request_api_cache/v1.2.2.5/dreq_release_consolidate.json')))['Data Request']
R=lambda t:d[t]['records']
def ids(x):
    if x is None: return []
    return x if isinstance(x,list) else [x]
realm={k:v['id'] for k,v in R('Modelling Realm').items()}
prio={k:v['Name'] for k,v in R('Priority Level').items()}
vars_=R('Variables'); vg=R('Variable Group'); opp=R('Opportunity')
def link(x):
    out=[]
    for i in ids(x):
        out.append(i.split('link::')[-1] if isinstance(i,str) else i)
    return out
def is_obgc(v): return any(realm.get(r)=='ocnBgchem' for r in link(v.get('Modelling Realm - Primary')))
def is_co2(v):
    n=v['CMIP7 Compound Name'].split('.')[1]
    return 'co2' in n.lower()
res=[]
for oid,o in opp.items():
    o_ob=collections.defaultdict(set); o_co=collections.defaultdict(set)
    for g in link(o.get('Variable Groups')):
        G=vg.get(g)
        if not G: continue
        p=[prio.get(x,x) for x in link(G.get('Priority Level'))]; p=p[0] if p else '?'
        for vid in link(G.get('Variables')):
            v=vars_.get(vid)
            if not v: continue
            if is_obgc(v): o_ob[p].add(v['CMIP7 Compound Name'])
            if is_co2(v): o_co[p].add(v['CMIP7 Compound Name'])
    res.append((o['Opportunity ID'],o['Title of Opportunity'],o_ob,o_co))
order=['Core','High','Medium','Low']
def fmt(dd):
    seen=set(); parts=[]
    for p in order+[k for k in dd if k not in order]:
        s=dd.get(p,set())-seen
        if s: parts.append(f"{p[0]}{len(s)}"); seen|=s
    return ' '.join(parts) or '-'
for oid,t,ob,co in sorted(res,key=lambda r:int(r[0])):
    if any(ob.values()) or any(co.values()):
        cos=sorted({n.split('.')[1] for s in co.values() for n in s})
        print(f"{oid:>3} | {t[:60]:60} | obgc {fmt(ob):18} | co2 {fmt(co):12} | {','.join(cos)}")
print('none:',[ (r[0],r[1][:40]) for r in res if not any(r[2].values()) and not any(r[3].values())])
