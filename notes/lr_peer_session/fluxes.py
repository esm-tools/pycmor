import json, os, re, collections, sys
d=json.load(open(os.path.expanduser('~/.CMIP7_data_request_api_cache/v1.2.2.5/dreq_release_consolidate.json')))['Data Request']
R=lambda t:d[t]['records']
L=lambda x:x if isinstance(x,list) else ([x] if x else [])
realm={k:v['id'] for k,v in R('Modelling Realm').items()}
P={k:v['Name'] for k,v in R('Priority Level').items()}
V=R('Variables'); G=R('Variable Group'); O=R('Opportunity'); PP=R('Physical Parameters')
ice=re.compile(r'^(ficeberg|flandice|hfibthermds|libmassbf|licalvf|lifmassbf|tendlicalvf)$')
out=[]
for k,v in V.items():
    n=v['CMIP7 Compound Name']; root=n.split('.')[1]
    ob=any(realm.get(r)=='ocnBgchem' for r in L(v.get('Modelling Realm - Primary')))
    co='co2' in root.lower() and '4co2' not in root
    ic=bool(ice.match(root))
    if not (ob or co or ic): continue
    pp=[PP[x] for x in L(v.get('Physical Parameter')) if x in PP]
    sn=pp[0].get('CF Standard Name','') if pp else ''
    sn=sn if isinstance(sn,str) else str(sn)
    units=pp[0].get('Units','') if pp else ''
    isflux=('flux' in (sn or '').lower()) or ('flux' in v['Title'].lower()) or re.search(r's-1',str(units)) and re.search(r'm-2',str(units))
    if not isflux: continue
    opps=set()
    for g in L(v.get('CMIP7 Variable Groups')):
        gg=G.get(g)
        if not gg: continue
        p=P.get(L(gg.get('Priority Level'))[0],'?')[0]
        for o in L(gg.get('Opportunity')):
            if o in O: opps.add((int(O[o]['Opportunity ID']),p))
    best={}
    for o,p in opps:
        if o not in best or 'CHML'.index(p)<'CHML'.index(best[o]): best[o]=p
    out.append(dict(name=n,root=root,title=v['Title'],units=units,sn=sn,dims=v.get('Dimensions'),
        opps=' '.join(f'{o}{p}' for o,p in sorted(best.items())),
        prio=min((p for p in best.values()),key='CHML'.index) if best else '-',
        cat='ice' if ic else ('obgc' if ob else 'co2')))
json.dump(out,open(sys.argv[1],'w'),indent=1)
print(len(out)); 
for r in sorted(out,key=lambda r:(r['cat'],r['name'])): print(r['cat'],r['name'],'|',r['units'],'|',r['title'][:55],'|',r['opps'])
