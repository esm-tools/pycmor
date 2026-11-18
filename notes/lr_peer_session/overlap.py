exec(open(__file__.replace('overlap.py','opp.py')).read().split('order=')[0])
import collections
ob={r[0]:set().union(*r[2].values()) if r[2] else set() for r in res}
co={r[0]:{n for s in r[3].values() for n in s if '4co2' not in n} for r in res}
allob=set().union(*ob.values()); allco=set().union(*co.values())
print('union obgc',len(allob),' sum over opps',sum(map(len,ob.values())))
print('union co2',len(allco),' sum over opps',sum(map(len,co.values())))
cnt=collections.Counter(n for s in ob.values() for n in s)
print('obgc vars by #opps requesting:',sorted(collections.Counter(cnt.values()).items()))
keys=[k for k in ob if ob[k]]
keys.sort(key=lambda k:-len(ob[k]))
for k in keys:
    others=set().union(*[ob[j] for j in keys if j!=k])
    print(f"{k:>3} n={len(ob[k]):3} unique_to_it={len(ob[k]-others):3}")
# greedy cover
cov=set(); 
for _ in range(len(keys)):
    k=max(keys,key=lambda j:len(ob[j]-cov)); g=len(ob[k]-cov)
    if g==0: break
    cov|=ob[k]; print('greedy',k,'+',g,'->',len(cov))
