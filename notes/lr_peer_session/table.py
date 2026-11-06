import json, sys
rows=json.load(open(sys.argv[1]))
# add the two 3D rate variables (pp, graz)
extra=[('ocnBgchem.pp.tavg-ol-hxy-sea.mon.glb','10M 16M 18M 44M'),('ocnBgchem.pp.tavg-ols-hxy-sea.mon.glb','16H 44H'),
       ('ocnBgchem.pp.tavg-ols-hxy-sea.mon.30S-90S','61H'),('ocnBgchem.graz.tavg-ol-hxy-sea.mon.glb','16M 44M'),
       ('ocnBgchem.graz.tavg-ol-hxy-sea.mon.30S-90S','61H')]
# W = written now (rule only), C = namelist/config change, S = small source change, N = not in model
M={
 'fgco2':('W','CO2f (mmolC/m2/d, + into ocean)'),
 'fg13co2':('N','ciso=.false.; with ciso on still needs a 13C air-sea flux stream'),
 'fg14co2abio':('N','use_transit=.false.; no abiotic 14C air-sea flux stream'),
 'fgdms':('N','no DMS in REcoM'),
 'epn':('S','sinking flux not saved; ballasting makes wsink x mean(Det) only approximate'),
 'expc':('S','as epn: DetC*wsink_det1 + idetz2c*wsink_det2'),
 'expcalc':('S','as epn: DetCalc, idetz2calc'),
 'epsi':('S','as epn: DetSi, idetz2si'),
 'epp':('N','no phosphorus in REcoM'),'exppob':('N','no phosphorus'),'intpbp':('N','no phosphorus'),
 'exparagob':('N','no aragonite'),'intparag':('N','no aragonite'),
 'expcob':('S','bottom sinking flux into benthos exists internally, no stream'),
 'expnob':('S','as expcob'),'expsiob':('S','as expcob'),'expcalcob':('S','as expcob'),
 'expfeob':('S','as expnob x Fe2N (fixed 0.033)'),
 'froc':('S','as expcob'),'fric':('S','as expcalcob'),
 'frn':('S','benthic N loss + denitrification, internal only'),
 'frfe':('S','Fe burial/scavenging, internal only'),
 'fsfe':('W','aFe (umolFe/m2/s); rivers off'),
 'fsn':('W','aN (mmolN/m2/s); no N2 fixation, rivers off'),
 'icfriver':('N','userivers=.false.; no riverine DIC stream'),
 'ocfriver':('N','userivers=.false.; no riverine DOC stream'),
 'intpp':('W','NPPn + NPPd (2D, depth-integrated)'),
 'intppdiat':('W','NPPd'),'intppnano':('W','NPPn (small phytoplankton)'),
 'intppmisc':('N','small phyto already reported as nano'),
 'intpppico':('N','no picophytoplankton'),'intppdiaz':('N','no diazotrophs'),'intpn2':('N','no N2 fixation'),
 'intppcalc':('N','enable_coccos=.false.; NPPc stream is commented out'),
 'intppnitrate':('N','DIN not split into NO3/NH4'),
 'intpbn':('W','NNAn + NNAd'),'intpbfe':('W','(NNAn + NNAd) x Fe2N, fixed ratio'),
 'intpbsi':('S','diatom Si uptake not saved'),
 'intpcalcite':('W','calcif summed over depth'),
 'sigpp':('N','no sea-ice algae'),
 'pp':('W','NPPn3D + NPPd3D'),'graz':('S','vertgraz* arrays exist, no stream'),
 'fco2all':('W','co2apf + co2nbf + co2fire + co2of (signs to check)'),
 'fco2antt':('W','co2apf (+ LPJG fLuc); LPJG also writes fco2antt_monthly.out'),
 'fco2fos':('W','co2apf, if it is fossil only'),
 'fco2nat':('W','co2nbf + co2fire + co2of; LPJG writes fco2nat_monthly.out (land part)'),
 'netAtmosLandCO2Flux':('W','LPJG netAtmosLandCO2Flux_monthly.out'),
 'ficeberg':('W','1000 x (ibfwb+ibfwbv+ibfwl+ibfwe)'),
 'flandice':('W','partial: -1000 x fw under cavities (shelf basal melt only)'),
 'hfibthermds':('W','ibhf (3D) / sum over depth'),
 'libmassbf':('W','1000 x fw at cavity nodes'),
 'licalvf':('N','no ice-sheet model; calving prescribed via icb_*.dat'),
 'lifmassbf':('N','no ice-sheet model'),
 'tendlicalvf':('N','no ice-sheet model; total could be summed from icb_*.dat'),
}
def status(r):
    n=r['name']; root=r['root']; parts=n.split('.'); br,fr,reg=parts[2],parts[3],parts[4]
    s,src=M[root]
    if fr=='subhr': return 'N','site (hs) sub-hourly output not set up in XIOS'
    if root in('libmassbf',) and ('-gis' in br or reg=='grl'): return 'N','grounded ice / no Greenland cavities'
    if root=='ficeberg' and fr=='3hr': return 'S','icb stream hardcodes monthly'
    if root=='flandice' and fr=='3hr': return 'C','fw at 3h in namelist.io; partial as monthly'
    if root=='intpp' and fr=='day': return 'C','NPPn, NPPd daily in namelist.io'
    if root=='expcob' and fr=='day': return 'S',src
    return s,src
for n,o in extra:
    rows.append(dict(name=n,root=n.split('.')[1],title='Primary production rate' if '.pp.' in n else 'Grazing rate',opps=o,cat='obgc',units='mol m-3 s-1'))
NODE,LEV,SO30,SO60,ATM=220509,47,93000,42798,40320
T={'mon':12,'yr':1,'day':365,'3hr':2920,'fx':1,'subhr':0}
def vol(n):
    realm,root,br,fr,reg=n.split('.')
    if '-hm-' in br or '-hs-' in br: return 0.0
    if realm in('ocean','ocnBgchem','landIce'):
        h={'30S-90S':SO30,'ata':SO60,'grl':0 if realm=='landIce' and root=='libmassbf' else 50085}.get(reg,NODE)
    else: h=ATM
    z=LEV if '-ol-' in br else 1
    return h*z*T[fr]*4/1e6
out=[]
for r in rows:
    s,src=status(r); v=vol(r['name'])
    out.append((r['cat'],r['name'],r['opps'],s,src,v))
order={'W':0,'C':1,'S':2,'N':3}
cats={'obgc':'Ocean biogeochemistry','co2':'Atmosphere/land CO2','ice':'Icebergs and ice shelves'}
for c in ['obgc','co2','ice']:
    print(f'\n### {cats[c]}\n\n| Variable | Requested by | Status | Source / reason | MB/yr |\n|---|---|---|---|---|')
    for cat,n,o,s,src,v in sorted([x for x in out if x[0]==c],key=lambda x:(order[x[3]],x[1])):
        print(f'| `{n}` | {o} | {s} | {src} | {v:,.0f} |' if v>=1 else f'| `{n}` | {o} | {s} | {src} | {"<1" if v>0 else "~0"} |')
print()
import collections
agg=collections.defaultdict(lambda:[0,0.0])
for cat,n,o,s,src,v in out: agg[s][0]+=1; agg[s][1]+=v
for s in 'WCSN': print(s,agg[s][0],'vars',f'{agg[s][1]/1e3:.2f} GB/yr')
print('total',len(out))
