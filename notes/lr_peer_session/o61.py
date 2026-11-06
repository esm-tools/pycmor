import sys; sys.argv=['x']
src=open(sys.path[0]+'/overlap.py').read().split('cnt=')[0].replace("__file__.replace('overlap.py','opp.py')", "sys.path[0]+'/opp.py'")
exec(src)
import collections
print(collections.Counter('.'.join(n.split('.')[2:]) for n in ob['61']).most_common(8))
others=set().union(*[ob[j] for j in ob if j!='61'])
v61={n.split('.')[1] for n in ob['61']}; vo={n.split('.')[1] for n in others}
print('61 var names also requested elsewhere at other freq/shape:',len(v61&vo),'of',len(v61))
