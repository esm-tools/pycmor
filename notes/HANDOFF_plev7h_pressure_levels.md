# plev7h: das Modell schreibt den CMIP6-Satz, der Data Request will den CMIP7-Satz

Stand 2026-09-08. Fünf 6-stündliche Variablen sind für HR deaktiviert, weil zwei
der sieben Druckflächen falsch sind und sich nachträglich nicht beschaffen lassen.

Betroffen: `hus`, `ta`, `ua`, `va`, `zg`, jeweils `tpt-p7h-hxy-air.6hr.glb`.

## Der Befund

`aicc` meldet auf jeder der fünf Dateien:

```
[AICC005] Coordinate 'plev7h'
'plev' missing requested value(s) [60000.0, 5000.0] (outside tolerance, factor=1.0).
```

Nachgemessen an `atmos_6h_pl7h_{ta,ua,hus}_1851-1851.nc`:

| | Flächen in Pa |
|---|---|
| geschrieben | 100000, 92500, 85000, 70000, 50000, 25000, 10000 |
| verlangt | 92500, 85000, 70000, 60000, 50000, 25000, 5000 |

Fünf stimmen. `100000` steht, wo `60000` gehört, und `10000`, wo `5000` gehört.
Das ist der CMIP6-Satz, nicht der CMIP7-Satz.

Ursache ist eine Zeile in `esm_tools`,
`namelists/oifs/48r1/xios/cmip7/axis_def.xml`:

```xml
<axis id="pressure_levels_7h" long_name="vertical pressure levels plev7h" n_glo="7"
      value="(0,6)[100000.0 92500.0 85000.0 70000.0 50000.0 25000.0 10000.0]" />
```

## Warum nichts zu retten ist

Der Lauf hat 196 Jahre geschrieben, 1850 bis 2045, und endete am 27.08.

Ein Nachrechnen der zwei fehlenden Flächen bräuchte 6-stündliche Felder auf
Modellflächen. Die gibt es nicht. Vorhanden sind nur `atmos_day_ml_cl` und
`atmos_day_ml_pfull` täglich sowie `atmos_mon_ml_{ta,hus,hur,cli,clw}` monatlich.
Aus täglichen oder monatlichen Feldern lassen sich keine 6-stündlichen
Momentanwerte herstellen.

Interpolation wurde erwogen und verworfen. `60000` läge zwischen `70000` und
`50000`, beide vorhanden, und wäre in log-p sauber interpolierbar. `5000` liegt
über der obersten geschriebenen Fläche und wäre Extrapolation. In beiden Fällen
stünde am Ende eine Zahl unter einem Label, das Modellausgabe behauptet. Für
`5000` ist das offensichtlich unhaltbar, für `60000` wäre es eine Entscheidung,
die niemand still treffen sollte.

## Was das kostet

Nicht wenig. Die fünf korrekten Flächen sind 925, 850, 700, 500 und 250 hPa,
also genau die, die für Sturmzugbahnen und Extremwertanalysen gebraucht werden,
und sie liegen 6-stündlich über 196 Jahre vor. Die Deaktivierung wirft das mit
weg. Wer die Daten trotzdem will, findet sie unverändert im Modelloutput unter
`atmos_6h_pl7h_*`.

## Für LR und jede HR-Fortsetzung

In `axis_def.xml` korrigiert auf:

```xml
value="(0,6)[92500.0 85000.0 70000.0 60000.0 50000.0 25000.0 5000.0]"
```

Beide Listen stehen absteigend, es ist also ein reiner Werteaustausch ohne
Umsortierung. `grid_def.xml` referenziert die Achse nur über `pressure_levels_7h`
und braucht keine Änderung.

Danach die fünf Regeln in
`awi-esm3-veg-hr-variables/cap7_atm/cmip7_awiesm3-veg-hr_cap7_atm.yaml` wieder
einkommentieren.

## Einordnung

Gleiche Klasse wie:

- die sechs FESOM-Tendenzvariablen, 2-D statt 3-D geschrieben (`a0435c30`,
  [HANDOFF_fesom_3d_tendencies.md](HANDOFF_fesom_3d_tendencies.md))
- `sistress` (`ecee1bad`)
- die beiden `mrsol`-d10cm-Regeln mit falscher Tiefe (`3f0fbf35`)
- `mrsol` 3hr tavg-d100cm ohne gemittelte Quelle (`8f709fc8`)

Jedes Mal dieselbe Ursache: die Modellkonfiguration schreibt nicht, was der Data
Request verlangt, und nach dem Lauf ist es nicht mehr zu heilen. Es lohnt sich,
die XIOS-Definitionen vor dem LR-Lauf einmal vollständig gegen die
Koordinatentabelle zu prüfen, statt sie einzeln über QC-Befunde zu finden.
