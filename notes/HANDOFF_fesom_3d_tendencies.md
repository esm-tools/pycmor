# FESOM-Tendenzdiagnostik: 2D gerechnet, 3D verlangt

Stand 2026-08-17. Betrifft sechs CMIP7-Ozeanvariablen. Für HR sind die pycmor-Regeln
deaktiviert (`awi-esm3-veg-hr-variables/lrcs_ocean/`), für LR muss es vor dem Schnitt
der neuen Version aus `develop` in den FESOM-Quellcode.

## Was der Data Request will

| Variable | verlangt | was wir liefern |
|---|---|---|
| `opottemptend` | `(time, lev, ncells)` | `(time, nod2)` |
| `opottemprmadvect` | `(time, lev, ncells)` | `(time, nod2)` |
| `opottempdiff` | `(time, lev, ncells)` | `(time, nod2)` |
| `osalttend` | `(time, lev, ncells)` | `(time, nod2)` |
| `osaltrmadvect` | `(time, lev, ncells)` | `(time, nod2)` |
| `osaltdiff` | `(time, lev, ncells)` | `(time, nod2)` |

Nachgemessen an `AWI-ESM3-VEG-HR-CMIP7-piControl/outdata/fesom`, Jahre 1850 und 1851,
alle sechs Ströme einheitlich `(time=12, nod2=3146761)`.

## Warum XIOS das nicht lösen kann

Der erste Verdacht war eine fehlende Zeile im `file_def`. Ist es nicht. Die Felder sind
im Diagnosemodul konstruktiv zweidimensional, und der Quellcode sagt es selbst:

`fesom-2.7/src/gen_modules_cmor_diag.F90`, gleich in allen develop-Bäumen geprüft:

```fortran
real(kind=WP), save, allocatable :: opottemptend(:)    ! Ocean potential temperature tendency [W/m^2] - 2D field
real(kind=WP), save, allocatable :: osalttend(:)       ! Salinity tendency [psu*m/s] - 2D column-integrated
real(kind=WP), save, allocatable :: opottemprmadvect(:)! Temp tendency from advection [W/m^2] - 2D
real(kind=WP), save, allocatable :: opottempdiff(:)    ! Temp tendency from diffusion [W/m^2] - 2D
real(kind=WP), save, allocatable :: osaltrmadvect(:)   ! Salt tendency from advection [psu*m/s] - 2D
real(kind=WP), save, allocatable :: osaltdiff(:)       ! Salt tendency from diffusion [psu*m/s] - 2D
```

```fortran
allocate(opottemptend(myDim_nod2D))
```

Ein XIOS-`grid_ref` auf ein 3D-Gitter läuft ins Leere, wenn das Feld nur eine Dimension
hat. Passend dazu stehen die regemappten Varianten im `file_def` schon heute explizit auf
`grid_2d_nod_reg`, während `rsdoabsorb` daneben `grid_3d_nod_reg` bekommt und tatsächlich
`(time, nod2, nz)` liefert.

## Was zu ändern ist

Die gute Nachricht: der Term pro Level wird bereits gerechnet, er wird nur aufsummiert.

```fortran
opottemptend(n2) = opottemptend(n2) + &
    (temp(k, n2) - previous_temp(k, n2)) / dt * vcpw * hnode(k, n2)
```

Das steht in der `k`-Schleife über die Level. Für die 3D-Variante fällt nur die
Akkumulation weg:

1. Deklaration auf `(:,:)` ändern, wie `previous_temp(:,:)` in derselben Datei, das
   bereits `(nl-1, myDim_nod2D)` ist.
2. `allocate(opottemptend(nl-1, myDim_nod2D))`.
3. Zuweisung statt Akkumulation: `opottemptend(k, n2) = (temp(k,n2) - previous_temp(k,n2)) / dt * vcpw * hnode(k,n2)`.
4. Zurücksetzen auf `0.0_WP` entsprechend über beide Dimensionen.
5. Für die anderen fünf dasselbe an ihren jeweiligen Stellen.

Einheiten bleiben `W m-2` beziehungsweise `psu m s-1`. Die CMIP-Definition ist die
Änderung des Wärmeinhalts pro Flächeneinheit **der jeweiligen Schicht**, nicht der
Säule, also ist die per-Level-Größe schon die richtige und die Summe war der Zusatz.

Danach in `namelists/fesom2/xios_xml_cmip7/file_def_fesom.xml.j2`:

- native Einträge (aktuell Zeilen 81 bis 86) bleiben, brauchen kein `grid_ref`
- die regemappten (aktuell Zeilen 231 bis 236) von `grid_2d_nod_reg` auf
  `grid_3d_nod_reg` umstellen, wie bei `rsdoabsorb`

## Danach

Die sechs Regeln in `awi-esm3-veg-hr-variables/lrcs_ocean/cmip7_awiesm3-veg-hr_lrcs_ocean.yaml`
wieder einkommentieren. Sie sind als Block auskommentiert und tragen den Verweis auf
diese Datei. Für HR bleibt es dabei, solange kein Lauf mit der neuen Version existiert.

## Gegenprobe

Ob es gewirkt hat, sieht man ohne pycmor:

```bash
ncdump -h opottemptend.fesom.<jahr>.nc | grep "opottemptend("
# soll: opottemptend(time, nod2, nz) statt opottemptend(time, nod2)
```

Und im cmorisierten Lauf meldet `cc-plugin-aicc` es direkt:

```
[AICC003] Vertical coordinate 'lev' (olevel)
  Vertical coordinate variable not found for 'olevel' (expected out_name='lev', standard_name='depth')
```

Dieser Befund muss verschwinden.
