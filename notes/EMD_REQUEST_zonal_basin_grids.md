# EMD: Gitter für die zonalen Schnitte von AWI-ESM3-4-2-veg-HR

Stand 2026-08-17. Vier Dateien unserer CMIP7-Ausgabe haben kein flächiges Gitter, sondern
eine Breitenachse und eine Beckenachse. Für die gibt es im Register keinen passenden
`horizontal_grid_cell`. Das ist eine Stage-1-Registrierung, wie bei g235, g236 und g238.

## Zuerst: bei uns ist etwas uneinheitlich

Das würde ich vor dem Einreichen geradeziehen, sonst zementiert der Eintrag den Fehler.
Gemessen an cli114:

| Datei | Punkte | Bereich | Schritt | Konvention |
|---|---|---|---|---|
| `msftm_tavg-ol-hyb-sea` | 181 | −90.0 bis 90.0 | 1° | Werte auf **ganzen** Grad |
| `msftm_tavg-rho-hyb-sea` | 180 | −89.5 bis 89.5 | 1° | Werte auf **halben** Grad |
| `hfbasin` | 166 | −77.5 bis 87.5 | 1° | halbe Grad, beschnitten |
| `sltbasin` | 166 | −77.5 bis 87.5 | 1° | halbe Grad, beschnitten |

Die unteren drei sind dieselbe Konvention, einmal global und zweimal auf den Ozeanbereich
gekürzt. Die oberste ist eine andere: 181 Punkte auf ganzen Grad von Pol zu Pol sind
Zellkanten, keine Zellmitten. Dieselbe Variable `msftm`, einmal auf Tiefe und einmal auf
Dichte gerechnet, landet also auf zwei unvereinbaren Achsen. Ursache ist vermutlich, dass
die beiden Rechenwege verschieden binnen.

**Vorschlag:** alles auf 180 Zellen mit Mitten auf halben Grad, −89.5 bis 89.5. Das ist
die Konvention, die drei der vier schon benutzen, es sind saubere 1°-Zellen mit Kanten auf
ganzen Grad, und die Pole liegen auf Zellkanten statt auf Zellmitten. Wo eine Variable
physikalisch nur einen Teilbereich abdeckt, wird auf diesem Gitter maskiert statt die
Achse zu kürzen. Dann beschreibt **ein** Eintrag alle vier Dateien.

## Was zu registrieren wäre

Ein `horizontal_grid_cell` für ein zonales Mittelungsgitter:

| Feld | Wert | Anmerkung |
|---|---|---|
| `grid_type` | `regular-latitude-longitude` | es gibt keinen eigenen Typ für zonale Achsen |
| `n_cells` | 180 | eine Zelle je Breitenband |
| `x_resolution` | 360 | jedes Band umspannt alle Längen |
| `y_resolution` | 1 | 1° in der Breite |
| `units` | `degree` | |
| `southernmost_latitude` | −89.5 | **Mitte** der südlichsten Zelle, nicht deren Kante |
| `westernmost_longitude` | 0 | siehe Anmerkung |
| `region` | `global` | |
| `temporal_refinement` | `static` | |
| `description` | Zonal mean grid: 180 latitude bands of 1 degree, each spanning all longitudes. Used for basin-integrated and zonally averaged ocean transports. | |

Die Beckenachse (`basin`, drei Werte: Atlantik/Arktis, Indopazifik, global) ist **keine**
Gitterdimension, sondern eine Zeichenkoordinate aus dem CMIP7-CV. Die gehört nicht in die
Registrierung.

**Korrektur, 2026-08-17.** Hier stand zuerst `southernmost_latitude: -90` mit der
Begründung, das sei die südliche Kante der ersten Zelle. Das ist die falsche Konvention.
Das Register führt dort die **Mitte** der südlichsten Zellreihe. Maßgeblich ist `g106`,
exakt unser Fall:

```
g106   grid_type: regular-latitude-longitude
       y_resolution: 1
       southernmost_latitude: -89.5
       westernmost_longitude: 0.5
```

Bei `g113` (0.25°) steht entsprechend −89.875, bei `g131` (0.5°) −89.75, bei `g129`
−89.859375. Alles Zellmitten. Für unsere 1°-Bänder ist also **−89.5** richtig.

Offen bleibt `westernmost_longitude`. `g106` führt dort 0.5, also wieder die Mitte der
ersten 1°-Längenzelle. Unser Band umspannt aber alle Längen (`x_resolution: 360`), und
die Mitte davon wäre 180. Ob das Register bei einer einzelnen umlaufenden Zelle 0 oder
180 erwartet, geht aus den vorhandenen Einträgen nicht hervor; `g190` als einziger
Ein-Zellen-Eintrag führt 0. Im Zweifel beim Einreichen nachfragen.

## Zur Einordnung, falls die Frage kommt

Warum ein eigener Eintrag und nicht das native Gitter: die Daten liegen nicht mehr auf dem
FESOM-Mesh. Sie sind über Längengrade und über Becken integriert, das Ergebnis hat mit den
3146761 Knoten nichts mehr zu tun. Aktuell tragen die vier Dateien `g130` und behaupten
damit ein Gitter, auf dem sie nicht liegen. Genau dieselbe Sorte Fehler wie die 142
Dateien mit `g113` in cli109.

Nicht Teil dieses Antrags, aber verwandt und noch offen: 30 weitere Dateien sind globale
Mittel und haben überhaupt keine horizontale Achse. Dafür ist laut DKRZ der Bereich der
ersten 100 Label reserviert, die Entscheidung steht noch aus. `g190` im Register
("A single grid cell for the entire globe", `n_cells: 1`) sieht passend aus, ist aber
nicht unserer, wird von niemandem referenziert, und beschreibt eine echte Zellgeometrie mit
Ursprung und Ausdehnung, die unsere Dateien gar nicht mitliefern. Deshalb bewusst nicht
benutzt.

## Erledigt, 2026-08-19

Registriert als **`g239`**, gemergt am 18.08. über PR #1294 zu Issue #1293. Der Eintrag
übernimmt den Antrag unverändert:

```
g239   grid_type: regular-latitude-longitude
       n_cells: 180        x_resolution: 360      y_resolution: 1
       southernmost_latitude: -89.5               westernmost_longitude: 0
       region: global      temporal_refinement: static
```

Damit ist auch die offene Frage von oben beantwortet: bei einer einzelnen umlaufenden
Zelle steht `westernmost_longitude: 0`, nicht 180.

Die Uneinheitlichkeit aus dem ersten Abschnitt ist vorher schon behoben worden. In
cli116 liegen alle vier Dateien auf derselben Achse, 180 Bänder von −89.5 bis 89.5 mit
Schritt 1. Die 181-Punkt-Variante von `msftm` auf Dichte und die auf 166 Punkte
gekürzten `hfbasin`/`sltbasin` gibt es nicht mehr.

`grid_label` für die vier Regeln in
`awi-esm3-veg-hr-variables/lrcs_ocean/cmip7_awiesm3-veg-hr_lrcs_ocean.yaml` steht jetzt
auf `g239`. Vorher erbten sie `g130` aus dem `inherit`-Block des Tiers und behaupteten
damit die 3146761 FESOM-Knoten. Wirksam wird das mit dem nächsten Lauf.
