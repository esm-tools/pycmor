# Upstream im QC-Stack: was offen ist und auf wen es wartet

Stand 2026-08-21, nach der Runde vom selben Tag. Erhoben gegen die tatsächlichen GitHub-Zustände, nicht aus dem
Gedächtnis. Gemessen an `cli117` (534 Dateien, 24 HIGH, 306 MEDIUM).

Zweck: verhindern, dass ein eigener PR unbemerkt auf uns wartet. Genau das war
bei `#1323` sechs Wochen lang der Fall.

## Reihenfolge

| # | Was | Aufwand | Wirkung | Wer ist dran |
|---|---|---|---|---|
| 1 | `compliance-checker#1323` nachbessern | ✅ `becb3ff` + Antwort | 25 MEDIUM | wartet auf `benjwadams` |
| 2 | `WCRP-universe#190` nachfragen | ✅ gefragt, `#47` geschlossen | ~70 MEDIUM | wartet |
| 3 | `vegtype`-Tippfehler melden | ✅ entfällt, war schon gemeldet | 1 HIGH | upstream, mit Plan |
| 4 | `compliance-checker#1293` nachfassen | ✅ nachgefasst | Rauschen auf jedem Mesh | wartet |

Alles offene wartet jetzt auf fremde Arbeit und braucht von uns nichts.

## Was am 21.08. rausging

| Wohin | Was |
|---|---|
| [`compliance-checker#1323`](https://github.com/ioos/compliance-checker/pull/1323#issuecomment-5367535835) | Commit `becb3ff` plus Antwort an `benjwadams` |
| [`WCRP-universe#190`](https://github.com/WCRP-CMIP/WCRP-universe/issues/190#issuecomment-5368962786) | Frage nach dem weiteren Weg |
| [`cc-plugin-wcrp#47`](https://github.com/ESGF/cc-plugin-wcrp/pull/47#issuecomment-5368970297) | kommentiert und geschlossen |
| [`compliance-checker#1293`](https://github.com/ioos/compliance-checker/issues/1293#issuecomment-5368997596) | Nachfassen nach vier Monaten |

Offen an `#1323`: der Titel sagt weiter „per CF 1.11", obwohl der Kommentar jetzt
CF 1.6 zitiert. Und `benjwadams` muss seine `CHANGES_REQUESTED` selbst
zurücknehmen, das geschieht nicht automatisch.

## Offene PRs von uns

### `ioos/compliance-checker#1323` — cf §7.2, mehrere `cell_measures`

**Blockiert an uns.** `ocefpaf` hat am 29.06. approved, mit „I'll merge in ~1
week". Danach hat `benjwadams` am 09.07. Änderungen angefordert, und seitdem ist
nichts passiert.

Der gesamte Einwand ist ein Kommentar an `compliance_checker/cf/cf_1_6.py:2799`:

> We don't really want to mix and match commentary from separate CF versions if
> possible [...] I'd update to mention a pertinent section of CF 1.6 in the
> comment, or eliminate the reference to CF 1.11 in the comment entirely.

Kein Codeverhalten, nur die Versionsangabe im Kommentar. Der PR räumt die
25 `cf §7.2`-MEDIUM ab, die wir uns beim `cell_measures`-Tausch eingehandelt
haben (39 `ATTR004` raus, 31 §7.2 rein, weil der cf-Checker die kombinierte Form
`area: X volume: Y` nicht akzeptierte).

### `ESGF/cc-plugin-wcrp#47` — ATTR004 `long_name`, Variante vor Wurzel

Offen seit 17.06., unbewegt seit 02.07., aber **derzeit wirkungslos**.

**Korrektur, 2026-08-21.** Hier stand, `WCRP-universe#191` sei am 15.07.
gemergt worden und #47 sei die fehlende zweite Hälfte. Falsch. Die GitHub-API
sagt `"merged": false`: #191 wurde von `ltroussellier` **geschlossen, ohne
gemergt zu werden**, ohne Abschlusskommentar. Ich hatte aus „closed_by" auf
„merged" geschlossen.

Nachgemessen in der ausgelieferten CV `cmip7@1.2.18`, die Registry ist
unverändert:

    cveg       long_name = "Carbon Mass in Vegetation on Grass Tiles"
    cveggrass  long_name = None
    cvegshrub  long_name = None
    cvegtree   long_name = None

Damit ist #47 **ein No-op**. Er nimmt die Variante, deren registrierter
`long_name` zum `long_name` der Datei passt, und fällt auf den Wurzelterm
zurück, wenn keine passt. Solange alle Varianten `null` führen, passt nie eine,
also greift immer der Rückfall, also ändert sich nichts. Nachfassen bringt hier
nichts.

**Warum #191 zu ist.** Die Diskussion lief auf zwei Einwände hinaus. `taylor13`
am 23.06.: „Is the plan to make changes to the descriptions and long_names
without first correcting the data request? I'm not sure we should do that."
`glevava` antwortete „that's not the plan". Dazu `ltroussellier` am 19.06., die
Validierung ziehe ohnehin in ein neues `known_branded_variables`-Pydantic-Modell
um, das die Sache mit erledigen könne. Danach drei Wochen Stille, dann zu.

Die rund 70 MEDIUM sind also nicht liegengeblieben, sondern upstream anders
eingeordnet: sie sollen über die Datenanfrage korrigiert werden, nicht über die
Registry. Offen ist nur, ob das jemand verfolgt.

### `ioos/compliance-checker#1324` — cf §7.3.4, `cell_methods`

`ocefpaf` approved 29.06., wartet ausdrücklich auf einen zweiten Blick von
`benjwadams`. Von uns nichts zu tun.

### `ioos/compliance-checker#1346` — cf §2.4, `formula_terms`-Bounds

Null Reviews seit 16.08. Fünf Tage alt, Stille ist normal.

## Offene Issues von uns

| Issue | seit | Kommentare | Lage |
|---|---|---|---|
| `cc-plugin-wcrp#66` | 04.08. | 0 | `sol1105` hat mit PR #67 reagiert, der wartet auf Review |
| `cc-plugin-wcrp#56` | 01.07. | 4, zuletzt 03.07. | ATTR007 verlangt `sub_experiment_id`, das CMIP7 abgeschafft hat |
| `compliance-checker#1345` | 16.08. | 0 | §2.4 auf unstrukturierten Gittern, jung |
| `compliance-checker#1293` | 17.04. | 1 | offene Frage an die Maintainer (Option A/B/C/D), **seit vier Monaten unbeantwortet** |
| `WCRP-universe#190` | 17.06. | 10, zuletzt 24.06. | Vorarbeit zu #191, erledigt |

## Fremdes, worauf wir warten

- `CMIP7-CVs#592` **wurde am 04.09. gemergt** (Korrektur, hier stand vorher
  "Entwurfsstatus"). Aber: `grid_label/g239.json` liefert auf `main` 404 und
  auf `esgvoc_dev` 200. Das Label ist also im Entwicklungszweig angekommen,
  nicht dort, wo Releases geschnitten werden. Die neuesten CV-Releases sind
  `2.0.1` (01.09.) und `1.2.19` (26.08.), beide aelter als der Merge, also
  enthaelt noch keine veroeffentlichte CV das `g239`. Die 16 HIGH bleiben bis
  zum naechsten Release.
- `cc-plugin-wcrp#67` von `sol1105`, offen seit 05.08., wartet auf `Ayoubnac1`.
  Daran hängt der `basin`-HIGH.

## Nicht upstream, sondern unsere Modellseite

- `plev7h`, 5 HIGH. XIOS schreibt 100000/10000 statt 60000/5000. Braucht eine
  Modelländerung und einen neuen Lauf.
- `pfull`-Klimatologie, 1 HIGH. Kein Fehler, das ist die Idempotenz des
  Akkumulators.

## `vegtype`: gemeldet, mit Plan

**Korrektur, 2026-08-21.** Hier stand, der Tippfehler sei von niemandem
gemeldet. Falsch. Martin Schupfner (`sol1105`) hat ihn am 19.08. in
[`CMIP-Data-Request/CMIP7_DReq_Content#29`](https://github.com/CMIP-Data-Request/CMIP7_DReq_Content/issues/29#issuecomment-5339324809)
gemeldet und Jan namentlich als Finder genannt.

Meine Vorabprüfung hatte nur `ESGF`, `WCRP-CMIP` und `ioos` durchsucht. Die
Organisation **`CMIP-Data-Request`** fehlte, und genau dort lag es. Für die
nächste Prüfung mitnehmen: `cmip7-cmor-tables` erbt seine Werte aus dem Data
Request, die Wurzel ist also immer `CMIP7_DReq_Content`.

Aus dem Issue kommt mehr. `taylor13` schlägt vor, eine Koordinate
`modelvegtype` ohne `requested`-Werte anzulegen und `landCoverFrac` darauf
umzustellen, weil dessen PFT-Kategorien modellabhängig sind und eine feste
Werteliste dort ohnehin falsch ist. Unser HIGH auf `landCoverFrac` löst sich
damit aus zwei Gründen auf, dem korrigierten Tippfehler und der wegfallenden
Werteprüfung. Von uns ist nichts zu tun.

## 2026-09-08: cc-plugin-wcrp#67 merged, and what it exposed

`#67` was merged on 7 September and the local checkout at
`/work/ab0246/a270092/software/cc-plugin-wcrp` was fast-forwarded from
`88c6c45` (21 July) to `8f2574c`. That pulled ten commits and three PRs, not
just `#67`: also `#74` (institution check downgraded) and `#76` (consistency
output refactor). Measured on three cli117 files:

| file | HIGH before | after | |
|---|---:|---:|---|
| `basin` | 1 | 0 | the `#67` restore works |
| `tas` | 0 | 0 | control, unchanged |
| `volo` (`dec`) | 0 | 1 | new `TIME001`, from `dec` entering `AVERAGE_CORRECTION_FREQ` |

Two separate things came out of the `volo` finding.

**Ours, fixed in `99e672dd`.** `_looks_yearly` lumped `dec` in with `yr` and
`yrPt`, so any single-stamp decadal file went through `_create_yearly_bounds`
and a ten-year mean got twelve months of bnds. This was not a consequence of
the one-year test data: a full decade produced the same 365-day bounds. `dec`
now has `_looks_decadal` and `_create_decadal_bounds`.

**Upstream, filed as `cc-plugin-wcrp#80`.** TIME001 and TIME003 cannot both be
satisfied by a decadal file. TIME001 treats the filename token as a period
start and reconstructs the midpoint, TIME003 compares the coordinate against
the filename edges at year precision. Four filename and timestamp variants were
measured, none passes both, and multiple decades per file does not help.

Worth knowing before proposing anything there: `#58`, opened by `sol1105` in
July and still open, argues that TIME003 must read the coordinate rather than
the bounds, citing the CMIP7 Guidance. The obvious fix on the TIME003 side
would revert his own request, which is why `#80` is an issue and not a PR.

## 2026-09-11: Martin's aicc 0.2 run on cli117, fixed for cli118

Fixed on our side in `13bf5f40`, measured on seven cli117 files: longitude
wrapped to [0, 360] (299 files), g132 unified (tauuo and hfx now bitwise
equal), data cast to float32 per DReq `real` (227 files, fixes the hur cf
crash), time2/time4 as CF climatologies, long_names per the table. Checker
wiring in `cb89961d`: our own aicc grid registry via `qc_checker_options`,
because aicc hardcodes g100 to g236 (issue drafted, on hold since 10.09.).

Two new upstream candidates in `ioos/compliance-checker`, both searched, no
existing issue. Not filed yet.

| finding | files | why it is the checker |
|---|---:|---|
| Appendix A: `formula_terms` not allowed on `lev_bnds` (HIGH) | 8 | `cf_1_7.py:304` requires it on the bounds of a parametric coordinate, as CF 1.7 §7.1 does. The two checks contradict each other. |
| §7.4: climatology `cell_methods` format (MEDIUM) | 3 | the regex is anchored at `^time:`, so `area: mean time: maximum within days time: mean over days` fails. `#1144` fixed a different part of that regex. |

`time4` long_name typo `Satistics`: already in `CMIP7_DReq_Content#31`
(`sol1105`), nothing to file.

