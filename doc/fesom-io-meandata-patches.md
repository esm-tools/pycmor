# FESOM2 io_meandata.F90 patches

Two independent source fixes required to honor io_list frequency requests and
long variable names. File: `fesom-2.7/src/io_meandata.F90`.

## Patch 1 — allow daily/3hr output for utemp/vtemp/usalt/vsalt

The `ldiag_trflx` block unconditionally registers these streams at monthly
frequency (`1, 'm'`), ignoring any io_list entry. Two options:

### Option A (minimal): honor io_list freq/unit if the user listed the var

Scan `io_list` for each name before falling back to monthly default.

```diff
--- a/fesom-2.7/src/io_meandata.F90
+++ b/fesom-2.7/src/io_meandata.F90
@@ -1515,12 +1515,25 @@
     end if
     !___________________________________________________________________________
     ! Tracers flux diagnostics without predefined freq, freq_unit, prec, -->
-    ! default monthly output
+    ! default monthly output; honor io_list entries if present.
     if (ldiag_trflx .and. sel_trgrd_xyz==0) then
-        call def_stream((/nl-1,  elem2D/), (/nl-1, myDim_elem2D/), 'utemp',   'u*temp',           'm/s*°C',     tuv(1,:,:), 1, 'm', i_real8, partit, mesh)
-        call def_stream((/nl-1,  elem2D/), (/nl-1, myDim_elem2D/), 'vtemp',   'v*temp',           'm/s*°C',     tuv(2,:,:), 1, 'm', i_real8, partit, mesh)
-        call def_stream((/nl-1,  elem2D/), (/nl-1, myDim_elem2D/), 'usalt',   'u*salt',           'm/s*psu',   suv(1,:,:), 1, 'm', i_real8, partit, mesh)
-        call def_stream((/nl-1,  elem2D/), (/nl-1, myDim_elem2D/), 'vsalt',   'v*salt',           'm/s*psu',   suv(2,:,:), 1, 'm', i_real8, partit, mesh)
+        call def_trflx_stream('utemp', tuv(1,:,:), 'u*temp', 'm/s*°C')
+        call def_trflx_stream('vtemp', tuv(2,:,:), 'v*temp', 'm/s*°C')
+        call def_trflx_stream('usalt', suv(1,:,:), 'u*salt', 'm/s*psu')
+        call def_trflx_stream('vsalt', suv(2,:,:), 'v*salt', 'm/s*psu')
     end if
```

Plus an internal helper that searches `io_list` for the name and uses its freq
if found, otherwise defaults to `1, 'm'`. Pseudocode:

```fortran
subroutine def_trflx_stream(name, arr, longname, units)
    character(len=*), intent(in) :: name, longname, units
    real(real8), intent(in) :: arr(:,:)
    integer :: k, f
    character :: u
    f = 1; u = 'm'              ! default monthly
    do k = 1, size(io_list)
        if (trim(io_list(k)%id) == trim(name)) then
            f = io_list(k)%freq
            u = io_list(k)%unit
            exit
        end if
    end do
    call def_stream((/nl-1, elem2D/), (/nl-1, myDim_elem2D/), &
                    name, longname, units, arr, f, u, i_real8, partit, mesh)
end subroutine
```

### Option B (cleaner): move utemp/vtemp/usalt/vsalt into the CASE dispatcher

Register them in the `select case` block alongside `osalttend`,
`opottempdiff`, etc., gated on `ldiag_trflx`. This removes the auto-registration
block entirely and makes them behave like every other diagnostic — they only
appear if explicitly listed in `io_list`, and their frequency comes from the
list entry.

Preferred for consistency; more invasive.

## Patch 2 — extend io_list id length

`opottemprmadvect` is 16 characters; `io_entry%id` is 15. Names are truncated
on read, so the CASE match fails.

```diff
--- a/fesom-2.7/src/io_meandata.F90
+++ b/fesom-2.7/src/io_meandata.F90
@@ -89,7 +89,7 @@
   character(len=1), save         :: filesplit_freq='y'
   integer, save                  :: compression_level=0
   type io_entry
-        CHARACTER(len=15)        :: id        ='unknown   '
+        CHARACTER(len=20)        :: id        ='unknown             '
         INTEGER                  :: freq      =0
         CHARACTER                :: unit      =''
         INTEGER                  :: precision =0
```

Also audit the namelist parser that reads `io_list` entries — typical pattern
is a fixed-width read. If a read-format string specifies `A10` or `A15`,
extend it to `A20`. Search for the `read` that populates `io_list(i)%id`
(commonly via `namelist /nml_list/` with type-derived I/O, which should pick up
the new length automatically).

After this patch, namelist entries should be padded to 20 chars, e.g.:

```
'opottemprmadvect    ', 1, 'm', 8,
```

## Testing

After applying, re-run with:
- `utemp`, `vtemp` at `1, 'd'` in io_list → should produce 365 timesteps.
- `opottemprmadvect`, `opottempdiff`, `osalttend`, `osaltrmadvect`, `osaltdiff`
  at `1, 'm'` → should each produce 12-timestep output files.
