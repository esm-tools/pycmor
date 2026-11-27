"""Keep CMIP7 files in a direct DRS tree and execution evidence in attempts/."""
from datetime import datetime
import fcntl
import json
import os
from pathlib import Path
import re


def drs_relative(path, staging):
    """Validate the directory/filename structure without altering its metadata."""
    relative = Path(path).relative_to(staging)
    parts = relative.parts
    if (len(parts) != 14 or parts[:2] != ('MIP-DRS7', 'CMIP7')
            or '..' in parts or not re.fullmatch(r'v\d{8}', parts[-2])
            or not parts[-1].endswith('.nc')):
        raise ValueError(f'Unexpected CMIP7 DRS path: {relative}')
    datetime.strptime(parts[-2][1:], '%Y%m%d')
    prefix = '_'.join(parts[i] for i in (9, 10, 8, 7, 11, 4, 5, 6))
    if not (parts[-1] == prefix + '.nc' or parts[-1].startswith(prefix + '_')):
        raise ValueError(f'Filename disagrees with its DRS directories: {relative}')
    return relative


def relocate_references(value, files, prefixes=()):
    """Update JSON path values and keys, preserving all recorded check results."""
    if isinstance(value, str):
        if value in files:
            return files[value]
        for old, new in prefixes:
            if value == old or value.startswith(old + '/'):
                relocated = new + value[len(old):]
                return files.get(relocated, relocated)
        return value
    if isinstance(value, list):
        return [relocate_references(v, files, prefixes) for v in value]
    if isinstance(value, dict):
        return {relocate_references(k, files, prefixes):
                relocate_references(v, files, prefixes) for k, v in value.items()}
    return value


def write_json(path, value):
    temporary = path.with_name(path.name + f'.{os.getpid()}.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def promote(campaign, attempt, validation, prefixes=()):
    """Move validated files into <campaign>/MIP-DRS7, refusing any overwrite.

    Conversions and their QC run in an isolated attempt first. A same-filesystem
    rename preserves every NetCDF byte, inode and recorded size/mtime. The lock
    serializes the final collision check across concurrent conversion workers.
    """
    campaign, attempt = Path(campaign), Path(attempt)
    pairs = [(Path(p), campaign / drs_relative(p, attempt))
             for p in validation['files']]
    if not pairs or len({target for _, target in pairs}) != len(pairs):
        raise ValueError('Missing or duplicate output paths')
    mapping = {str(source): str(target) for source, target in pairs}
    sidecars = sorted(attempt.glob('qc_*.json'))
    if (attempt / 'source_validation.json').is_file():
        sidecars.append(attempt / 'source_validation.json')
    originals = {p: p.read_bytes() for p in sidecars}
    replacements = {p: relocate_references(json.loads(data), mapping, prefixes)
                    for p, data in originals.items()}
    moved = []
    with (campaign / '.output_layout.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        for source, target in pairs:
            stat = source.stat()
            if [stat.st_size, stat.st_mtime_ns] != validation['output_state'][str(source)]:
                raise ValueError(f'Output changed after validation: {source}')
            if target.exists() or target.is_symlink():
                raise FileExistsError(f'Refusing to overwrite CMIP output: {target}')
            if stat.st_dev != campaign.stat().st_dev:
                raise ValueError('CMIP output and attempts must share a filesystem')
        try:
            for source, target in pairs:
                target.parent.mkdir(parents=True, exist_ok=True)
                source.rename(target)
                moved.append((source, target))
            for path, value in replacements.items():
                write_json(path, value)
            write_json(attempt / 'output_layout.json', dict(
                data_directory=str(campaign / 'MIP-DRS7'), files=mapping))
        except Exception:
            for path, content in originals.items():
                path.write_bytes(content)
            for source, target in reversed(moved):
                target.rename(source)
            raise
    for source, _ in pairs:
        parent = source.parent
        while parent != attempt:
            try:
                parent.rmdir()
            except OSError:
                break
            parent = parent.parent
    return relocate_references(validation, mapping)
