"""
ispac_extractor.py
==================
Extract .dtsx files and Project.params from an .ispac archive.
An .ispac is a standard ZIP file produced by SSMS "Export" or VS build.
"""
from __future__ import annotations

import zipfile
from pathlib import Path

from sql_migration.core.logger import get_logger

log = get_logger("parser.ispac")


def extract_ispac(ispac_path: str, output_dir: str) -> dict:
    """
    Unzip an .ispac file and return paths to extracted contents.

    Returns:
        {
            "dtsx_paths": ["/output/Package1.dtsx", ...],
            "params_path": "/output/Project.params" or "",
            "manifest_path": "/output/@Project.manifest" or "",
        }
    """
    ispac = Path(ispac_path)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    if not ispac.exists():
        raise FileNotFoundError(f"ISPAC file not found: {ispac_path}")

    if not zipfile.is_zipfile(str(ispac)):
        raise ValueError(
            f"File is not a valid ZIP/ISPAC archive: {ispac_path}. "
            f"If the package is encrypted with EncryptAllWithPassword, "
            f"please decrypt it first and re-upload."
        )

    dtsx_paths: list[str] = []
    params_path = ""
    manifest_path = ""

    with zipfile.ZipFile(str(ispac), "r") as zf:
        for member in zf.namelist():
            zf.extract(member, str(out))
            extracted = str(out / member)

            if member.lower().endswith(".dtsx"):
                dtsx_paths.append(extracted)
            elif member.lower() == "project.params":
                params_path = extracted
            elif member.lower() == "@project.manifest":
                manifest_path = extracted

    log.info("ispac_extracted",
             ispac=ispac.name,
             dtsx_count=len(dtsx_paths),
             has_params=bool(params_path))

    if not dtsx_paths:
        raise ValueError(
            f"No .dtsx files found in ISPAC archive: {ispac_path}")

    return {
        "dtsx_paths": sorted(dtsx_paths),
        "params_path": params_path,
        "manifest_path": manifest_path,
    }