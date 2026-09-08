from __future__ import annotations

import os

# Some hosts export PROJ_LIB/PROJ_DATA pointing at an unrelated PROJ install (e.g. a
# system-wide conda base environment) that is incompatible with the PROJ version bundled inside
# this project's own rasterio/pyproj wheels, making CRS lookups fail with e.g. "PROJ: ... lacks
# DATABASE.LAYOUT.VERSION.MAJOR / MINOR metadata. It comes from another PROJ installation."
# GDAL/PROJ reads these once, when rasterio/pyproj's C extension first initializes its PROJ
# context - which happens as soon as this package (or anything importing it) is imported, well
# before any per-test fixture would run - so this has to happen here, at conftest module import
# time (pytest loads conftest.py before collecting sibling test modules), rather than in a
# fixture. Clearing both lets each library fall back to its own bundled proj.db.
os.environ.pop("PROJ_LIB", None)
os.environ.pop("PROJ_DATA", None)
