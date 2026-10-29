# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */
"""Shared helpers for the geo components.

`zalfmas_common.geo.name_to_struct_type` lowercases the name when matching `2d`, `xy` and `latlon`
but compares the raw string for `wgs84`, `gk*` and `utm*`, so the capitalised names the components
document - WGS84, GK5, UTM32N - do not resolve. Normalising here fixes every documented form without
needing a change upstream.
"""

from __future__ import annotations

from typing import Any, Final

from mas.schema.geo import geo_capnp
from zalfmas_common import geo

#: Content type per coordinate struct, so an emitted coordinate can be read by type-driven
#: components downstream instead of needing its type configured again.
CONTENT_TYPES: Final[dict[int, str]] = {
    geo_capnp.LatLonCoord.schema.node.id: "@0xecf1fc3039cc8ffb = geo/geo.capnp:LatLonCoord",
    geo_capnp.UTMCoord.schema.node.id: "@0xeb1acd255e40f049 = geo/geo.capnp:UTMCoord",
    geo_capnp.GKCoord.schema.node.id: "@0x97ff7d61786091ae = geo/geo.capnp:GKCoord",
    geo_capnp.Point2D.schema.node.id: "@0xc88fb91c1e6986e2 = geo/geo.capnp:Point2D",
}


def struct_type_for(name: str) -> Any | None:
    """The coordinate struct type a name refers to, case-insensitively."""
    return geo.name_to_struct_type(name.lower())


def struct_instance_for(name: str) -> Any | None:
    """A new coordinate of the type a name refers to, case-insensitively."""
    return geo.name_to_struct_instance(name.lower())


def content_type_of(coord: Any) -> str | None:
    """The content type string for a coordinate struct, or None if it is not one."""
    schema = getattr(coord, "schema", None)
    node = getattr(schema, "node", None)
    return CONTENT_TYPES.get(node.id) if node is not None else None
