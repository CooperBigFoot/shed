# Raster Cache

Remote raster refinement has two cache paths:

| Path | Status | Behavior |
|---|---|---|
| `rasters/` | R2 stopgap | Stores a complete remote raster after `RemoteRasterCache::get_or_fetch`. This API remains for explicit callers but is not used by default engine refinement. |
| `raster-windows/` | R3 default | Stores a small local GeoTIFF for the terminal catchment bbox. The cache key includes the remote path hash, raster kind, adapter version, and planned pixel window. |

For R3, remote MERIT-Basins rasters are treated as COGs. The engine reads the
TIFF header/IFD and tile offset arrays, plans the tiles intersecting the
terminal catchment bbox with a one-pixel pad, fetches only those compressed tile
ranges through `object_store`, decodes the selected tiles locally, and writes an
uncompressed cache-local GeoTIFF. `shed-gdal` then opens that local file through
the existing refinement code.

Unsupported remote TIFF layouts fail loudly rather than falling back to a
multi-GB full download. The first supported layout is the published MERIT
variant: one-band, 512x512 tiled COGs, `u8` flow direction and `f32` flow
accumulation, with GeoTIFF scale/tiepoint metadata in EPSG:4326.

The Phase 4 smoke script reports COG reads separately:

| Field | Meaning |
|---|---|
| `cog_header_bytes` | TIFF header/IFD bytes read to plan windows |
| `cog_tile_bytes` | Compressed tile-range bytes read for raster windows |
| `cog_tile_count` | Number of selected COG tiles |
| `cog_window_pixels` | Pixels in the materialized local windows |
| `non_cog_bytes_on_wire` | Remote bytes attributed to manifest, graph, parquet, and other non-COG reads |
