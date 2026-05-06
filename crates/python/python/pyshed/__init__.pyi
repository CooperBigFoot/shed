from __future__ import annotations

from typing import Callable, Literal, Mapping, TypedDict, overload

__version__: str


def set_log_level(level: str) -> None: ...


class _Outlet(TypedDict):
    lat: float
    lon: float


class ProgressEvent(TypedDict, total=False):
    index: int
    total: int
    lat: float
    lon: float
    duration_ms: int
    status: str  # "ok" | "error"
    n_catchments: int  # only on success
    error: str  # only on failure


ProgressCallback = Callable[[ProgressEvent], None]


class ShedError(Exception): ...


class DatasetError(ShedError): ...


class ResolutionError(ShedError): ...


class AssemblyError(ShedError): ...


class DelineationResult:
    @property
    def terminal_atom_id(self) -> int: ...

    @property
    def input_outlet(self) -> tuple[float, float]: ...

    @property
    def resolved_outlet(self) -> tuple[float, float]: ...

    @property
    def refined_outlet(self) -> tuple[float, float] | None: ...

    @property
    def resolution_method(self) -> str: ...

    @property
    def upstream_atom_ids(self) -> list[int]: ...

    @property
    def area_km2(self) -> float: ...

    @property
    def geometry_bbox(self) -> tuple[float, float, float, float] | None: ...

    @property
    def geometry_wkb(self) -> bytes: ...

    def to_geojson(self) -> str: ...

    def __repr__(self) -> str: ...


class AreaOnlyResult:
    @property
    def terminal_atom_id(self) -> int: ...

    @property
    def input_outlet(self) -> tuple[float, float]: ...

    @property
    def resolved_outlet(self) -> tuple[float, float]: ...

    @property
    def refined_outlet(self) -> tuple[float, float] | None: ...

    @property
    def resolution_method(self) -> str: ...

    @property
    def upstream_atom_ids(self) -> list[int]: ...

    @property
    def area_km2(self) -> float: ...

    def __repr__(self) -> str: ...


class Engine:
    def __init__(
        self,
        dataset_path: str,
        *,
        snap_radius: float | None = None,
        snap_strategy: Literal["distance-first", "weight-first"] | None = None,
        snap_threshold: int | None = None,
        clean_epsilon: float | None = None,
        refine: bool = True,
        repair_geometry: Literal["auto", "gdal", "clean"] | Literal[False] | None = "auto",
        parquet_cache: bool | None = None,
        parquet_cache_max_mb: int = 512,
    ) -> None: ...

    @overload
    def delineate(
        self, *, lat: float, lon: float, geometry: Literal[True] = ...
    ) -> DelineationResult: ...

    @overload
    def delineate(
        self, *, lat: float, lon: float, geometry: Literal[False]
    ) -> AreaOnlyResult: ...

    @overload
    def delineate(
        self, *, lat: float, lon: float, geometry: bool
    ) -> DelineationResult | AreaOnlyResult: ...

    # Passing progress disables Rayon parallelism and runs the batch
    # sequentially to preserve monotonic callback order.
    def delineate_batch(
        self,
        outlets: list[_Outlet],
        *,
        progress: ProgressCallback | None = None,
    ) -> list[DelineationResult]: ...
