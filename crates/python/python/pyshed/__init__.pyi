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
    def terminal_unit_id(self) -> int: ...

    @property
    def input_outlet(self) -> tuple[float, float]: ...

    @property
    def resolved_outlet(self) -> tuple[float, float]: ...

    @property
    def refined_outlet(self) -> tuple[float, float] | None: ...

    @property
    def resolution_method(self) -> str: ...

    @property
    def upstream_unit_ids(self) -> list[int]: ...

    @property
    def upstream_units(self) -> list[DelineationUnitMetadata]: ...

    @property
    def area_km2(self) -> float: ...

    @property
    def geometry_bbox(self) -> tuple[float, float, float, float] | None: ...

    @property
    def geometry_wkb(self) -> bytes: ...

    def to_geojson(self) -> str: ...

    def __repr__(self) -> str: ...


class DelineationUnitMetadata:
    @property
    def id(self) -> int: ...

    @property
    def level(self) -> int: ...

    @property
    def area_km2(self) -> float: ...

    @property
    def up_area_km2(self) -> float | None: ...

    @property
    def outlet(self) -> tuple[float, float]: ...

    def __repr__(self) -> str: ...


class AreaOnlyResult:
    @property
    def terminal_unit_id(self) -> int: ...

    @property
    def input_outlet(self) -> tuple[float, float]: ...

    @property
    def resolved_outlet(self) -> tuple[float, float]: ...

    @property
    def refined_outlet(self) -> tuple[float, float] | None: ...

    @property
    def resolution_method(self) -> str: ...

    @property
    def upstream_unit_ids(self) -> list[int]: ...

    @property
    def area_km2(self) -> float: ...

    def __repr__(self) -> str: ...


class SelectedLevel:
    @property
    def level(self) -> int: ...

    def __repr__(self) -> str: ...


class ResolvedOutlet:
    @property
    def level(self) -> int: ...

    @property
    def terminal_unit_id(self) -> int: ...

    @property
    def input_outlet(self) -> tuple[float, float]: ...

    @property
    def resolved_outlet(self) -> tuple[float, float]: ...

    @property
    def resolution_method(self) -> str: ...

    def __repr__(self) -> str: ...


class UpstreamUnits:
    @property
    def terminal_unit_id(self) -> int: ...

    @property
    def level(self) -> int: ...

    @property
    def unit_ids(self) -> list[int]: ...

    def __repr__(self) -> str: ...


class PreMergeDrainageUnit:
    @property
    def id(self) -> int: ...

    @property
    def level(self) -> int: ...

    @property
    def area_km2(self) -> float: ...

    @property
    def up_area_km2(self) -> float | None: ...

    @property
    def outlet(self) -> tuple[float, float]: ...

    def __repr__(self) -> str: ...


class PreMergeDrainageUnits:
    R3_NOTE: str

    @property
    def terminal_unit_id(self) -> int: ...

    @property
    def level(self) -> int: ...

    @property
    def units(self) -> list[PreMergeDrainageUnit]: ...

    @property
    def unit_geometry_wkb(self) -> list[bytes]: ...

    def __repr__(self) -> str: ...


class TerminalRefinement:
    @property
    def status(self) -> Literal["applied", "best_effort_skipped", "disabled"]: ...

    @property
    def refined_outlet(self) -> tuple[float, float] | None: ...

    def __repr__(self) -> str: ...


class DissolvedWatershed:
    @property
    def area_km2(self) -> float: ...

    @property
    def geometry_wkb(self) -> bytes: ...

    def __repr__(self) -> str: ...


class BasinGeoParquetWriter:
    def __init__(self) -> None: ...

    def write(
        self,
        engine: Engine,
        path: str,
        results: list[DelineationResult],
        *,
        basin_ids: list[str] | None = None,
        method: str | None = None,
        allow_default_basin_id: bool = False,
    ) -> None: ...

    def __repr__(self) -> str: ...


class UnitBundleGeoParquetWriter:
    def __init__(self) -> None: ...

    def write(
        self,
        engine: Engine,
        path: str,
        bundles: list[PreMergeDrainageUnits],
        refinements: list[TerminalRefinement],
        *,
        method: str | None = None,
    ) -> None: ...

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

    def select_level(self) -> SelectedLevel: ...

    def resolve_outlet(
        self, level: SelectedLevel, *, lat: float, lon: float
    ) -> ResolvedOutlet: ...

    def traverse(self, outlet: ResolvedOutlet) -> UpstreamUnits: ...

    def pre_merge_units(self, upstream: UpstreamUnits) -> PreMergeDrainageUnits: ...

    def refine(
        self, outlet: ResolvedOutlet, units: PreMergeDrainageUnits
    ) -> TerminalRefinement: ...

    def dissolve(
        self, units: PreMergeDrainageUnits, refinement: TerminalRefinement
    ) -> DissolvedWatershed: ...

    def compose_result(
        self,
        outlet: ResolvedOutlet,
        upstream: UpstreamUnits,
        units: PreMergeDrainageUnits,
        refinement: TerminalRefinement,
        dissolved: DissolvedWatershed,
    ) -> DelineationResult: ...
