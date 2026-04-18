from __future__ import annotations

from typing import Literal, TypedDict

__version__: str


class _Outlet(TypedDict):
    lat: float
    lon: float


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
    def geometry_wkb(self) -> bytes: ...

    def to_geojson(self) -> str: ...

    def __repr__(self) -> str: ...


class Engine:
    def __init__(
        self,
        dataset_path: str,
        *,
        snap_radius: float | None = ...,
        snap_strategy: Literal["distance-first", "weight-first"] | None = ...,
        snap_threshold: int | None = ...,
        clean_epsilon: float | None = ...,
        refine: bool = ...,
    ) -> None: ...

    def delineate(self, *, lat: float, lon: float) -> DelineationResult: ...

    def delineate_batch(self, outlets: list[_Outlet]) -> list[DelineationResult]: ...
