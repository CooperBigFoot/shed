//! Windowed COG reads for remote raster refinement.

use std::cmp::{max, min};
use std::fs::File;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use geo::Rect;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tempfile::NamedTempFile;
use tiff::decoder::{Decoder, DecodingResult};
use tiff::encoder::{TiffEncoder, colortype};
use tiff::tags::Tag;
use tracing::debug;

use crate::error::CacheError;
use crate::session::RasterKind;

const HEADER_RANGE_BYTES: u64 = 16 * 1024 * 1024;
const MODEL_PIXEL_SCALE_TAG: Tag = Tag::ModelPixelScaleTag;
const MODEL_TIEPOINT_TAG: Tag = Tag::ModelTiepointTag;
const GEO_KEY_DIRECTORY_TAG: Tag = Tag::GeoKeyDirectoryTag;
const GDAL_NODATA_TAG: Tag = Tag::GdalNodata;

/// A geographic raster window request.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RasterWindowRequest {
    kind: RasterKind,
    bbox: Rect<f64>,
}

impl RasterWindowRequest {
    /// Create a request for `kind` intersecting `bbox`.
    pub(crate) fn new(kind: RasterKind, bbox: Rect<f64>) -> Self {
        Self { kind, bbox }
    }

    pub(crate) fn kind(&self) -> RasterKind {
        self.kind
    }
}

/// A local GeoTIFF window and the remote bytes used to produce it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LocalizedRasterWindow {
    path: PathBuf,
    header_bytes: u64,
    tile_bytes: u64,
    tile_count: usize,
    window_pixels: u64,
}

impl LocalizedRasterWindow {
    pub(crate) fn cached(path: PathBuf) -> Self {
        Self {
            path,
            header_bytes: 0,
            tile_bytes: 0,
            tile_count: 0,
            window_pixels: 0,
        }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn header_bytes(&self) -> u64 {
        self.header_bytes
    }

    pub(crate) fn tile_bytes(&self) -> u64 {
        self.tile_bytes
    }

    pub(crate) fn tile_count(&self) -> usize {
        self.tile_count
    }

    pub(crate) fn window_pixels(&self) -> u64 {
        self.window_pixels
    }
}

/// Supported one-band MERIT sample layouts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CogSampleType {
    U8,
    F32,
}

/// Metadata needed to plan and materialize a COG window.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CogMetadata {
    width: u32,
    height: u32,
    tile_width: u32,
    tile_height: u32,
    origin_x: f64,
    origin_y: f64,
    pixel_width: f64,
    pixel_height: f64,
    nodata: String,
    sample_type: CogSampleType,
    compression: u16,
    predictor: u16,
    tile_offsets: Vec<u64>,
    tile_byte_counts: Vec<u64>,
}

impl CogMetadata {
    fn tiles_across(&self) -> u32 {
        self.width.div_ceil(self.tile_width)
    }

    fn tiles_down(&self) -> u32 {
        self.height.div_ceil(self.tile_height)
    }
}

/// Pixel-space raster window, half-open in both dimensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RasterPixelWindow {
    col_off: u32,
    row_off: u32,
    width: u32,
    height: u32,
}

impl RasterPixelWindow {
    pub(crate) fn from_bbox(metadata: &CogMetadata, bbox: &Rect<f64>) -> Result<Self, String> {
        if metadata.pixel_width <= 0.0 || metadata.pixel_height >= 0.0 {
            return Err(
                "only north-up rasters with positive x and negative y pixels are supported"
                    .to_string(),
            );
        }

        let min_col =
            ((bbox.min().x - metadata.origin_x) / metadata.pixel_width).floor() as i64 - 1;
        let max_col = ((bbox.max().x - metadata.origin_x) / metadata.pixel_width).ceil() as i64 + 1;
        let min_row =
            ((bbox.max().y - metadata.origin_y) / metadata.pixel_height).floor() as i64 - 1;
        let max_row =
            ((bbox.min().y - metadata.origin_y) / metadata.pixel_height).ceil() as i64 + 1;

        let col_off = min_col.clamp(0, metadata.width as i64) as u32;
        let row_off = min_row.clamp(0, metadata.height as i64) as u32;
        let col_end = max_col.clamp(0, metadata.width as i64) as u32;
        let row_end = max_row.clamp(0, metadata.height as i64) as u32;

        let width = col_end.saturating_sub(col_off);
        let height = row_end.saturating_sub(row_off);
        if width == 0 || height == 0 {
            return Err("requested bbox does not intersect raster extent".to_string());
        }

        Ok(Self {
            col_off,
            row_off,
            width,
            height,
        })
    }

    pub(crate) fn cache_fragment(&self) -> String {
        format!(
            "x{}-y{}-w{}-h{}",
            self.col_off, self.row_off, self.width, self.height
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedTile {
    index: u32,
    range: Range<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TilePlan {
    tiles: Vec<PlannedTile>,
}

impl TilePlan {
    pub(crate) fn for_window(metadata: &CogMetadata, window: RasterPixelWindow) -> Self {
        let first_tile_col = window.col_off / metadata.tile_width;
        let last_tile_col = (window.col_off + window.width - 1) / metadata.tile_width;
        let first_tile_row = window.row_off / metadata.tile_height;
        let last_tile_row = (window.row_off + window.height - 1) / metadata.tile_height;
        let tiles_across = metadata.tiles_across();

        let tiles = (first_tile_row..=last_tile_row)
            .flat_map(|tile_row| {
                (first_tile_col..=last_tile_col).map(move |tile_col| {
                    let index = tile_row * tiles_across + tile_col;
                    let offset = metadata.tile_offsets[index as usize];
                    let byte_count = metadata.tile_byte_counts[index as usize];
                    PlannedTile {
                        index,
                        range: offset..offset + byte_count,
                    }
                })
            })
            .collect();

        Self { tiles }
    }

    pub(crate) fn ranges(&self) -> Vec<Range<u64>> {
        self.tiles.iter().map(|tile| tile.range.clone()).collect()
    }

    pub(crate) fn byte_count(&self) -> u64 {
        self.tiles
            .iter()
            .map(|tile| tile.range.end - tile.range.start)
            .sum()
    }
}

/// Header-derived plan for a remote COG window.
#[derive(Debug, Clone)]
pub(crate) struct PreparedCogWindow {
    object_size: u64,
    header_end: u64,
    header: Bytes,
    metadata: CogMetadata,
    window: RasterPixelWindow,
    plan: TilePlan,
}

impl PreparedCogWindow {
    pub(crate) fn cache_fragment(&self) -> String {
        self.window.cache_fragment()
    }
}

/// Read COG metadata and plan the intersecting tile byte ranges.
pub(crate) async fn prepare_window(
    store: &dyn ObjectStore,
    remote_path: &ObjectPath,
    request: &RasterWindowRequest,
) -> Result<PreparedCogWindow, CacheError> {
    let object_meta = store
        .head(remote_path)
        .await
        .map_err(|source| CacheError::ObjectStore {
            path: remote_path.clone(),
            source,
        })?;
    let object_size = object_meta.size as u64;
    let header_end = min(HEADER_RANGE_BYTES, object_size);
    let header = store
        .get_range(remote_path, 0..header_end)
        .await
        .map_err(|source| CacheError::ObjectStore {
            path: remote_path.clone(),
            source,
        })?;

    let reader = RangeBackedTiffReader::new(object_size, vec![(0..header_end, header.clone())]);
    let metadata = read_metadata(reader, remote_path)?;
    validate_merit_layout(&metadata, request.kind(), remote_path)?;
    let window = RasterPixelWindow::from_bbox(&metadata, &request.bbox).map_err(|reason| {
        CacheError::UnsupportedCog {
            path: remote_path.clone(),
            reason,
        }
    })?;
    let plan = TilePlan::for_window(&metadata, window);
    Ok(PreparedCogWindow {
        object_size,
        header_end,
        header,
        metadata,
        window,
        plan,
    })
}

/// Read and materialize a planned remote COG window into `canonical`.
pub(crate) async fn fetch_window_to_path(
    store: &dyn ObjectStore,
    remote_path: &ObjectPath,
    prepared: PreparedCogWindow,
    canonical: &Path,
) -> Result<LocalizedRasterWindow, CacheError> {
    let ranges = prepared.plan.ranges();
    let tile_bytes = store
        .get_ranges(remote_path, &ranges)
        .await
        .map_err(|source| CacheError::ObjectStore {
            path: remote_path.clone(),
            source,
        })?;
    let mut backed_ranges = Vec::with_capacity(ranges.len() + 1);
    backed_ranges.push((0..prepared.header_end, prepared.header));
    backed_ranges.extend(ranges.into_iter().zip(tile_bytes));

    let reader = RangeBackedTiffReader::new(prepared.object_size, backed_ranges);
    let window_data = decode_window(
        reader,
        &prepared.metadata,
        prepared.window,
        &prepared.plan,
        remote_path,
    )?;
    write_window_geotiff(
        canonical,
        &prepared.metadata,
        prepared.window,
        &window_data,
        remote_path,
    )?;

    let stats = LocalizedRasterWindow {
        path: canonical.to_path_buf(),
        header_bytes: prepared.header_end,
        tile_bytes: prepared.plan.byte_count(),
        tile_count: prepared.plan.tiles.len(),
        window_pixels: u64::from(prepared.window.width) * u64::from(prepared.window.height),
    };
    debug!(
        path = %canonical.display(),
        cog_header_bytes = stats.header_bytes,
        cog_tile_bytes = stats.tile_bytes,
        cog_tile_count = stats.tile_count,
        window_pixels = stats.window_pixels,
        "materialized remote COG window"
    );
    Ok(stats)
}

fn read_metadata(
    reader: RangeBackedTiffReader,
    remote_path: &ObjectPath,
) -> Result<CogMetadata, CacheError> {
    let mut decoder = Decoder::new(reader).map_err(|source| CacheError::Tiff {
        path: remote_path.as_ref().to_string(),
        source,
    })?;

    let (width, height) = decoder.dimensions().map_err(|source| CacheError::Tiff {
        path: remote_path.as_ref().to_string(),
        source,
    })?;
    let (tile_width, tile_height) = decoder.chunk_dimensions();
    let color_type = decoder.colortype().map_err(|source| CacheError::Tiff {
        path: remote_path.as_ref().to_string(),
        source,
    })?;
    let sample_formats = decoder
        .find_tag_unsigned_vec::<u16>(Tag::SampleFormat)
        .map_err(|source| CacheError::Tiff {
            path: remote_path.as_ref().to_string(),
            source,
        })?
        .unwrap_or_else(|| vec![1]);
    let sample_type = match (color_type, sample_formats.as_slice()) {
        (tiff::ColorType::Gray(8), [1]) => CogSampleType::U8,
        (tiff::ColorType::Gray(32), [3]) => CogSampleType::F32,
        (other, formats) => {
            return Err(CacheError::UnsupportedCog {
                path: remote_path.clone(),
                reason: format!("unsupported sample layout: {other:?} sample_format={formats:?}"),
            });
        }
    };
    let compression = decoder
        .find_tag_unsigned::<u16>(Tag::Compression)
        .map_err(|source| CacheError::Tiff {
            path: remote_path.as_ref().to_string(),
            source,
        })?
        .unwrap_or(1);
    let predictor = decoder
        .find_tag_unsigned::<u16>(Tag::Predictor)
        .map_err(|source| CacheError::Tiff {
            path: remote_path.as_ref().to_string(),
            source,
        })?
        .unwrap_or(1);

    let scale = decoder
        .get_tag_f64_vec(MODEL_PIXEL_SCALE_TAG)
        .map_err(|source| CacheError::Tiff {
            path: remote_path.as_ref().to_string(),
            source,
        })?;
    let tiepoint = decoder
        .get_tag_f64_vec(MODEL_TIEPOINT_TAG)
        .map_err(|source| CacheError::Tiff {
            path: remote_path.as_ref().to_string(),
            source,
        })?;
    if scale.len() < 2 || tiepoint.len() < 6 {
        return Err(CacheError::UnsupportedCog {
            path: remote_path.clone(),
            reason: "missing GeoTIFF model scale or tiepoint values".to_string(),
        });
    }

    let tile_offsets = decoder
        .find_tag_unsigned_vec::<u64>(Tag::TileOffsets)
        .map_err(|source| CacheError::Tiff {
            path: remote_path.as_ref().to_string(),
            source,
        })?
        .ok_or_else(|| CacheError::UnsupportedCog {
            path: remote_path.clone(),
            reason: "missing TileOffsets tag".to_string(),
        })?;
    let tile_byte_counts = decoder
        .find_tag_unsigned_vec::<u64>(Tag::TileByteCounts)
        .map_err(|source| CacheError::Tiff {
            path: remote_path.as_ref().to_string(),
            source,
        })?
        .ok_or_else(|| CacheError::UnsupportedCog {
            path: remote_path.clone(),
            reason: "missing TileByteCounts tag".to_string(),
        })?;
    let nodata = decoder
        .get_tag_ascii_string(GDAL_NODATA_TAG)
        .unwrap_or_else(|_| match sample_type {
            CogSampleType::U8 => "255".to_string(),
            CogSampleType::F32 => "-1".to_string(),
        });

    let origin_x = tiepoint[3] - tiepoint[0] * scale[0];
    let origin_y = tiepoint[4] + tiepoint[1] * scale[1];

    Ok(CogMetadata {
        width,
        height,
        tile_width,
        tile_height,
        origin_x,
        origin_y,
        pixel_width: scale[0],
        pixel_height: -scale[1],
        nodata,
        sample_type,
        compression,
        predictor,
        tile_offsets,
        tile_byte_counts,
    })
}

fn validate_merit_layout(
    metadata: &CogMetadata,
    kind: RasterKind,
    remote_path: &ObjectPath,
) -> Result<(), CacheError> {
    if metadata.tile_width != 512 || metadata.tile_height != 512 {
        return Err(CacheError::UnsupportedCog {
            path: remote_path.clone(),
            reason: format!(
                "expected 512x512 tiled COG, got {}x{}",
                metadata.tile_width, metadata.tile_height
            ),
        });
    }
    let expected_tiles = metadata.tiles_across() as usize * metadata.tiles_down() as usize;
    if metadata.tile_offsets.len() != expected_tiles
        || metadata.tile_byte_counts.len() != expected_tiles
    {
        return Err(CacheError::UnsupportedCog {
            path: remote_path.clone(),
            reason: "tile offset/count arrays do not match raster dimensions".to_string(),
        });
    }
    let expected_sample = match kind {
        RasterKind::FlowDir => CogSampleType::U8,
        RasterKind::FlowAcc => CogSampleType::F32,
    };
    if metadata.sample_type != expected_sample {
        return Err(CacheError::UnsupportedCog {
            path: remote_path.clone(),
            reason: format!(
                "{kind:?} expected {expected_sample:?} samples, got {:?}",
                metadata.sample_type
            ),
        });
    }
    if !matches!(metadata.compression, 8 | 32946) {
        return Err(CacheError::UnsupportedCog {
            path: remote_path.clone(),
            reason: format!("expected DEFLATE compression, got {}", metadata.compression),
        });
    }
    let expected_predictor = match kind {
        RasterKind::FlowDir => 2,
        RasterKind::FlowAcc => 3,
    };
    if metadata.predictor != expected_predictor {
        return Err(CacheError::UnsupportedCog {
            path: remote_path.clone(),
            reason: format!(
                "{kind:?} expected TIFF predictor {expected_predictor}, got {}",
                metadata.predictor
            ),
        });
    }
    Ok(())
}

fn decode_window(
    reader: RangeBackedTiffReader,
    metadata: &CogMetadata,
    window: RasterPixelWindow,
    plan: &TilePlan,
    remote_path: &ObjectPath,
) -> Result<WindowData, CacheError> {
    let mut decoder = Decoder::new(reader).map_err(|source| CacheError::Tiff {
        path: remote_path.as_ref().to_string(),
        source,
    })?;

    match metadata.sample_type {
        CogSampleType::U8 => {
            let mut out = vec![0_u8; window.width as usize * window.height as usize];
            for tile in &plan.tiles {
                let decoded =
                    decoder
                        .read_chunk(tile.index)
                        .map_err(|source| CacheError::Tiff {
                            path: remote_path.as_ref().to_string(),
                            source,
                        })?;
                let DecodingResult::U8(data) = decoded else {
                    return Err(CacheError::UnsupportedCog {
                        path: remote_path.clone(),
                        reason: "decoded flow_dir tile was not u8".to_string(),
                    });
                };
                copy_tile_u8(&data, &mut out, metadata, window, tile.index);
            }
            Ok(WindowData::U8(out))
        }
        CogSampleType::F32 => {
            let nodata = metadata.nodata.parse::<f32>().ok();
            let mut out =
                vec![nodata.unwrap_or(f32::NAN); window.width as usize * window.height as usize];
            for tile in &plan.tiles {
                let decoded =
                    decoder
                        .read_chunk(tile.index)
                        .map_err(|source| CacheError::Tiff {
                            path: remote_path.as_ref().to_string(),
                            source,
                        })?;
                let DecodingResult::F32(data) = decoded else {
                    return Err(CacheError::UnsupportedCog {
                        path: remote_path.clone(),
                        reason: "decoded flow_acc tile was not f32".to_string(),
                    });
                };
                copy_tile_f32(&data, &mut out, metadata, window, tile.index);
            }
            Ok(WindowData::F32(out))
        }
    }
}

fn copy_tile_u8(
    tile_data: &[u8],
    out: &mut [u8],
    metadata: &CogMetadata,
    window: RasterPixelWindow,
    tile_index: u32,
) {
    let (tile_col, tile_row) = tile_col_row(metadata, tile_index);
    let (src_width, _src_height, dst_col, dst_row, copy_width, copy_height) =
        tile_copy_span(metadata, window, tile_col, tile_row);

    for row in 0..copy_height {
        let src_start = ((dst_row + row - tile_row * metadata.tile_height) * src_width
            + (dst_col - tile_col * metadata.tile_width)) as usize;
        let dst_start =
            ((dst_row + row - window.row_off) * window.width + (dst_col - window.col_off)) as usize;
        out[dst_start..dst_start + copy_width as usize]
            .copy_from_slice(&tile_data[src_start..src_start + copy_width as usize]);
    }
}

fn copy_tile_f32(
    tile_data: &[f32],
    out: &mut [f32],
    metadata: &CogMetadata,
    window: RasterPixelWindow,
    tile_index: u32,
) {
    let (tile_col, tile_row) = tile_col_row(metadata, tile_index);
    let (src_width, _src_height, dst_col, dst_row, copy_width, copy_height) =
        tile_copy_span(metadata, window, tile_col, tile_row);

    for row in 0..copy_height {
        let src_start = ((dst_row + row - tile_row * metadata.tile_height) * src_width
            + (dst_col - tile_col * metadata.tile_width)) as usize;
        let dst_start =
            ((dst_row + row - window.row_off) * window.width + (dst_col - window.col_off)) as usize;
        out[dst_start..dst_start + copy_width as usize]
            .copy_from_slice(&tile_data[src_start..src_start + copy_width as usize]);
    }
}

fn tile_col_row(metadata: &CogMetadata, tile_index: u32) -> (u32, u32) {
    let tiles_across = metadata.tiles_across();
    (tile_index % tiles_across, tile_index / tiles_across)
}

fn tile_copy_span(
    metadata: &CogMetadata,
    window: RasterPixelWindow,
    tile_col: u32,
    tile_row: u32,
) -> (u32, u32, u32, u32, u32, u32) {
    let tile_x = tile_col * metadata.tile_width;
    let tile_y = tile_row * metadata.tile_height;
    let src_width = min(metadata.tile_width, metadata.width - tile_x);
    let src_height = min(metadata.tile_height, metadata.height - tile_y);
    let dst_col = max(window.col_off, tile_x);
    let dst_row = max(window.row_off, tile_y);
    let copy_end_col = min(window.col_off + window.width, tile_x + src_width);
    let copy_end_row = min(window.row_off + window.height, tile_y + src_height);
    (
        src_width,
        src_height,
        dst_col,
        dst_row,
        copy_end_col - dst_col,
        copy_end_row - dst_row,
    )
}

enum WindowData {
    U8(Vec<u8>),
    F32(Vec<f32>),
}

fn write_window_geotiff(
    canonical: &Path,
    metadata: &CogMetadata,
    window: RasterPixelWindow,
    data: &WindowData,
    remote_path: &ObjectPath,
) -> Result<(), CacheError> {
    let parent = canonical.parent().ok_or_else(|| CacheError::Io {
        op: "parent",
        path: canonical.to_path_buf(),
        source: std::io::Error::new(ErrorKind::InvalidInput, "cache path has no parent"),
    })?;
    std::fs::create_dir_all(parent).map_err(|source| CacheError::Io {
        op: "create_dir_all",
        path: parent.to_path_buf(),
        source,
    })?;
    let mut temp = NamedTempFile::new_in(parent).map_err(|source| CacheError::Io {
        op: "create_temp",
        path: parent.to_path_buf(),
        source,
    })?;
    let temp_path = temp.path().to_path_buf();
    {
        let file = temp.as_file_mut();
        match data {
            WindowData::U8(values) => {
                write_tiff_image::<colortype::Gray8>(file, metadata, window, values, remote_path)?
            }
            WindowData::F32(values) => write_tiff_image::<colortype::Gray32Float>(
                file,
                metadata,
                window,
                values,
                remote_path,
            )?,
        }
        file.flush().map_err(|source| CacheError::Io {
            op: "flush",
            path: temp_path.clone(),
            source,
        })?;
        file.sync_all().map_err(|source| CacheError::Io {
            op: "sync_all",
            path: temp_path,
            source,
        })?;
    }
    match temp.persist_noclobber(canonical) {
        Ok(_) => Ok(()),
        Err(error) if error.error.kind() == ErrorKind::AlreadyExists => Ok(()),
        Err(source) => Err(CacheError::Persist { source }),
    }
}

fn write_tiff_image<C>(
    file: &mut File,
    metadata: &CogMetadata,
    window: RasterPixelWindow,
    data: &[C::Inner],
    remote_path: &ObjectPath,
) -> Result<(), CacheError>
where
    C: colortype::ColorType,
    [C::Inner]: tiff::encoder::TiffValue,
{
    let mut encoder = TiffEncoder::new(file).map_err(|source| CacheError::Tiff {
        path: remote_path.as_ref().to_string(),
        source,
    })?;
    let mut image = encoder
        .new_image::<C>(window.width, window.height)
        .map_err(|source| CacheError::Tiff {
            path: remote_path.as_ref().to_string(),
            source,
        })?;
    let origin_x = metadata.origin_x + f64::from(window.col_off) * metadata.pixel_width;
    let origin_y = metadata.origin_y + f64::from(window.row_off) * metadata.pixel_height;
    let pixel_scale = [metadata.pixel_width, -metadata.pixel_height, 0.0];
    let tiepoint = [0.0, 0.0, 0.0, origin_x, origin_y, 0.0];
    let geo_keys: [u16; 20] = [
        1, 1, 0, 4, // header: version, revision, minor, key count
        1024, 0, 1, 2, // GTModelTypeGeoKey = Geographic
        1025, 0, 1, 1, // GTRasterTypeGeoKey = PixelIsArea
        2048, 0, 1, 4326, // GeographicTypeGeoKey = EPSG:4326
        2054, 0, 1, 9102, // GeogAngularUnitsGeoKey = degree
    ];
    image
        .encoder()
        .write_tag(MODEL_PIXEL_SCALE_TAG, &pixel_scale[..])
        .map_err(|source| CacheError::Tiff {
            path: remote_path.as_ref().to_string(),
            source,
        })?;
    image
        .encoder()
        .write_tag(MODEL_TIEPOINT_TAG, &tiepoint[..])
        .map_err(|source| CacheError::Tiff {
            path: remote_path.as_ref().to_string(),
            source,
        })?;
    image
        .encoder()
        .write_tag(GEO_KEY_DIRECTORY_TAG, &geo_keys[..])
        .map_err(|source| CacheError::Tiff {
            path: remote_path.as_ref().to_string(),
            source,
        })?;
    image
        .encoder()
        .write_tag(GDAL_NODATA_TAG, metadata.nodata.as_str())
        .map_err(|source| CacheError::Tiff {
            path: remote_path.as_ref().to_string(),
            source,
        })?;
    image.write_data(data).map_err(|source| CacheError::Tiff {
        path: remote_path.as_ref().to_string(),
        source,
    })
}

/// `Read + Seek` over a sparse set of prefetched byte ranges.
#[derive(Debug, Clone)]
pub(crate) struct RangeBackedTiffReader {
    len: u64,
    pos: u64,
    ranges: Vec<(Range<u64>, Bytes)>,
}

impl RangeBackedTiffReader {
    pub(crate) fn new(len: u64, mut ranges: Vec<(Range<u64>, Bytes)>) -> Self {
        ranges.sort_by_key(|(range, _)| range.start);
        Self {
            len,
            pos: 0,
            ranges,
        }
    }

    fn current_range(&self) -> Option<(&Range<u64>, &Bytes)> {
        self.ranges
            .iter()
            .find(|(range, _)| range.start <= self.pos && self.pos < range.end)
            .map(|(range, bytes)| (range, bytes))
    }
}

impl Read for RangeBackedTiffReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() || self.pos >= self.len {
            return Ok(0);
        }
        let Some((range, bytes)) = self.current_range() else {
            return Err(std::io::Error::new(
                ErrorKind::UnexpectedEof,
                format!("missing prefetched TIFF range at byte {}", self.pos),
            ));
        };
        let src_off = (self.pos - range.start) as usize;
        let available = bytes.len().saturating_sub(src_off);
        let wanted = min(buf.len(), available);
        buf[..wanted].copy_from_slice(&bytes[src_off..src_off + wanted]);
        self.pos += wanted as u64;
        Ok(wanted)
    }
}

impl Seek for RangeBackedTiffReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset as i128,
            SeekFrom::End(offset) => self.len as i128 + offset as i128,
            SeekFrom::Current(offset) => self.pos as i128 + offset as i128,
        };
        if new_pos < 0 {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "cannot seek before start of TIFF",
            ));
        }
        self.pos = new_pos as u64;
        Ok(self.pos)
    }
}

#[cfg(test)]
mod tests {
    use geo::coord;

    use super::*;

    fn metadata() -> CogMetadata {
        CogMetadata {
            width: 2048,
            height: 1024,
            tile_width: 512,
            tile_height: 512,
            origin_x: -180.0,
            origin_y: 90.0,
            pixel_width: 1.0 / 1200.0,
            pixel_height: -1.0 / 1200.0,
            nodata: "255".to_string(),
            sample_type: CogSampleType::U8,
            compression: 8,
            predictor: 2,
            tile_offsets: (0..8).map(|idx| 1000 + idx * 100).collect(),
            tile_byte_counts: vec![50; 8],
        }
    }

    #[test]
    fn bbox_to_pixel_window_clamps_and_pads() {
        let meta = metadata();
        let bbox = Rect::new(
            coord! { x: -180.0, y: 89.99 },
            coord! { x: -179.99, y: 90.0 },
        );

        let window = RasterPixelWindow::from_bbox(&meta, &bbox).unwrap();

        assert_eq!(window.col_off, 0);
        assert_eq!(window.row_off, 0);
        assert!(window.width > 12);
        assert!(window.height > 12);
    }

    #[test]
    fn tile_plan_returns_intersecting_ranges() {
        let meta = metadata();
        let window = RasterPixelWindow {
            col_off: 500,
            row_off: 500,
            width: 30,
            height: 30,
        };

        let plan = TilePlan::for_window(&meta, window);

        assert_eq!(
            plan.tiles.iter().map(|tile| tile.index).collect::<Vec<_>>(),
            vec![0, 1, 4, 5]
        );
        assert_eq!(plan.byte_count(), 200);
    }

    #[test]
    fn range_reader_reads_across_present_ranges_and_errors_on_gap() {
        let ranges = vec![
            (0..4, Bytes::from_static(b"abcd")),
            (10..14, Bytes::from_static(b"klmn")),
        ];
        let mut reader = RangeBackedTiffReader::new(20, ranges);
        let mut buf = [0_u8; 3];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"abc");
        reader.seek(SeekFrom::Start(10)).unwrap();
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"klm");
        reader.seek(SeekFrom::Start(5)).unwrap();
        let err = reader.read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn materialized_window_geotiff_preserves_pixels_and_transform_tags() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("window.tif");
        let meta = metadata();
        let window = RasterPixelWindow {
            col_off: 10,
            row_off: 20,
            width: 2,
            height: 2,
        };

        write_window_geotiff(
            &path,
            &meta,
            window,
            &WindowData::U8(vec![1, 2, 3, 4]),
            &ObjectPath::from("remote/flow_dir.tif"),
        )
        .unwrap();

        let mut decoder = Decoder::new(File::open(path).unwrap()).unwrap();
        assert_eq!(decoder.dimensions().unwrap(), (2, 2));
        let scale = decoder.get_tag_f64_vec(MODEL_PIXEL_SCALE_TAG).unwrap();
        let tiepoint = decoder.get_tag_f64_vec(MODEL_TIEPOINT_TAG).unwrap();
        assert_eq!(scale, vec![meta.pixel_width, -meta.pixel_height, 0.0]);
        assert_eq!(
            tiepoint,
            vec![
                0.0,
                0.0,
                0.0,
                meta.origin_x + f64::from(window.col_off) * meta.pixel_width,
                meta.origin_y + f64::from(window.row_off) * meta.pixel_height,
                0.0
            ]
        );
        match decoder.read_image().unwrap() {
            DecodingResult::U8(values) => assert_eq!(values, vec![1, 2, 3, 4]),
            other => panic!("unexpected decoding result: {other:?}"),
        }
    }
}
