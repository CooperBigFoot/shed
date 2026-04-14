//! D8 flow direction encoding with support for ESRI and TauDEM conventions.
//!
//! ESRI D8 encoding: 1=E, 2=SE, 4=S, 8=SW, 16=W, 32=NW, 64=N, 128=NE (powers of two).
//! TauDEM D8 encoding: 1=E, 2=NE, 3=N, 4=NW, 5=W, 6=SW, 7=S, 8=SE (counter-clockwise).
//! 0 = nodata for both encodings.

use hfx_core::FlowDirEncoding;

/// Error returned when a byte is not a valid D8 encoding.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum InvalidFlowDir {
    /// The byte value is not one of the eight valid D8 encodings for the given convention.
    #[error("invalid D8 flow direction encoding: {value}")]
    InvalidEncoding {
        /// The invalid byte value.
        value: u8,
    },
}

/// D8 flow direction supporting both ESRI and TauDEM encodings.
///
/// ESRI powers-of-two: E=1, SE=2, S=4, SW=8, W=16, NW=32, N=64, NE=128.
/// TauDEM counter-clockwise from east: E=1, NE=2, N=3, NW=4, W=5, SW=6, S=7, SE=8.
/// A raw value of 0 represents nodata for both conventions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FlowDir {
    /// Flow toward the east (column increases).
    East,
    /// Flow toward the southeast.
    Southeast,
    /// Flow toward the south (row increases).
    South,
    /// Flow toward the southwest.
    Southwest,
    /// Flow toward the west (column decreases).
    West,
    /// Flow toward the northwest.
    Northwest,
    /// Flow toward the north (row decreases).
    North,
    /// Flow toward the northeast.
    Northeast,
}

impl FlowDir {
    /// All eight D8 directions in ESRI encoding order (ascending powers of two).
    pub const ALL: [FlowDir; 8] = [
        Self::East,
        Self::Southeast,
        Self::South,
        Self::Southwest,
        Self::West,
        Self::Northwest,
        Self::North,
        Self::Northeast,
    ];

    /// Returns the ESRI D8 byte code for this direction.
    pub fn to_esri(self) -> u8 {
        match self {
            Self::East => 1,
            Self::Southeast => 2,
            Self::South => 4,
            Self::Southwest => 8,
            Self::West => 16,
            Self::Northwest => 32,
            Self::North => 64,
            Self::Northeast => 128,
        }
    }

    /// Decodes an ESRI D8 byte into a `FlowDir`.
    ///
    /// # Errors
    ///
    /// | Condition | Error |
    /// |-----------|-------|
    /// | `value` is not a valid ESRI D8 code and not 0 | [`InvalidFlowDir::InvalidEncoding`] |
    ///
    /// Returns `Ok(None)` for `value == 0` (nodata cell).
    pub fn from_esri(value: u8) -> Result<Option<Self>, InvalidFlowDir> {
        match value {
            0 => Ok(None),
            1 => Ok(Some(Self::East)),
            2 => Ok(Some(Self::Southeast)),
            4 => Ok(Some(Self::South)),
            8 => Ok(Some(Self::Southwest)),
            16 => Ok(Some(Self::West)),
            32 => Ok(Some(Self::Northwest)),
            64 => Ok(Some(Self::North)),
            128 => Ok(Some(Self::Northeast)),
            _ => Err(InvalidFlowDir::InvalidEncoding { value }),
        }
    }

    /// Returns the TauDEM D8 byte code for this direction.
    ///
    /// TauDEM uses east-origin, counter-clockwise encoding:
    /// E=1, NE=2, N=3, NW=4, W=5, SW=6, S=7, SE=8.
    pub fn to_taudem(self) -> u8 {
        match self {
            Self::East => 1,
            Self::Northeast => 2,
            Self::North => 3,
            Self::Northwest => 4,
            Self::West => 5,
            Self::Southwest => 6,
            Self::South => 7,
            Self::Southeast => 8,
        }
    }

    /// Decodes a TauDEM D8 byte into a `FlowDir`.
    ///
    /// TauDEM D8 encoding: 1=E, 2=NE, 3=N, 4=NW, 5=W, 6=SW, 7=S, 8=SE.
    /// 0 = nodata.
    ///
    /// # Errors
    ///
    /// | Condition | Error |
    /// |-----------|-------|
    /// | `value` is not in 0–8 | [`InvalidFlowDir::InvalidEncoding`] |
    ///
    /// Returns `Ok(None)` for `value == 0` (nodata cell).
    pub fn from_taudem(value: u8) -> Result<Option<Self>, InvalidFlowDir> {
        match value {
            0 => Ok(None),
            1 => Ok(Some(Self::East)),
            2 => Ok(Some(Self::Northeast)),
            3 => Ok(Some(Self::North)),
            4 => Ok(Some(Self::Northwest)),
            5 => Ok(Some(Self::West)),
            6 => Ok(Some(Self::Southwest)),
            7 => Ok(Some(Self::South)),
            8 => Ok(Some(Self::Southeast)),
            _ => Err(InvalidFlowDir::InvalidEncoding { value }),
        }
    }

    /// Decodes a raw byte using the specified encoding convention.
    ///
    /// Dispatches to [`Self::from_esri`] or [`Self::from_taudem`] based on `encoding`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidFlowDir::InvalidEncoding`] if `value` is invalid for the chosen encoding.
    pub fn from_encoded(
        value: u8,
        encoding: FlowDirEncoding,
    ) -> Result<Option<Self>, InvalidFlowDir> {
        match encoding {
            FlowDirEncoding::Esri => Self::from_esri(value),
            FlowDirEncoding::Taudem => Self::from_taudem(value),
        }
    }

    /// Encodes this direction as a raw byte using the specified encoding convention.
    ///
    /// Dispatches to [`Self::to_esri`] or [`Self::to_taudem`] based on `encoding`.
    pub fn to_encoded(self, encoding: FlowDirEncoding) -> u8 {
        match encoding {
            FlowDirEncoding::Esri => self.to_esri(),
            FlowDirEncoding::Taudem => self.to_taudem(),
        }
    }

    /// Returns the column offset for one step in this direction.
    ///
    /// Positive means east (column index increases), negative means west.
    pub fn dx(self) -> isize {
        match self {
            Self::East | Self::Northeast | Self::Southeast => 1,
            Self::West | Self::Northwest | Self::Southwest => -1,
            Self::North | Self::South => 0,
        }
    }

    /// Returns the row offset for one step in this direction.
    ///
    /// In raster space row 0 is at the top, so north = -1 and south = +1.
    pub fn dy(self) -> isize {
        match self {
            Self::North | Self::Northwest | Self::Northeast => -1,
            Self::South | Self::Southwest | Self::Southeast => 1,
            Self::East | Self::West => 0,
        }
    }

    /// Returns the direction exactly opposite to `self`.
    pub fn opposite(self) -> Self {
        match self {
            Self::East => Self::West,
            Self::Southeast => Self::Northwest,
            Self::South => Self::North,
            Self::Southwest => Self::Northeast,
            Self::West => Self::East,
            Self::Northwest => Self::Southeast,
            Self::North => Self::South,
            Self::Northeast => Self::Southwest,
        }
    }
}

impl TryFrom<u8> for FlowDir {
    type Error = InvalidFlowDir;

    /// Converts a raw byte to a [`FlowDir`] using ESRI encoding.
    ///
    /// Unlike [`FlowDir::from_esri`], a value of `0` is treated as an error
    /// because `TryFrom` cannot return an `Option`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidFlowDir::InvalidEncoding`] for any byte that is not a
    /// valid non-zero ESRI D8 code (including `0`).
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::East),
            2 => Ok(Self::Southeast),
            4 => Ok(Self::South),
            8 => Ok(Self::Southwest),
            16 => Ok(Self::West),
            32 => Ok(Self::Northwest),
            64 => Ok(Self::North),
            128 => Ok(Self::Northeast),
            _ => Err(InvalidFlowDir::InvalidEncoding { value }),
        }
    }
}

#[cfg(test)]
mod tests {
    use hfx_core::FlowDirEncoding;

    use super::{FlowDir, InvalidFlowDir};

    const ALL_VARIANTS: [FlowDir; 8] = FlowDir::ALL;

    // --- ESRI tests (ported from hydra-shed) ---

    #[test]
    fn esri_round_trip() {
        for v in ALL_VARIANTS {
            assert_eq!(
                FlowDir::from_esri(v.to_esri()),
                Ok(Some(v)),
                "round-trip failed for {v:?}"
            );
        }
    }

    #[test]
    fn from_esri_nodata() {
        assert_eq!(FlowDir::from_esri(0), Ok(None));
    }

    #[test]
    fn from_esri_invalid() {
        for bad in [3u8, 5, 6, 7, 255] {
            assert!(
                matches!(
                    FlowDir::from_esri(bad),
                    Err(InvalidFlowDir::InvalidEncoding { value }) if value == bad
                ),
                "expected Err for {bad}"
            );
        }
    }

    #[test]
    fn try_from_zero_is_err() {
        assert!(FlowDir::try_from(0u8).is_err());
    }

    #[test]
    fn try_from_valid() {
        let cases = [
            (1u8, FlowDir::East),
            (2, FlowDir::Southeast),
            (4, FlowDir::South),
            (8, FlowDir::Southwest),
            (16, FlowDir::West),
            (32, FlowDir::Northwest),
            (64, FlowDir::North),
            (128, FlowDir::Northeast),
        ];
        for (byte, expected) in cases {
            assert_eq!(FlowDir::try_from(byte), Ok(expected), "try_from({byte})");
        }
    }

    #[test]
    fn dx_dy_all_directions() {
        let cases = [
            (FlowDir::East, 1isize, 0isize),
            (FlowDir::Southeast, 1, 1),
            (FlowDir::South, 0, 1),
            (FlowDir::Southwest, -1, 1),
            (FlowDir::West, -1, 0),
            (FlowDir::Northwest, -1, -1),
            (FlowDir::North, 0, -1),
            (FlowDir::Northeast, 1, -1),
        ];
        for (dir, expected_dx, expected_dy) in cases {
            assert_eq!(dir.dx(), expected_dx, "dx for {dir:?}");
            assert_eq!(dir.dy(), expected_dy, "dy for {dir:?}");
        }
    }

    #[test]
    fn opposite_symmetry() {
        for v in ALL_VARIANTS {
            assert_eq!(
                v.opposite().opposite(),
                v,
                "double-opposite failed for {v:?}"
            );
        }
    }

    #[test]
    fn opposite_correctness() {
        assert_eq!(FlowDir::East.opposite(), FlowDir::West);
        assert_eq!(FlowDir::North.opposite(), FlowDir::South);
        assert_eq!(FlowDir::Northeast.opposite(), FlowDir::Southwest);
        assert_eq!(FlowDir::Northwest.opposite(), FlowDir::Southeast);
    }

    #[test]
    fn all_matches_esri_order() {
        let expected_codes: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
        let actual_codes: Vec<u8> = FlowDir::ALL.iter().map(|d| d.to_esri()).collect();
        assert_eq!(actual_codes, expected_codes);
    }

    // --- TauDEM tests (new) ---

    #[test]
    fn taudem_all_directions() {
        let cases = [
            (1u8, FlowDir::East),
            (2, FlowDir::Northeast),
            (3, FlowDir::North),
            (4, FlowDir::Northwest),
            (5, FlowDir::West),
            (6, FlowDir::Southwest),
            (7, FlowDir::South),
            (8, FlowDir::Southeast),
        ];
        for (byte, expected) in cases {
            assert_eq!(
                FlowDir::from_taudem(byte),
                Ok(Some(expected)),
                "from_taudem({byte})"
            );
        }
    }

    #[test]
    fn taudem_nodata() {
        assert_eq!(FlowDir::from_taudem(0), Ok(None));
    }

    #[test]
    fn taudem_invalid() {
        for bad in [9u8, 255] {
            assert!(
                matches!(
                    FlowDir::from_taudem(bad),
                    Err(InvalidFlowDir::InvalidEncoding { value }) if value == bad
                ),
                "expected Err for {bad}"
            );
        }
    }

    #[test]
    fn taudem_round_trip() {
        for v in ALL_VARIANTS {
            assert_eq!(
                FlowDir::from_taudem(v.to_taudem()),
                Ok(Some(v)),
                "TauDEM round-trip failed for {v:?}"
            );
        }
    }

    // --- from_encoded / to_encoded dispatch tests ---

    #[test]
    fn from_encoded_esri_dispatch() {
        assert_eq!(
            FlowDir::from_encoded(1, FlowDirEncoding::Esri),
            Ok(Some(FlowDir::East))
        );
        assert_eq!(
            FlowDir::from_encoded(128, FlowDirEncoding::Esri),
            Ok(Some(FlowDir::Northeast))
        );
        assert_eq!(FlowDir::from_encoded(0, FlowDirEncoding::Esri), Ok(None));
    }

    #[test]
    fn from_encoded_taudem_dispatch() {
        assert_eq!(
            FlowDir::from_encoded(1, FlowDirEncoding::Taudem),
            Ok(Some(FlowDir::East))
        );
        assert_eq!(
            FlowDir::from_encoded(2, FlowDirEncoding::Taudem),
            Ok(Some(FlowDir::Northeast))
        );
        assert_eq!(FlowDir::from_encoded(0, FlowDirEncoding::Taudem), Ok(None));
    }

    #[test]
    fn to_encoded_esri_dispatch() {
        assert_eq!(FlowDir::East.to_encoded(FlowDirEncoding::Esri), 1);
        assert_eq!(FlowDir::Northeast.to_encoded(FlowDirEncoding::Esri), 128);
    }

    #[test]
    fn to_encoded_taudem_dispatch() {
        assert_eq!(FlowDir::East.to_encoded(FlowDirEncoding::Taudem), 1);
        assert_eq!(FlowDir::Northeast.to_encoded(FlowDirEncoding::Taudem), 2);
    }

    #[test]
    fn from_encoded_invalid_esri() {
        assert!(FlowDir::from_encoded(3, FlowDirEncoding::Esri).is_err());
    }

    #[test]
    fn from_encoded_invalid_taudem() {
        assert!(FlowDir::from_encoded(9, FlowDirEncoding::Taudem).is_err());
        assert!(FlowDir::from_encoded(255, FlowDirEncoding::Taudem).is_err());
    }
}
