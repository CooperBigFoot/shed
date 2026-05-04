//! Friendly kwargs validation for PyEngine methods.
//!
//! Replaces PyO3's terse "unexpected keyword argument" error with actionable
//! messages that include a hint (registry-based or edit-distance fallback) and
//! a list of the valid kwargs for the called method.

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

/// The PyEngine method being validated — drives the hint registry and the
/// method name embedded in the error message.
#[allow(dead_code)]
pub enum KwargContext {
    /// `Engine.__init__`
    EngineNew,
    /// `engine.delineate(...)`
    Delineate,
    /// `engine.delineate_batch(...)`
    DelineateBatch,
}

impl KwargContext {
    fn method_name(&self) -> &'static str {
        match self {
            KwargContext::EngineNew => "__init__",
            KwargContext::Delineate => "delineate",
            KwargContext::DelineateBatch => "delineate_batch",
        }
    }
}

/// Return a static hint for a known-but-misplaced/misspelled kwarg name, or
/// `None` if the registry has no entry.
fn registry_hint(name: &str, ctx: &KwargContext) -> Option<String> {
    // Constructor-only kwargs that users accidentally pass to delineate/batch.
    let constructor_kwargs = [
        "snap_radius",
        "snap_strategy",
        "snap_threshold",
        "clean_epsilon",
        "refine",
        "parquet_cache",
        "parquet_cache_max_mb",
    ];
    if constructor_kwargs.contains(&name) {
        return Some(match ctx {
            KwargContext::EngineNew => {
                // Shouldn't appear here since these ARE valid on EngineNew, but
                // the validate_kwargs caller won't call us with allowed names.
                // Defensive fallback only.
                format!(
                    "'{name}' is a valid constructor argument; \
                     check the spelling against the allowed list."
                )
            }
            KwargContext::Delineate | KwargContext::DelineateBatch => {
                let method = ctx.method_name();
                format!(
                    "'{name}' is a constructor argument of `Engine(dataset_path, ...)`; \
                     pass it when constructing the engine, not when calling `{method}`."
                )
            }
        });
    }

    // Delineate-only kwargs that users accidentally pass to the constructor.
    let delineate_kwargs = ["lat", "lon", "geometry"];
    if delineate_kwargs.contains(&name) {
        return Some(match ctx {
            KwargContext::EngineNew => {
                format!(
                    "'{name}' is an argument of `engine.delineate(lat, lon, ...)`; \
                     pass it when calling delineate, not when constructing the engine."
                )
            }
            // Already allowed on Delineate — only reachable if someone
            // removed them from the allowed list, so be defensive.
            KwargContext::Delineate => format!(
                "'{name}' should be in the allowed list for this method; \
                 this is a pyshed bug — please report it."
            ),
            KwargContext::DelineateBatch => format!(
                "'{name}' is a per-outlet field, not a batch kwarg. \
                 Pass it inside outlets, e.g. engine.delineate_batch(outlets=[{{\"lat\": 47.0, \"lon\": 8.0}}])."
            ),
        });
    }

    // Common misspellings / synonyms for lat/lon/geometry.
    match name {
        "latitude" | "lattitude" | "y" => {
            return Some("did you mean `lat`?".to_owned());
        }
        "longitude" | "lng" | "x" => {
            return Some("did you mean `lon`?".to_owned());
        }
        "return_geometry" | "geom" | "with_geometry" => {
            return Some("did you mean `geometry`?".to_owned());
        }
        // Common path/dataset synonyms passed to delineate instead of constructor.
        "dataset" | "path" | "dataset_path"
            if matches!(ctx, KwargContext::Delineate | KwargContext::DelineateBatch) =>
        {
            return Some(
                "this is the first positional argument of `Engine(dataset_path, ...)`; \
                 pass it at construction time."
                    .to_owned(),
            );
        }
        _ => {}
    }

    None
}

/// Validate kwarg names against the allowed names for `ctx`.
///
/// On success returns `Ok(())`. On the first unknown kwarg, raises a
/// string error with a hint from the registry (or an edit-distance suggestion)
/// and a list of valid kwargs.
pub fn validate_kwarg_names(
    names: &[&str],
    allowed: &[&str],
    ctx: &KwargContext,
) -> Result<(), String> {
    for name in names {
        if allowed.contains(name) {
            continue;
        }

        // Build the hint.
        let hint = if let Some(h) = registry_hint(name, ctx) {
            h
        } else {
            // Edit-distance fallback: find closest allowed kwarg within ≤ 2.
            let best = allowed
                .iter()
                .map(|&candidate| (candidate, strsim::levenshtein(name, candidate)))
                .min_by_key(|&(_, dist)| dist);

            match best {
                Some((candidate, dist)) if dist <= 2 => {
                    format!("did you mean `{candidate}`?")
                }
                _ => {
                    // No close match — list the valid kwargs.
                    format!(
                        "valid kwargs for `{}`: {}.",
                        ctx.method_name(),
                        allowed.join(", ")
                    )
                }
            }
        };

        let method = ctx.method_name();
        let valid_list = allowed.join(", ");
        return Err(format!(
            "Engine.{method}() got unexpected keyword argument '{name}'.\n\
             Hint: {hint}\n\
             Valid kwargs for {method}: {valid_list}."
        ));
    }

    Ok(())
}

/// Validate a Python kwargs dict against the allowed names for `ctx`.
///
/// Extracts keyword names from Python, then delegates all validation and hint
/// formatting to [`validate_kwarg_names`].
///
/// # Errors
///
/// - [`pyo3::exceptions::PyTypeError`] — when an unknown kwarg is encountered.
pub fn validate_kwargs(
    kwargs: Option<&Bound<'_, PyDict>>,
    allowed: &[&str],
    ctx: KwargContext,
) -> PyResult<()> {
    let Some(kwargs) = kwargs else {
        return Ok(());
    };

    let names = kwargs
        .iter()
        .map(|(key, _val)| key.extract::<String>())
        .collect::<PyResult<Vec<_>>>()?;
    let name_refs = names.iter().map(String::as_str).collect::<Vec<_>>();

    validate_kwarg_names(&name_refs, allowed, &ctx).map_err(PyTypeError::new_err)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An allowed kwarg must pass silently.
    #[test]
    fn known_kwarg_passes() {
        let result = validate_kwarg_names(
            &["snap_radius"],
            &["snap_radius", "refine"],
            &KwargContext::EngineNew,
        );
        assert!(result.is_ok(), "known kwarg should pass");
    }

    /// `snap_radius` on Delineate fires the registry hint about the constructor.
    #[test]
    fn registry_hint_fires_for_constructor_kwarg_on_delineate() {
        let result = validate_kwarg_names(
            &["snap_radius"],
            &["lat", "lon", "geometry"],
            &KwargContext::Delineate,
        );
        let msg = result.unwrap_err();
        assert!(
            msg.contains("snap_radius"),
            "message should mention 'snap_radius': {msg}"
        );
        assert!(
            msg.contains("constructor"),
            "message should mention 'constructor': {msg}"
        );
    }

    /// `geomtry` (typo) triggers the Levenshtein suggestion for `geometry`.
    #[test]
    fn levenshtein_suggestion_fires() {
        let result = validate_kwarg_names(
            &["geomtry"],
            &["lat", "lon", "geometry"],
            &KwargContext::Delineate,
        );
        let msg = result.unwrap_err();
        assert!(
            msg.contains("geometry"),
            "message should suggest 'geometry': {msg}"
        );
    }

    /// A completely unrelated name with no close match lists the valid kwargs.
    #[test]
    fn unknown_unrelated_name_lists_valid_kwargs() {
        let result = validate_kwarg_names(
            &["foobarqux"],
            &["lat", "lon", "geometry"],
            &KwargContext::Delineate,
        );
        let msg = result.unwrap_err();
        assert!(
            msg.contains("lat"),
            "message should list valid kwargs including 'lat': {msg}"
        );
        assert!(
            msg.contains("lon"),
            "message should list valid kwargs including 'lon': {msg}"
        );
        assert!(
            msg.contains("geometry"),
            "message should list valid kwargs including 'geometry': {msg}"
        );
    }

    /// An empty kwargs name slice is always valid.
    #[test]
    fn empty_kwargs_passes() {
        let result =
            validate_kwarg_names(&[], &["lat", "lon", "geometry"], &KwargContext::Delineate);
        assert!(result.is_ok(), "empty kwargs should pass");
    }
}
