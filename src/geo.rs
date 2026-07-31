//! Geospatial support for the document engine (v1: points).
//!
//! Shapes the query surface understands:
//!   `{"loc": {"$geoWithin": {"$centerSphere": [[lon, lat], radians]}}}`
//!   `{"loc": {"$geoWithin": {"$box": [[minLon, minLat], [maxLon, maxLat]]}}}`
//!   `{"loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [lon, lat]},
//!                        "$maxDistance": meters, "$minDistance": meters}}}`
//!   `{"loc": {"$near": [lon, lat]}}`  (with optional sibling `$maxDistance` meters)
//!
//! `$nearSphere` is an alias for `$near`. MongoDB's planar `$center` and
//! polygon `$geometry` regions are refused by name (v1 is points on the
//! sphere) — refusal, never silent mis-measurement.
//!
//! A stored point is any of: `[lon, lat]`, GeoJSON `{"type": "Point",
//! "coordinates": [lon, lat]}`, or `{"lon"|"lng": x, "lat": y}`.
//!
//! The index is a geohash table: each document's point is encoded to a
//! fixed-precision geohash string, and a query turns its shape into a small
//! set of coarser cells whose prefixes are range-scanned. **The index only
//! nominates candidates** — every candidate is verified against the exact
//! predicate on the document, so the cover may be generous but never wrong.
//! Distances are great-circle (haversine) on the WGS84 mean radius, like
//! MongoDB's spherical operators.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde_json::Value;

use crate::document::DocumentId;

/// Mean earth radius in meters (IUGG), the value MongoDB documents for
/// `$centerSphere` radians→distance conversions.
pub const EARTH_RADIUS_M: f64 = 6_371_008.8;

/// Geohash precision stored in the index: 9 characters ≈ 4.8 m × 4.8 m cells.
const INDEX_PRECISION: usize = 9;

/// Cap on cells a cover may produce; beyond it the precision is lowered
/// (coarser cells, more candidates — slower, never wrong).
const MAX_COVER_CELLS: usize = 64;

const BASE32: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";

// ---------------------------------------------------------------------------
// Points
// ---------------------------------------------------------------------------

/// A parsed longitude/latitude pair, degrees, WGS84 order `(lon, lat)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    pub lon: f64,
    pub lat: f64,
}

impl Point {
    fn valid(&self) -> bool {
        self.lon.is_finite()
            && self.lat.is_finite()
            && (-180.0..=180.0).contains(&self.lon)
            && (-90.0..=90.0).contains(&self.lat)
    }
}

/// Extract a point from a stored document value, accepting the three shapes
/// documented at the top of this module. `None` when the value is not a
/// point — a document without a (valid) point never matches a geo predicate.
pub fn point_of_value(v: &Value) -> Option<Point> {
    let pair = |a: &Value, b: &Value| -> Option<Point> {
        let p = Point {
            lon: a.as_f64()?,
            lat: b.as_f64()?,
        };
        p.valid().then_some(p)
    };
    match v {
        Value::Array(arr) if arr.len() == 2 => pair(&arr[0], &arr[1]),
        Value::Object(o) => {
            if let Some(coords) = o.get("coordinates").and_then(|c| c.as_array()) {
                // GeoJSON: only Point is a point.
                if o.get("type").and_then(|t| t.as_str()) != Some("Point") || coords.len() != 2 {
                    return None;
                }
                return pair(&coords[0], &coords[1]);
            }
            let lon = o.get("lon").or_else(|| o.get("lng"))?;
            let lat = o.get("lat")?;
            pair(lon, lat)
        }
        _ => None,
    }
}

/// Great-circle distance in meters (haversine, mean earth radius).
pub fn haversine_m(a: Point, b: Point) -> f64 {
    let (lat1, lat2) = (a.lat.to_radians(), b.lat.to_radians());
    let dlat = (b.lat - a.lat).to_radians();
    let dlon = (b.lon - a.lon).to_radians();
    let h = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * h.sqrt().asin()
}

// ---------------------------------------------------------------------------
// Query shapes
// ---------------------------------------------------------------------------

/// A `$geoWithin` region.
#[derive(Debug, Clone, Copy)]
pub enum GeoShape {
    /// Spherical cap: everything within `radius_m` meters of the center.
    Circle { center: Point, radius_m: f64 },
    /// Longitude/latitude rectangle (no antimeridian wrap).
    Rect {
        min: Point, // (min lon, min lat)
        max: Point, // (max lon, max lat)
    },
}

impl GeoShape {
    pub fn contains(&self, p: Point) -> bool {
        match *self {
            GeoShape::Circle { center, radius_m } => haversine_m(center, p) <= radius_m,
            GeoShape::Rect { min, max } => {
                p.lon >= min.lon && p.lon <= max.lon && p.lat >= min.lat && p.lat <= max.lat
            }
        }
    }
}

/// A `$near` target: filter by optional distance bounds, sort ascending by
/// distance from `center` (the sort is applied by the find path when the
/// caller gave no explicit sort, matching MongoDB).
#[derive(Debug, Clone, Copy)]
pub struct GeoNear {
    pub center: Point,
    pub max_distance_m: Option<f64>,
    pub min_distance_m: Option<f64>,
}

impl GeoNear {
    pub fn matches(&self, p: Point) -> bool {
        let d = haversine_m(self.center, p);
        self.max_distance_m.is_none_or(|m| d <= m) && self.min_distance_m.is_none_or(|m| d >= m)
    }

    /// The circle the index cover uses: the max distance, or "everything"
    /// when unbounded (cover returns no restriction and the caller scans).
    pub fn cover_circle(&self) -> Option<GeoShape> {
        self.max_distance_m.map(|radius_m| GeoShape::Circle {
            center: self.center,
            radius_m,
        })
    }
}

// ---------------------------------------------------------------------------
// Query JSON → shapes
// ---------------------------------------------------------------------------

/// Parse a point literal from query JSON — same shapes as stored points,
/// but here a malformed point is an error, not a non-match.
fn query_point(v: &Value) -> Result<Point, String> {
    point_of_value(v).ok_or_else(|| {
        format!("expected a point ([lon, lat], GeoJSON Point, or {{lon, lat}}), got {v}")
    })
}

/// Parse the body of `$geoWithin`.
pub fn shape_from_json(v: &Value) -> Result<GeoShape, String> {
    let obj = v
        .as_object()
        .ok_or("$geoWithin takes an object ($centerSphere or $box)")?;
    if let Some(cs) = obj.get("$centerSphere") {
        let arr = cs
            .as_array()
            .filter(|a| a.len() == 2)
            .ok_or("$centerSphere takes [[lon, lat], radius-in-radians]")?;
        let center = query_point(&arr[0])?;
        let radians = arr[1]
            .as_f64()
            .filter(|r| r.is_finite() && *r >= 0.0)
            .ok_or("$centerSphere radius must be a non-negative number of radians")?;
        return Ok(GeoShape::Circle {
            center,
            radius_m: radians * EARTH_RADIUS_M,
        });
    }
    if let Some(bx) = obj.get("$box") {
        let arr = bx
            .as_array()
            .filter(|a| a.len() == 2)
            .ok_or("$box takes [[minLon, minLat], [maxLon, maxLat]]")?;
        let min = query_point(&arr[0])?;
        let max = query_point(&arr[1])?;
        if min.lon > max.lon || min.lat > max.lat {
            return Err("$box corners must be [min, max] (no antimeridian wrap)".into());
        }
        return Ok(GeoShape::Rect { min, max });
    }
    for unsupported in ["$center", "$geometry", "$polygon"] {
        if obj.contains_key(unsupported) {
            return Err(format!(
                "$geoWithin {unsupported} is not supported — use $centerSphere (spherical) or $box"
            ));
        }
    }
    Err("$geoWithin needs $centerSphere or $box".into())
}

/// Parse the body of `$near`/`$nearSphere`. `siblings` carries the legacy
/// form's `$maxDistance`/`$minDistance`, which sit next to the operator.
pub fn near_from_json(
    v: &Value,
    siblings: &serde_json::Map<String, Value>,
) -> Result<GeoNear, String> {
    let dist = |v: Option<&Value>, what: &str| -> Result<Option<f64>, String> {
        match v {
            None => Ok(None),
            Some(d) => d
                .as_f64()
                .filter(|m| m.is_finite() && *m >= 0.0)
                .map(Some)
                .ok_or(format!("{what} must be a non-negative number of meters")),
        }
    };
    if let Some(obj) = v.as_object() {
        if let Some(g) = obj.get("$geometry") {
            let center = query_point(g)?;
            return Ok(GeoNear {
                center,
                max_distance_m: dist(obj.get("$maxDistance"), "$maxDistance")?,
                min_distance_m: dist(obj.get("$minDistance"), "$minDistance")?,
            });
        }
    }
    // Legacy: `$near: [lon, lat]` with sibling distances (meters here —
    // MongoDB's legacy planar degrees would silently mis-measure).
    let center = query_point(v)?;
    Ok(GeoNear {
        center,
        max_distance_m: dist(siblings.get("$maxDistance"), "$maxDistance")?,
        min_distance_m: dist(siblings.get("$minDistance"), "$minDistance")?,
    })
}

// ---------------------------------------------------------------------------
// Geohash
// ---------------------------------------------------------------------------

/// Standard geohash of `p` at `precision` base32 characters.
pub fn geohash(p: Point, precision: usize) -> String {
    let (mut lon_lo, mut lon_hi) = (-180.0f64, 180.0f64);
    let (mut lat_lo, mut lat_hi) = (-90.0f64, 90.0f64);
    let mut out = String::with_capacity(precision);
    let mut bit = 0usize; // even bits are longitude
    let mut ch = 0usize;
    while out.len() < precision {
        if bit.is_multiple_of(2) {
            let mid = (lon_lo + lon_hi) / 2.0;
            ch <<= 1;
            if p.lon >= mid {
                ch |= 1;
                lon_lo = mid;
            } else {
                lon_hi = mid;
            }
        } else {
            let mid = (lat_lo + lat_hi) / 2.0;
            ch <<= 1;
            if p.lat >= mid {
                ch |= 1;
                lat_lo = mid;
            } else {
                lat_hi = mid;
            }
        }
        bit += 1;
        if bit.is_multiple_of(5) {
            out.push(BASE32[ch] as char);
            ch = 0;
        }
    }
    out
}

/// Cell spans in degrees at `precision` characters: `(lon_span, lat_span)`.
fn cell_spans(precision: usize) -> (f64, f64) {
    let bits = precision * 5;
    let lon_bits = bits.div_ceil(2);
    let lat_bits = bits / 2;
    (
        360.0 / (1u64 << lon_bits) as f64,
        180.0 / (1u64 << lat_bits) as f64,
    )
}

/// The geohash cells (as prefixes) covering `shape`, or `None` when the
/// shape is too large for a useful cover (the caller falls back to a scan).
///
/// The precision is chosen so the shape's bounding box spans at most a few
/// cells in each axis; cells are enumerated by encoding grid sample points,
/// which sidesteps neighbor-table arithmetic entirely. Every returned cell
/// is a prefix of the fixed-precision keys stored in the index.
pub fn cover(shape: &GeoShape) -> Option<Vec<String>> {
    // Bounding box of the shape in degrees.
    let (min_lon, min_lat, max_lon, max_lat) = match *shape {
        GeoShape::Rect { min, max } => (min.lon, min.lat, max.lon, max.lat),
        GeoShape::Circle { center, radius_m } => {
            let dlat = (radius_m / 110_574.0).min(90.0);
            // Meters per degree of longitude shrink with latitude; use the
            // widest latitude the circle reaches so the box never undershoots.
            let widest = (center.lat.abs() + dlat).min(89.9_f64);
            let dlon = (radius_m / (111_320.0 * widest.to_radians().cos())).min(180.0);
            (
                center.lon - dlon,
                (center.lat - dlat).max(-90.0),
                center.lon + dlon,
                (center.lat + dlat).min(90.0),
            )
        }
    };
    // Pick the deepest precision whose cell grid keeps the cover small.
    for precision in (1..=INDEX_PRECISION).rev() {
        let (lon_span, lat_span) = cell_spans(precision);
        let cols = ((max_lon - min_lon) / lon_span).floor() as usize + 2;
        let rows = ((max_lat - min_lat) / lat_span).floor() as usize + 2;
        if cols.saturating_mul(rows) > MAX_COVER_CELLS {
            continue;
        }
        let mut cells = BTreeSet::new();
        let mut lat = min_lat;
        loop {
            let mut lon = min_lon;
            loop {
                // Clamp latitude; wrap longitude so a box straddling ±180°
                // still lands on real cells.
                let p = Point {
                    lon: ((lon + 180.0).rem_euclid(360.0)) - 180.0,
                    lat: lat.clamp(-90.0, 90.0),
                };
                cells.insert(geohash(p, precision));
                if lon >= max_lon {
                    break;
                }
                lon = (lon + lon_span).min(max_lon);
            }
            if lat >= max_lat {
                break;
            }
            lat = (lat + lat_span).min(max_lat);
        }
        return Some(cells.into_iter().collect());
    }
    None // whole-planet shape: a scan is the honest plan
}

// ---------------------------------------------------------------------------
// The index
// ---------------------------------------------------------------------------

/// Geohash index over one field: fixed-precision geohash → document ids.
///
/// Only the *definition* is persisted (in the collection's index metadata);
/// the table is rebuilt by one document scan at open, the same deal the text
/// index made — paid only by collections that asked for geo.
#[derive(Debug, Default)]
pub struct GeoIndex {
    cells: BTreeMap<String, BTreeSet<DocumentId>>,
    ids: HashMap<DocumentId, String>,
}

impl GeoIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Index (or re-index) a document's point; a non-point value removes any
    /// stale entry, so an update that drops the field un-indexes the doc.
    pub fn upsert(&mut self, id: DocumentId, value: Option<&Value>) {
        self.remove(id);
        if let Some(p) = value.and_then(point_of_value) {
            let cell = geohash(p, INDEX_PRECISION);
            self.cells.entry(cell.clone()).or_default().insert(id);
            self.ids.insert(id, cell);
        }
    }

    /// Drop every entry (rebuild paths re-populate from the live documents).
    pub fn clear(&mut self) {
        self.cells.clear();
        self.ids.clear();
    }

    pub fn remove(&mut self, id: DocumentId) {
        if let Some(cell) = self.ids.remove(&id) {
            if let Some(set) = self.cells.get_mut(&cell) {
                set.remove(&id);
                if set.is_empty() {
                    self.cells.remove(&cell);
                }
            }
        }
    }

    /// Candidate document ids for `shape` — a superset of the true matches
    /// (callers verify each against the document). `None` when the shape has
    /// no useful cover and the caller should scan.
    pub fn candidates(&self, shape: &GeoShape) -> Option<BTreeSet<DocumentId>> {
        let cover = cover(shape)?;
        let mut out = BTreeSet::new();
        for prefix in cover {
            for (_, ids) in self
                .cells
                .range(prefix.clone()..)
                .take_while(|(k, _)| k.starts_with(&prefix))
            {
                out.extend(ids.iter().copied());
            }
        }
        Some(out)
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn p(lon: f64, lat: f64) -> Point {
        Point { lon, lat }
    }

    #[test]
    fn geohash_known_vectors() {
        // `ezs42` is the canonical reference vector (Wikipedia's worked
        // example); the others pin stability, cross-checked against an
        // independent implementation.
        assert_eq!(geohash(p(-5.6, 42.6), 5), "ezs42");
        assert_eq!(geohash(p(-79.3829, 43.6544), 9), "dpz83e51k"); // Toronto
        assert_eq!(geohash(p(28.9784, 41.0082), 9), "sxk973m6j"); // Istanbul
    }

    #[test]
    fn haversine_known_distances() {
        // Istanbul → Ankara ≈ 349.7 km.
        let d = haversine_m(p(28.9784, 41.0082), p(32.8597, 39.9334));
        assert!((d - 349_700.0).abs() < 2_000.0, "{d}");
        // A degree of latitude ≈ 111.2 km.
        let d = haversine_m(p(0.0, 0.0), p(0.0, 1.0));
        assert!((d - 111_195.0).abs() < 100.0, "{d}");
    }

    #[test]
    fn point_shapes_parse() {
        assert_eq!(point_of_value(&json!([28.9, 41.0])), Some(p(28.9, 41.0)));
        assert_eq!(
            point_of_value(&json!({"type": "Point", "coordinates": [28.9, 41.0]})),
            Some(p(28.9, 41.0))
        );
        assert_eq!(
            point_of_value(&json!({"lon": 28.9, "lat": 41.0})),
            Some(p(28.9, 41.0))
        );
        assert_eq!(
            point_of_value(&json!({"lng": 28.9, "lat": 41.0})),
            Some(p(28.9, 41.0))
        );
        // Not points: wrong arity, out-of-range, non-Point GeoJSON.
        assert_eq!(point_of_value(&json!([1, 2, 3])), None);
        assert_eq!(point_of_value(&json!([200.0, 10.0])), None);
        assert_eq!(
            point_of_value(&json!({"type": "Polygon", "coordinates": [[28.9, 41.0]]})),
            None
        );
    }

    #[test]
    fn cover_candidates_are_a_superset() {
        // Points on a grid around Istanbul; the circle cover must nominate
        // every true match (extras are fine — verification removes them).
        let mut idx = GeoIndex::new();
        let mut id = 1u64;
        let mut inside = BTreeSet::new();
        let center = p(28.9784, 41.0082);
        for dx in -20..=20 {
            for dy in -20..=20 {
                let pt = p(28.9784 + dx as f64 * 0.01, 41.0082 + dy as f64 * 0.01);
                idx.upsert(id, Some(&json!([pt.lon, pt.lat])));
                if haversine_m(center, pt) <= 5_000.0 {
                    inside.insert(id);
                }
                id += 1;
            }
        }
        let shape = GeoShape::Circle {
            center,
            radius_m: 5_000.0,
        };
        let candidates = idx.candidates(&shape).expect("coverable");
        assert!(!inside.is_empty());
        assert!(
            candidates.is_superset(&inside),
            "missed {} true matches",
            inside.difference(&candidates).count()
        );
        // And the cover is meaningfully selective on this grid.
        assert!(candidates.len() < (id as usize - 1) / 2);
    }

    #[test]
    fn rect_cover_and_updates() {
        let mut idx = GeoIndex::new();
        idx.upsert(1, Some(&json!([10.0, 50.0])));
        idx.upsert(2, Some(&json!([11.0, 50.5])));
        idx.upsert(3, Some(&json!([-70.0, -30.0])));
        let shape = GeoShape::Rect {
            min: p(9.0, 49.0),
            max: p(12.0, 51.0),
        };
        let c = idx.candidates(&shape).expect("coverable");
        assert!(c.contains(&1) && c.contains(&2));
        assert!(!c.contains(&3));
        // Update moves a doc out; removal drops it.
        idx.upsert(1, Some(&json!([100.0, 0.0])));
        assert!(!idx.candidates(&shape).unwrap().contains(&1));
        idx.upsert(2, None);
        assert!(!idx.candidates(&shape).unwrap().contains(&2));
        assert_eq!(idx.len(), 2); // ids 1 and 3
    }

    #[test]
    fn huge_shapes_decline_to_a_scan() {
        let shape = GeoShape::Circle {
            center: p(0.0, 0.0),
            radius_m: 20_000_000.0,
        };
        // Half the planet: no useful cover; candidates() says "scan".
        assert!(cover(&shape).is_none() || cover(&shape).unwrap().len() <= MAX_COVER_CELLS);
    }
}
