use std::collections::{BinaryHeap, HashMap};
use std::io::{self, Read, Write};

use rand::Rng;
use serde_json::Value;

use crate::document::DocumentId;

/// Distance metric for vector similarity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    /// Cosine distance: 1 - cos(a, b). Range [0, 2], 0 = identical.
    Cosine,
    /// Euclidean (L2) distance: sqrt(sum((a_i - b_i)^2)).
    Euclidean,
    /// Negative dot product: -sum(a_i * b_i). Lower = more similar.
    DotProduct,
}

impl DistanceMetric {
    /// Convert a raw distance to a similarity score in [0, 1].
    pub fn to_similarity(self, distance: f32) -> f32 {
        match self {
            DistanceMetric::Cosine => 1.0 - distance / 2.0,
            DistanceMetric::Euclidean => 1.0 / (1.0 + distance),
            DistanceMetric::DotProduct => {
                // distance is -dot, so similarity = sigmoid(dot) = sigmoid(-distance)
                1.0 / (1.0 + (-distance).exp())
            }
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "cosine" => Some(DistanceMetric::Cosine),
            "euclidean" => Some(DistanceMetric::Euclidean),
            "dotproduct" | "dot_product" => Some(DistanceMetric::DotProduct),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            DistanceMetric::Cosine => "cosine",
            DistanceMetric::Euclidean => "euclidean",
            DistanceMetric::DotProduct => "dotproduct",
        }
    }

    fn to_u8(self) -> u8 {
        match self {
            DistanceMetric::Cosine => 0,
            DistanceMetric::Euclidean => 1,
            DistanceMetric::DotProduct => 2,
        }
    }

    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(DistanceMetric::Cosine),
            1 => Some(DistanceMetric::Euclidean),
            2 => Some(DistanceMetric::DotProduct),
            _ => None,
        }
    }
}

/// Result of a vector search.
#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    pub doc_id: DocumentId,
    pub distance: f32,
    pub similarity: f32,
}

/// HNSW configuration parameters.
#[derive(Debug, Clone)]
struct HnswConfig {
    m: usize,              // Max connections per node per layer
    m_max0: usize,         // Max connections at layer 0
    ef_construction: usize, // Search width during construction
    ml: f64,               // Level multiplier: 1/ln(M)
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            m: 16,
            m_max0: 32,
            ef_construction: 200,
            ml: 1.0 / (16.0_f64).ln(),
        }
    }
}

/// A node in the HNSW graph.
#[derive(Debug, Clone)]
struct HnswNode {
    doc_id: DocumentId,
    layers: Vec<Vec<usize>>, // neighbors[layer] = vec of node indices
}

/// Multi-layer navigable small-world graph for approximate nearest neighbor search.
#[derive(Debug, Clone)]
struct HnswGraph {
    nodes: Vec<HnswNode>,
    doc_to_node: HashMap<DocumentId, usize>,
    entry_point: Option<usize>,
    max_layer: usize,
    config: HnswConfig,
    deleted_count: usize,
}

impl HnswGraph {
    fn new(config: HnswConfig) -> Self {
        Self {
            nodes: Vec::new(),
            doc_to_node: HashMap::new(),
            entry_point: None,
            max_layer: 0,
            config,
            deleted_count: 0,
        }
    }

    /// Assign a random layer for a new node.
    fn random_level(&self) -> usize {
        let r: f64 = rand::rng().random::<f64>();
        (-r.ln() * self.config.ml).floor() as usize
    }

    /// Compute distance between a query vector and a node (by index).
    fn distance(&self, query: &[f32], node_idx: usize, vectors: &HashMap<DocumentId, Vec<f32>>, metric: DistanceMetric) -> f32 {
        let doc_id = self.nodes[node_idx].doc_id;
        match vectors.get(&doc_id) {
            Some(v) => compute_distance(query, v, metric),
            None => f32::INFINITY,
        }
    }

    /// Greedy search from entry point down to target layer.
    fn search_layer_greedy(
        &self,
        query: &[f32],
        entry: usize,
        target_layer: usize,
        vectors: &HashMap<DocumentId, Vec<f32>>,
        metric: DistanceMetric,
    ) -> usize {
        let mut current = entry;
        let mut current_dist = self.distance(query, current, vectors, metric);

        for layer in (target_layer..=self.max_layer).rev() {
            let mut changed = true;
            while changed {
                changed = false;
                if layer < self.nodes[current].layers.len() {
                    for &neighbor in &self.nodes[current].layers[layer] {
                        let d = self.distance(query, neighbor, vectors, metric);
                        if d < current_dist {
                            current = neighbor;
                            current_dist = d;
                            changed = true;
                        }
                    }
                }
            }
        }
        current
    }

    /// ef-bounded search at a specific layer. Returns top-ef nearest nodes.
    fn search_layer(
        &self,
        query: &[f32],
        entry_points: &[usize],
        ef: usize,
        layer: usize,
        vectors: &HashMap<DocumentId, Vec<f32>>,
        metric: DistanceMetric,
    ) -> Vec<(f32, usize)> {
        use std::collections::HashSet;

        let mut visited = HashSet::new();
        // candidates: min-heap (closest first)
        let mut candidates: BinaryHeap<std::cmp::Reverse<OrdF32Idx>> = BinaryHeap::new();
        // results: max-heap (furthest first for pruning)
        let mut results: BinaryHeap<OrdF32Idx> = BinaryHeap::new();

        for &ep in entry_points {
            let d = self.distance(query, ep, vectors, metric);
            visited.insert(ep);
            candidates.push(std::cmp::Reverse(OrdF32Idx(d, ep)));
            results.push(OrdF32Idx(d, ep));
        }

        while let Some(std::cmp::Reverse(OrdF32Idx(c_dist, c_idx))) = candidates.pop() {
            let furthest_dist = results.peek().map(|r| r.0).unwrap_or(f32::INFINITY);
            if c_dist > furthest_dist {
                break;
            }

            if layer < self.nodes[c_idx].layers.len() {
                for &neighbor in &self.nodes[c_idx].layers[layer] {
                    if visited.insert(neighbor) {
                        let d = self.distance(query, neighbor, vectors, metric);
                        let furthest = results.peek().map(|r| r.0).unwrap_or(f32::INFINITY);
                        if d < furthest || results.len() < ef {
                            candidates.push(std::cmp::Reverse(OrdF32Idx(d, neighbor)));
                            results.push(OrdF32Idx(d, neighbor));
                            if results.len() > ef {
                                results.pop();
                            }
                        }
                    }
                }
            }
        }

        results.into_sorted_vec().into_iter().map(|r| (r.0, r.1)).collect()
    }

    /// Insert a node into the HNSW graph.
    fn insert(&mut self, doc_id: DocumentId, vectors: &HashMap<DocumentId, Vec<f32>>, metric: DistanceMetric) {
        let node_idx = self.nodes.len();
        let level = self.random_level();

        // Push the node first so it's accessible during neighbor pruning
        let node = HnswNode {
            doc_id,
            layers: vec![Vec::new(); level + 1],
        };
        self.nodes.push(node);
        self.doc_to_node.insert(doc_id, node_idx);

        if self.nodes.len() == 1 {
            self.entry_point = Some(node_idx);
            self.max_layer = level;
            return;
        }

        let ep = match self.entry_point {
            Some(ep) if ep != node_idx => ep,
            _ => return,
        };

        let vec = match vectors.get(&doc_id) {
            Some(v) => v.clone(),
            None => return,
        };

        // Greedy descent from top layer to level+1
        let mut current_ep = ep;
        if self.max_layer > level {
            current_ep = self.search_layer_greedy(&vec, ep, level + 1, vectors, metric);
        }

        // Search and connect at each layer from level down to 0
        let mut entry_points = vec![current_ep];
        for l in (0..=level.min(self.max_layer)).rev() {
            let m_max = if l == 0 { self.config.m_max0 } else { self.config.m };
            let neighbors = self.search_layer(&vec, &entry_points, self.config.ef_construction, l, vectors, metric);

            // Select M nearest neighbors, excluding self
            let selected: Vec<usize> = neighbors.iter()
                .filter(|(_, idx)| *idx != node_idx)
                .take(self.config.m)
                .map(|(_, idx)| *idx)
                .collect();

            // Connect node to selected neighbors
            if l < self.nodes[node_idx].layers.len() {
                self.nodes[node_idx].layers[l] = selected.clone();
            }

            // Add backlinks from neighbors to this node
            for &neighbor_idx in &selected {
                if l < self.nodes[neighbor_idx].layers.len() {
                    self.nodes[neighbor_idx].layers[l].push(node_idx);
                    // Prune if over capacity
                    if self.nodes[neighbor_idx].layers[l].len() > m_max {
                        // Keep closest M neighbors
                        let neighbor_doc = self.nodes[neighbor_idx].doc_id;
                        if let Some(nv) = vectors.get(&neighbor_doc) {
                            let mut scored: Vec<(f32, usize)> = self.nodes[neighbor_idx].layers[l]
                                .iter()
                                .filter_map(|&ni| {
                                    let nd = self.nodes[ni].doc_id;
                                    let d = vectors.get(&nd)
                                        .map(|v| compute_distance(nv, v, metric))
                                        .unwrap_or(f32::INFINITY);
                                    Some((d, ni))
                                })
                                .collect();
                            scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                            self.nodes[neighbor_idx].layers[l] = scored.into_iter()
                                .take(m_max)
                                .map(|(_, idx)| idx)
                                .collect();
                        }
                    }
                }
            }

            entry_points = neighbors.iter()
                .filter(|(_, idx)| *idx != node_idx)
                .map(|(_, idx)| *idx)
                .collect();
            if entry_points.is_empty() {
                entry_points = vec![current_ep];
            }
        }

        if level > self.max_layer {
            self.max_layer = level;
            self.entry_point = Some(node_idx);
        }
    }

    /// Mark a node as removed (lazy deletion).
    fn remove(&mut self, doc_id: DocumentId) {
        if let Some(&node_idx) = self.doc_to_node.get(&doc_id) {
            // Collect all neighbors first to avoid borrowing conflict
            let neighbor_list: Vec<usize> = self.nodes[node_idx].layers
                .iter()
                .flat_map(|layer| layer.iter().copied())
                .collect();

            // Remove backlinks from all neighbors
            for neighbor in neighbor_list {
                for l in &mut self.nodes[neighbor].layers {
                    l.retain(|&n| n != node_idx);
                }
            }
            // Clear node's own links
            self.nodes[node_idx].layers.clear();
            self.doc_to_node.remove(&doc_id);
            self.deleted_count += 1;

            // If entry point was removed, find a new one
            if self.entry_point == Some(node_idx) {
                self.entry_point = self.doc_to_node.values().copied().next();
                if let Some(ep) = self.entry_point {
                    self.max_layer = self.nodes[ep].layers.len().saturating_sub(1);
                } else {
                    self.max_layer = 0;
                }
            }
        }
    }

    /// Search for k nearest neighbors.
    fn search(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        vectors: &HashMap<DocumentId, Vec<f32>>,
        metric: DistanceMetric,
    ) -> Vec<(f32, DocumentId)> {
        let ep = match self.entry_point {
            Some(ep) => ep,
            None => return Vec::new(),
        };

        // Greedy descent to layer 0
        let nearest = if self.max_layer > 0 {
            self.search_layer_greedy(query, ep, 1, vectors, metric)
        } else {
            ep
        };

        // ef-bounded search at layer 0
        let results = self.search_layer(query, &[nearest], ef_search.max(k), 0, vectors, metric);

        results.into_iter()
            .filter(|(_, idx)| {
                // Only return nodes that are still live (have an entry in doc_to_node)
                *idx < self.nodes.len() && self.doc_to_node.contains_key(&self.nodes[*idx].doc_id)
            })
            .take(k)
            .map(|(dist, idx)| (dist, self.nodes[idx].doc_id))
            .collect()
    }

    /// Check if the graph should be rebuilt (>20% deleted).
    fn needs_rebuild(&self) -> bool {
        let total = self.nodes.len();
        total > 0 && self.deleted_count * 5 > total
    }
}

/// Wrapper for f32+index to use in BinaryHeap.
#[derive(Debug, Clone)]
struct OrdF32Idx(f32, usize);

impl PartialEq for OrdF32Idx {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

impl Eq for OrdF32Idx {}

impl PartialOrd for OrdF32Idx {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF32Idx {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| self.1.cmp(&other.1))
    }
}

/// Compute distance between two vectors.
fn compute_distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::Cosine => {
            let mut dot = 0.0f32;
            let mut norm_a = 0.0f32;
            let mut norm_b = 0.0f32;
            for i in 0..a.len() {
                dot += a[i] * b[i];
                norm_a += a[i] * a[i];
                norm_b += b[i] * b[i];
            }
            let denom = norm_a.sqrt() * norm_b.sqrt();
            if denom == 0.0 {
                1.0
            } else {
                1.0 - dot / denom
            }
        }
        DistanceMetric::Euclidean => {
            let mut sum = 0.0f32;
            for i in 0..a.len() {
                let d = a[i] - b[i];
                sum += d * d;
            }
            sum.sqrt()
        }
        DistanceMetric::DotProduct => {
            let mut dot = 0.0f32;
            for i in 0..a.len() {
                dot += a[i] * b[i];
            }
            -dot
        }
    }
}

/// Extract a vector (array of numbers) from a JSON value at a field path.
fn extract_vector(data: &Value, field: &str) -> Option<Vec<f32>> {
    let mut current = data;
    for part in field.split('.') {
        current = current.as_object()?.get(part)?;
    }
    let arr = current.as_array()?;
    let mut vec = Vec::with_capacity(arr.len());
    for v in arr {
        vec.push(v.as_f64()? as f32);
    }
    Some(vec)
}

/// A vector index on a collection field.
pub struct VectorIndex {
    pub field: String,
    pub dimension: usize,
    pub metric: DistanceMetric,
    vectors: HashMap<DocumentId, Vec<f32>>,
    hnsw: Option<HnswGraph>,
    flat_threshold: usize,
    hnsw_config: HnswConfig,
}

impl VectorIndex {
    /// Create a new empty vector index.
    pub fn new(field: String, dimension: usize, metric: DistanceMetric) -> Self {
        Self {
            field,
            dimension,
            metric,
            vectors: HashMap::new(),
            hnsw: None,
            flat_threshold: 1000,
            hnsw_config: HnswConfig::default(),
        }
    }

    /// Parse metric from string, defaulting to Cosine.
    pub fn parse_metric(s: &str) -> DistanceMetric {
        DistanceMetric::from_str(s).unwrap_or(DistanceMetric::Cosine)
    }

    /// Get the metric as a string.
    pub fn metric_str(&self) -> &'static str {
        self.metric.as_str()
    }

    /// Number of indexed vectors.
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Returns true if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    /// Insert a document's vector into the index.
    pub fn insert(&mut self, doc_id: DocumentId, data: &Value) -> Result<(), String> {
        let vec = match extract_vector(data, &self.field) {
            Some(v) => v,
            None => return Ok(()), // Field not present or not an array — skip
        };

        if vec.len() != self.dimension {
            return Err(format!(
                "vector dimension mismatch: expected {}, got {}",
                self.dimension,
                vec.len()
            ));
        }

        self.vectors.insert(doc_id, vec);

        // Build/update HNSW if above threshold
        if self.vectors.len() >= self.flat_threshold {
            if self.hnsw.is_none() {
                self.rebuild_hnsw();
            } else if let Some(ref mut hnsw) = self.hnsw {
                hnsw.insert(doc_id, &self.vectors, self.metric);
            }
        }

        Ok(())
    }

    /// Remove a document from the index.
    pub fn remove(&mut self, doc_id: DocumentId) {
        self.vectors.remove(&doc_id);
        if let Some(ref mut hnsw) = self.hnsw {
            hnsw.remove(doc_id);
            if hnsw.needs_rebuild() {
                self.rebuild_hnsw();
            }
        }

        // Drop HNSW if we fell below threshold
        if self.vectors.len() < self.flat_threshold / 2 {
            self.hnsw = None;
        }
    }

    /// Clear the entire index (used during compaction).
    pub fn clear(&mut self) {
        self.vectors.clear();
        self.hnsw = None;
    }

    /// Search for the k nearest neighbors to the query vector.
    pub fn search(&self, query: &[f32], k: usize, ef_search: Option<usize>) -> Result<Vec<VectorSearchResult>, String> {
        if query.len() != self.dimension {
            return Err(format!(
                "query vector dimension mismatch: expected {}, got {}",
                self.dimension,
                query.len()
            ));
        }

        if self.vectors.is_empty() {
            return Ok(Vec::new());
        }

        let results = if self.vectors.len() < self.flat_threshold || self.hnsw.is_none() {
            self.flat_search(query, k)
        } else {
            let ef = ef_search.unwrap_or(50).max(k);
            self.hnsw_search(query, k, ef)
        };

        Ok(results)
    }

    /// Brute-force exact KNN search.
    fn flat_search(&self, query: &[f32], k: usize) -> Vec<VectorSearchResult> {
        let mut heap: BinaryHeap<OrdF32Id> = BinaryHeap::new();

        for (&doc_id, vec) in &self.vectors {
            let dist = compute_distance(query, vec, self.metric);
            if heap.len() < k {
                heap.push(OrdF32Id(dist, doc_id));
            } else if heap.peek().is_some_and(|top| dist < top.0) {
                heap.pop();
                heap.push(OrdF32Id(dist, doc_id));
            }
        }

        let mut results: Vec<VectorSearchResult> = heap.into_sorted_vec()
            .into_iter()
            .map(|OrdF32Id(dist, doc_id)| VectorSearchResult {
                doc_id,
                distance: dist,
                similarity: self.metric.to_similarity(dist),
            })
            .collect();
        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// HNSW approximate KNN search.
    fn hnsw_search(&self, query: &[f32], k: usize, ef_search: usize) -> Vec<VectorSearchResult> {
        let hnsw = match &self.hnsw {
            Some(h) => h,
            None => return self.flat_search(query, k),
        };

        let results = hnsw.search(query, k, ef_search, &self.vectors, self.metric);
        results.into_iter()
            .map(|(dist, doc_id)| VectorSearchResult {
                doc_id,
                distance: dist,
                similarity: self.metric.to_similarity(dist),
            })
            .collect()
    }

    /// Rebuild the HNSW graph from scratch.
    fn rebuild_hnsw(&mut self) {
        let mut graph = HnswGraph::new(self.hnsw_config.clone());
        let doc_ids: Vec<DocumentId> = self.vectors.keys().copied().collect();
        for doc_id in doc_ids {
            graph.insert(doc_id, &self.vectors, self.metric);
        }
        self.hnsw = Some(graph);
    }

    // -----------------------------------------------------------------------
    // Binary persistence (.vidx)
    // -----------------------------------------------------------------------

    /// Write the vector index to a binary stream.
    pub fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        // Field name
        let field_bytes = self.field.as_bytes();
        w.write_all(&(field_bytes.len() as u32).to_le_bytes())?;
        w.write_all(field_bytes)?;

        // Dimension
        w.write_all(&(self.dimension as u32).to_le_bytes())?;

        // Metric
        w.write_all(&[self.metric.to_u8()])?;

        // Vector count
        w.write_all(&(self.vectors.len() as u64).to_le_bytes())?;

        // Per-vector: doc_id(u64) + f32 * dim
        for (&doc_id, vec) in &self.vectors {
            w.write_all(&doc_id.to_le_bytes())?;
            for &v in vec {
                w.write_all(&v.to_le_bytes())?;
            }
        }

        Ok(())
    }

    /// Read a vector index from a binary stream.
    pub fn read_from<R: Read>(r: &mut R) -> io::Result<Self> {
        // Field name
        let mut len_buf = [0u8; 4];
        r.read_exact(&mut len_buf)?;
        let field_len = u32::from_le_bytes(len_buf) as usize;
        let mut field_bytes = vec![0u8; field_len];
        r.read_exact(&mut field_bytes)?;
        let field = String::from_utf8(field_bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Dimension
        r.read_exact(&mut len_buf)?;
        let dimension = u32::from_le_bytes(len_buf) as usize;

        // Metric
        let mut metric_buf = [0u8; 1];
        r.read_exact(&mut metric_buf)?;
        let metric = DistanceMetric::from_u8(metric_buf[0])
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid metric"))?;

        // Vector count
        let mut count_buf = [0u8; 8];
        r.read_exact(&mut count_buf)?;
        let count = u64::from_le_bytes(count_buf) as usize;

        let mut vectors = HashMap::with_capacity(count);
        let mut id_buf = [0u8; 8];
        let mut f32_buf = [0u8; 4];

        for _ in 0..count {
            r.read_exact(&mut id_buf)?;
            let doc_id = u64::from_le_bytes(id_buf);
            let mut vec = Vec::with_capacity(dimension);
            for _ in 0..dimension {
                r.read_exact(&mut f32_buf)?;
                vec.push(f32::from_le_bytes(f32_buf));
            }
            vectors.insert(doc_id, vec);
        }

        let flat_threshold = 1000;
        let hnsw_config = HnswConfig::default();

        let mut idx = Self {
            field,
            dimension,
            metric,
            vectors,
            hnsw: None,
            flat_threshold,
            hnsw_config,
        };

        // Rebuild HNSW if above threshold
        if idx.vectors.len() >= idx.flat_threshold {
            idx.rebuild_hnsw();
        }

        Ok(idx)
    }
}

/// Wrapper for f32+DocumentId to use in BinaryHeap (max-heap by distance).
#[derive(Debug, Clone)]
struct OrdF32Id(f32, DocumentId);

impl PartialEq for OrdF32Id {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

impl Eq for OrdF32Id {}

impl PartialOrd for OrdF32Id {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF32Id {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| self.1.cmp(&other.1))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_cosine_distance() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let d = compute_distance(&a, &b, DistanceMetric::Cosine);
        assert!((d - 1.0).abs() < 1e-6, "orthogonal vectors should have cosine distance ~1.0");

        let c = vec![1.0, 0.0, 0.0];
        let d2 = compute_distance(&a, &c, DistanceMetric::Cosine);
        assert!(d2.abs() < 1e-6, "identical vectors should have cosine distance ~0.0");
    }

    #[test]
    fn test_euclidean_distance() {
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        let d = compute_distance(&a, &b, DistanceMetric::Euclidean);
        assert!((d - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_dot_product_distance() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        let d = compute_distance(&a, &b, DistanceMetric::DotProduct);
        assert!((d - (-32.0)).abs() < 1e-6, "dot product distance should be -dot(a,b)");
    }

    #[test]
    fn test_similarity_conversion() {
        let sim = DistanceMetric::Cosine.to_similarity(0.0);
        assert!((sim - 1.0).abs() < 1e-6, "cosine distance 0 should give similarity 1");

        let sim = DistanceMetric::Euclidean.to_similarity(0.0);
        assert!((sim - 1.0).abs() < 1e-6, "euclidean distance 0 should give similarity 1");
    }

    #[test]
    fn test_flat_search_exact() {
        let mut idx = VectorIndex::new("embedding".to_string(), 3, DistanceMetric::Cosine);

        for i in 0..10u64 {
            let v = vec![i as f32, 0.0, 0.0];
            let doc = json!({"_id": i, "embedding": v});
            idx.insert(i, &doc).unwrap();
        }

        let query = vec![5.0, 0.0, 0.0];
        let results = idx.search(&query, 3, None).unwrap();
        assert_eq!(results.len(), 3);
        // All non-zero vectors with the same direction should have distance ~0
        // (cosine doesn't care about magnitude for same-direction vectors)
        for r in &results {
            assert!(r.doc_id > 0, "should not return zero vector");
        }
    }

    #[test]
    fn test_dimension_mismatch() {
        let mut idx = VectorIndex::new("vec".to_string(), 3, DistanceMetric::Cosine);
        let doc = json!({"vec": [1.0, 2.0]});
        let result = idx.insert(1, &doc);
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_remove_lifecycle() {
        let mut idx = VectorIndex::new("vec".to_string(), 2, DistanceMetric::Euclidean);

        let doc1 = json!({"vec": [1.0, 0.0]});
        let doc2 = json!({"vec": [0.0, 1.0]});
        idx.insert(1, &doc1).unwrap();
        idx.insert(2, &doc2).unwrap();
        assert_eq!(idx.len(), 2);

        idx.remove(1);
        assert_eq!(idx.len(), 1);

        let results = idx.search(&[0.0, 1.0], 5, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, 2);
    }

    #[test]
    fn test_binary_roundtrip() {
        let mut idx = VectorIndex::new("embedding".to_string(), 3, DistanceMetric::Euclidean);
        for i in 0..50u64 {
            let doc = json!({"embedding": [i as f64 * 0.1, i as f64 * 0.2, i as f64 * 0.3]});
            idx.insert(i, &doc).unwrap();
        }

        let mut buf = Vec::new();
        idx.write_to(&mut buf).unwrap();

        let mut cursor = io::Cursor::new(&buf);
        let idx2 = VectorIndex::read_from(&mut cursor).unwrap();

        assert_eq!(idx2.field, "embedding");
        assert_eq!(idx2.dimension, 3);
        assert_eq!(idx2.metric, DistanceMetric::Euclidean);
        assert_eq!(idx2.len(), 50);

        // Search should return same results
        let query = vec![2.5, 5.0, 7.5];
        let r1 = idx.search(&query, 3, None).unwrap();
        let r2 = idx2.search(&query, 3, None).unwrap();
        assert_eq!(r1.len(), r2.len());
        for (a, b) in r1.iter().zip(r2.iter()) {
            assert_eq!(a.doc_id, b.doc_id);
            assert!((a.distance - b.distance).abs() < 1e-6);
        }
    }

    #[test]
    fn test_missing_field_skipped() {
        let mut idx = VectorIndex::new("embedding".to_string(), 3, DistanceMetric::Cosine);
        let doc = json!({"name": "Alice"});
        assert!(idx.insert(1, &doc).is_ok());
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn test_hnsw_basic() {
        // Build index with enough vectors to trigger HNSW
        let dim = 8;
        let n = 1500; // above flat_threshold of 1000
        let mut idx = VectorIndex::new("vec".to_string(), dim, DistanceMetric::Euclidean);

        for i in 0..n as u64 {
            let v: Vec<f64> = (0..dim).map(|d| i as f64 + d as f64 * 0.01).collect();
            let doc = json!({"vec": v});
            idx.insert(i, &doc).unwrap();
        }
        assert!(idx.hnsw.is_some(), "HNSW should be built above threshold");
        assert_eq!(idx.len(), n);

        // Search should return results
        let query: Vec<f32> = (0..dim).map(|d| 500.0 + d as f32 * 0.01).collect();
        let k = 10;

        let hnsw_results = idx.search(&query, k, Some(200)).unwrap();
        assert!(!hnsw_results.is_empty(), "HNSW search should return results");
        assert!(hnsw_results.len() <= k, "should return at most k results");

        // Results should be sorted by distance
        for w in hnsw_results.windows(2) {
            assert!(w[0].distance <= w[1].distance, "results should be sorted by distance");
        }

        // The closest result should be reasonably near our query vector
        // (query is at 500.0, so doc 500 should be nearby)
        assert!(hnsw_results[0].distance < 10.0, "closest result should be near the query");
    }

    #[test]
    fn test_extract_vector_nested() {
        let doc = json!({"data": {"embedding": [1.0, 2.0, 3.0]}});
        let v = extract_vector(&doc, "data.embedding").unwrap();
        assert_eq!(v, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_metric_from_str() {
        assert_eq!(DistanceMetric::from_str("cosine"), Some(DistanceMetric::Cosine));
        assert_eq!(DistanceMetric::from_str("euclidean"), Some(DistanceMetric::Euclidean));
        assert_eq!(DistanceMetric::from_str("dotproduct"), Some(DistanceMetric::DotProduct));
        assert_eq!(DistanceMetric::from_str("dot_product"), Some(DistanceMetric::DotProduct));
        assert_eq!(DistanceMetric::from_str("invalid"), None);
    }
}
