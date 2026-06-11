//! GPU-accelerated vector distance computation using wgpu compute shaders.
//!
//! Dispatches parallel distance calculations on the GPU for brute-force
//! vector search. Each GPU thread computes the distance between the query
//! vector and one stored vector.
//!
//! Enabled with `--features gpu`. Cross-platform: Metal (macOS), Vulkan (Linux), DX12 (Windows).

use std::sync::Arc;

use wgpu::util::DeviceExt;

use crate::vector::DistanceMetric;

/// WGSL compute shader source — three entry points for three distance metrics.
const SHADER_SOURCE: &str = r#"
struct Params {
    count: u32,
    dimension: u32,
}

@group(0) @binding(0) var<storage, read> query: array<f32>;
@group(0) @binding(1) var<storage, read> vectors: array<f32>;
@group(0) @binding(2) var<storage, read_write> distances: array<f32>;
@group(0) @binding(3) var<uniform> params: Params;

@compute @workgroup_size(256)
fn cosine_distance(@builtin(global_invocation_id) id: vec3u) {
    let idx = id.x;
    if (idx >= params.count) { return; }

    let offset = idx * params.dimension;
    var dot: f32 = 0.0;
    var norm_a: f32 = 0.0;
    var norm_b: f32 = 0.0;

    for (var i: u32 = 0u; i < params.dimension; i = i + 1u) {
        let a = query[i];
        let b = vectors[offset + i];
        dot = dot + a * b;
        norm_a = norm_a + a * a;
        norm_b = norm_b + b * b;
    }

    let denom = sqrt(norm_a) * sqrt(norm_b);
    if (denom == 0.0) {
        distances[idx] = 1.0;
    } else {
        distances[idx] = 1.0 - dot / denom;
    }
}

@compute @workgroup_size(256)
fn euclidean_distance(@builtin(global_invocation_id) id: vec3u) {
    let idx = id.x;
    if (idx >= params.count) { return; }

    let offset = idx * params.dimension;
    var sum: f32 = 0.0;

    for (var i: u32 = 0u; i < params.dimension; i = i + 1u) {
        let d = query[i] - vectors[offset + i];
        sum = sum + d * d;
    }

    distances[idx] = sqrt(sum);
}

@compute @workgroup_size(256)
fn dot_product_distance(@builtin(global_invocation_id) id: vec3u) {
    let idx = id.x;
    if (idx >= params.count) { return; }

    let offset = idx * params.dimension;
    var dot: f32 = 0.0;

    for (var i: u32 = 0u; i < params.dimension; i = i + 1u) {
        dot = dot + query[i] * vectors[offset + i];
    }

    distances[idx] = -dot;
}
"#;

/// GPU compute backend for vector distance calculations.
pub struct GpuCompute {
    device: wgpu::Device,
    queue: wgpu::Queue,
    cosine_pipeline: wgpu::ComputePipeline,
    euclidean_pipeline: wgpu::ComputePipeline,
    dot_pipeline: wgpu::ComputePipeline,
    bind_group_layout: wgpu::BindGroupLayout,
    backend_name: String,
}

impl GpuCompute {
    /// Initialize the GPU compute backend.
    /// Returns `None` if no suitable GPU adapter is found.
    pub fn init() -> Option<Arc<Self>> {
        let instance = wgpu::Instance::new(&wgpu::InstanceDescriptor {
            backends: wgpu::Backends::all(),
            ..Default::default()
        });

        let adapter = pollster::block_on(instance.request_adapter(&wgpu::RequestAdapterOptions {
            power_preference: wgpu::PowerPreference::HighPerformance,
            compatible_surface: None,
            force_fallback_adapter: false,
        }))?;

        let backend_name = format!("{:?}", adapter.get_info().backend);

        let (device, queue) = pollster::block_on(adapter.request_device(
            &wgpu::DeviceDescriptor {
                label: Some("oxidb-gpu"),
                required_features: wgpu::Features::empty(),
                required_limits: wgpu::Limits::default(),
                ..Default::default()
            },
            None,
        ))
        .ok()?;

        let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("vector_distance"),
            source: wgpu::ShaderSource::Wgsl(SHADER_SOURCE.into()),
        });

        let bind_group_layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("vector_bgl"),
            entries: &[
                // query: storage read
                wgpu::BindGroupLayoutEntry {
                    binding: 0,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: true },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                // vectors: storage read
                wgpu::BindGroupLayoutEntry {
                    binding: 1,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: true },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                // distances: storage read_write
                wgpu::BindGroupLayoutEntry {
                    binding: 2,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: false },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                // params: uniform
                wgpu::BindGroupLayoutEntry {
                    binding: 3,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Uniform,
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
            ],
        });

        let pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("vector_pl"),
            bind_group_layouts: &[&bind_group_layout],
            push_constant_ranges: &[],
        });

        let create_pipeline = |entry: &str| {
            device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
                label: Some(entry),
                layout: Some(&pipeline_layout),
                module: &shader,
                entry_point: Some(entry),
                compilation_options: Default::default(),
                cache: None,
            })
        };

        let cosine_pipeline = create_pipeline("cosine_distance");
        let euclidean_pipeline = create_pipeline("euclidean_distance");
        let dot_pipeline = create_pipeline("dot_product_distance");

        Some(Arc::new(Self {
            device,
            queue,
            cosine_pipeline,
            euclidean_pipeline,
            dot_pipeline,
            bind_group_layout,
            backend_name,
        }))
    }

    /// GPU backend name (e.g., "Metal", "Vulkan", "Dx12").
    pub fn backend(&self) -> &str {
        &self.backend_name
    }

    /// Compute distances between a query vector and N stored vectors on the GPU.
    ///
    /// `vectors` must be a flat contiguous array of N * dimension f32 values.
    /// Returns a Vec<f32> of N distances.
    pub fn compute_distances(
        &self,
        query: &[f32],
        vectors: &[f32],
        count: u32,
        dimension: u32,
        metric: DistanceMetric,
    ) -> Vec<f32> {
        let pipeline = match metric {
            DistanceMetric::Cosine => &self.cosine_pipeline,
            DistanceMetric::Euclidean => &self.euclidean_pipeline,
            DistanceMetric::DotProduct => &self.dot_pipeline,
        };

        // Create GPU buffers
        let query_buf = self
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("query"),
                contents: bytemuck::cast_slice(query),
                usage: wgpu::BufferUsages::STORAGE,
            });

        let vectors_buf = self
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("vectors"),
                contents: bytemuck::cast_slice(vectors),
                usage: wgpu::BufferUsages::STORAGE,
            });

        let distances_size = (count as u64) * 4; // f32 = 4 bytes
        let distances_buf = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("distances"),
            size: distances_size,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
            mapped_at_creation: false,
        });

        let params = [count, dimension];
        let params_buf = self
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("params"),
                contents: bytemuck::cast_slice(&params),
                usage: wgpu::BufferUsages::UNIFORM,
            });

        // Staging buffer to read results back to CPU
        let staging_buf = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("staging"),
            size: distances_size,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        let bind_group = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("vector_bg"),
            layout: &self.bind_group_layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: query_buf.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: vectors_buf.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: distances_buf.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 3,
                    resource: params_buf.as_entire_binding(),
                },
            ],
        });

        // Dispatch compute shader
        let mut encoder = self.device.create_command_encoder(&Default::default());
        {
            let mut pass = encoder.begin_compute_pass(&Default::default());
            pass.set_pipeline(pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            let workgroups = (count + 255) / 256;
            pass.dispatch_workgroups(workgroups, 1, 1);
        }

        // Copy results to staging buffer
        encoder.copy_buffer_to_buffer(&distances_buf, 0, &staging_buf, 0, distances_size);
        self.queue.submit(Some(encoder.finish()));

        // Read back results
        let slice = staging_buf.slice(..);
        let (tx, rx) = std::sync::mpsc::channel();
        slice.map_async(wgpu::MapMode::Read, move |result| {
            let _ = tx.send(result);
        });
        self.device.poll(wgpu::Maintain::Wait);

        match rx.recv() {
            Ok(Ok(())) => {
                let data = slice.get_mapped_range();
                let result: Vec<f32> = bytemuck::cast_slice(&data).to_vec();
                drop(data);
                staging_buf.unmap();
                result
            }
            _ => {
                // Fallback: return max distances
                vec![f32::MAX; count as usize]
            }
        }
    }
}
