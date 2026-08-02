use bytemuck::{Pod, Zeroable};
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wgpu::util::DeviceExt;

// ---------------------------------------------------------------------------
// Colors (matching the HTML dashboard)
// ---------------------------------------------------------------------------
const BG: [f32; 4] = [0.027, 0.031, 0.047, 1.0];
const SURFACE: [f32; 4] = [0.055, 0.063, 0.090, 1.0];
const SURFACE2: [f32; 4] = [0.082, 0.094, 0.125, 1.0];
const BORDER: [f32; 4] = [0.118, 0.133, 0.188, 1.0];
const GREEN: [f32; 4] = [0.0, 0.902, 0.463, 1.0];
const RED: [f32; 4] = [1.0, 0.090, 0.267, 1.0];
const TEXT: [f32; 4] = [0.784, 0.804, 0.847, 1.0];
const TEXT_DIM: [f32; 4] = [0.353, 0.380, 0.471, 1.0];
const ACCENT: [f32; 4] = [0.267, 0.541, 1.0, 1.0];
const ACCENT_BG: [f32; 4] = [0.267, 0.541, 1.0, 0.12];
const BINANCE_CLR: [f32; 4] = [0.941, 0.725, 0.043, 1.0];
const COINBASE_CLR: [f32; 4] = [0.0, 0.322, 1.0, 1.0];
const KRAKEN_CLR: [f32; 4] = [0.482, 0.380, 1.0, 1.0];

const DEPTH: usize = 15;
const HEADER_H: f32 = 56.0;
const TABS_H: f32 = 56.0;
const STATS_H: f32 = 36.0;
const EXCH_HDR_H: f32 = 42.0;
const BOOK_HDR_H: f32 = 28.0;
const ROW_H: f32 = 24.0;
const CHART_H: f32 = 540.0;
const CHART_24H_H: f32 = 200.0;
const CHART_LABEL_H: f32 = 20.0;
const CHART_BG: [f32; 4] = [0.035, 0.039, 0.059, 1.0];
const CANDLE_TICKS: usize = 10;
const MAX_CANDLES: usize = 120;
const MAX_CANDLES_24H: usize = 288;
const FADE_MS: f64 = 200.0;

const PAIRS: &[&str] = &["btcusdt", "ethusdt", "solusdt", "xrpusdt", "bnbusdt", "dogeusdt", "trumpusdt"];
const EXCHANGES: &[&str] = &["binance", "coinbase", "kraken"];

fn exchange_has_pair(exchange: &str, pair: &str) -> bool {
    // BNB only on Binance
    if pair == "bnbusdt" && exchange != "binance" {
        return false;
    }
    true
}

// ---------------------------------------------------------------------------
// GPU data types
// ---------------------------------------------------------------------------
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct RectInstance {
    pos: [f32; 2],
    size: [f32; 2],
    color: [f32; 4],
    /// Gradient direction: 0=none, 1=fade left→right, -1=fade right→left
    gradient: f32,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct GlyphInstance {
    pos: [f32; 2],
    size: [f32; 2],
    uv_pos: [f32; 2],
    uv_size: [f32; 2],
    color: [f32; 4],
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct Uniforms {
    screen_size: [f32; 2],
    _pad: [f32; 2],
}

// ---------------------------------------------------------------------------
// Order book data
// ---------------------------------------------------------------------------
#[derive(Clone, Debug, Deserialize)]
struct Level {
    price: f64,
    qty: f64,
}

#[derive(Clone, Debug, Deserialize)]
struct OrderBook {
    exchange: String,
    pair: String,
    bids: Vec<Level>,
    asks: Vec<Level>,
}

#[derive(Clone, Debug)]
struct Candle {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------
struct AppState {
    books: HashMap<String, OrderBook>,
    active_pair: String,
    msg_count: u32,
    start_time: f64,
    connected: bool,
    mouse_x: f32,
    mouse_y: f32,
    candles: HashMap<String, VecDeque<Candle>>,
    candle_buf: HashMap<String, Vec<f64>>,
    bar_anim: HashMap<String, (Vec<f64>, Vec<f64>)>,
    last_frame_time: f64,
    loading: bool,
    history_24h: HashMap<String, VecDeque<Candle>>, // key: "exchange/pair"
    loaded_pairs: HashSet<String>, // pairs already fetched (even if empty)
}

impl AppState {
    fn new() -> Self {
        Self {
            books: HashMap::new(),
            active_pair: "btcusdt".into(),
            msg_count: 0,
            start_time: js_sys::Date::now() / 1000.0,
            connected: false,
            mouse_x: -1.0,
            mouse_y: -1.0,
            candles: HashMap::new(),
            candle_buf: HashMap::new(),
            bar_anim: HashMap::new(),
            last_frame_time: js_sys::Date::now(),
            loading: false,
            history_24h: HashMap::new(),
            loaded_pairs: HashSet::new(),
        }
    }

    fn handle_message(&mut self, data: &str) {
        if let Ok(ob) = serde_json::from_str::<OrderBook>(data) {
            let key = format!("{}/{}", ob.exchange, ob.pair);
            if let (Some(bid), Some(ask)) = (ob.bids.first(), ob.asks.first()) {
                let mid = (bid.price + ask.price) / 2.0;
                let buf = self.candle_buf.entry(key.clone()).or_default();
                buf.push(mid);
                if buf.len() >= CANDLE_TICKS {
                    let candle = Candle {
                        open: buf[0],
                        close: *buf.last().unwrap(),
                        high: buf.iter().copied().fold(f64::MIN, f64::max),
                        low: buf.iter().copied().fold(f64::MAX, f64::min),
                    };
                    let candles = self.candles.entry(key.clone()).or_default();
                    candles.push_back(candle);
                    if candles.len() > MAX_CANDLES { candles.pop_front(); }
                    buf.clear();
                }
            }
            self.books.insert(key, ob);
            self.msg_count += 1;
        }
    }

    fn get_book(&self, exchange: &str) -> Option<&OrderBook> {
        self.books.get(&format!("{}/{}", exchange, self.active_pair))
    }

    fn best_price(&self, pair: &str) -> Option<f64> {
        for ex in EXCHANGES {
            if let Some(book) = self.books.get(&format!("{}/{}", ex, pair)) {
                if let Some(bid) = book.bids.first() {
                    return Some(bid.price);
                }
            }
        }
        None
    }

    fn get_candles(&self, exchange: &str) -> Option<&VecDeque<Candle>> {
        self.candles.get(&format!("{}/{}", exchange, self.active_pair))
    }

    fn get_candles_24h(&self, exchange: &str) -> Option<&VecDeque<Candle>> {
        self.history_24h.get(&format!("{}/{}", exchange, self.active_pair))
    }

    fn update_animations(&mut self) {
        let now = js_sys::Date::now();
        let dt = (now - self.last_frame_time).max(0.1);
        self.last_frame_time = now;
        let factor = 1.0 - (-dt / FADE_MS).exp();

        for exchange in EXCHANGES {
            let key = format!("{}/{}", exchange, self.active_pair);
            let mut bid_targets = [0.0f64; DEPTH];
            let mut ask_targets = [0.0f64; DEPTH];
            if let Some(book) = self.books.get(&key) {
                for (j, level) in book.bids.iter().take(DEPTH).enumerate() {
                    bid_targets[j] = level.qty;
                }
                for (j, level) in book.asks.iter().take(DEPTH).enumerate() {
                    ask_targets[j] = level.qty;
                }
            }
            let anim = self.bar_anim.entry(key).or_insert_with(|| (vec![0.0; DEPTH], vec![0.0; DEPTH]));
            for j in 0..DEPTH {
                anim.0[j] += (bid_targets[j] - anim.0[j]) * factor;
                anim.1[j] += (ask_targets[j] - anim.1[j]) * factor;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Font atlas
// ---------------------------------------------------------------------------
struct GlyphMetrics {
    uv_x: f32,
    uv_y: f32,
    uv_w: f32,
    uv_h: f32,
}

struct FontAtlas {
    glyph_map: HashMap<char, GlyphMetrics>,
    cell_w: f32,
    cell_h: f32,
}

fn create_font_atlas(device: &wgpu::Device, queue: &wgpu::Queue) -> (wgpu::Texture, FontAtlas) {
    let document = web_sys::window().unwrap().document().unwrap();
    let canvas: web_sys::HtmlCanvasElement = document
        .create_element("canvas")
        .unwrap()
        .dyn_into()
        .unwrap();

    let font_size = 20.0_f64;
    let cell_w: u32 = 12;
    let cell_h: u32 = 24;
    let cols: u32 = 16;
    let rows: u32 = 6;
    let atlas_w = cols * cell_w;
    let atlas_h = rows * cell_h;

    canvas.set_width(atlas_w);
    canvas.set_height(atlas_h);

    let ctx: web_sys::CanvasRenderingContext2d = canvas
        .get_context("2d")
        .unwrap()
        .unwrap()
        .dyn_into()
        .unwrap();

    ctx.clear_rect(0.0, 0.0, atlas_w as f64, atlas_h as f64);
    ctx.set_font(&format!("{}px monospace", font_size));
    ctx.set_fill_style_str("white");
    ctx.set_text_baseline("top");

    let mut glyph_map = HashMap::new();

    for i in 0..96u32 {
        let ch = (i + 32) as u8 as char;
        let col = i % cols;
        let row = i / cols;
        let x = col * cell_w;
        let y = row * cell_h;

        let _ = ctx.fill_text(&ch.to_string(), x as f64, y as f64 + 2.0);

        glyph_map.insert(
            ch,
            GlyphMetrics {
                uv_x: x as f32 / atlas_w as f32,
                uv_y: y as f32 / atlas_h as f32,
                uv_w: cell_w as f32 / atlas_w as f32,
                uv_h: cell_h as f32 / atlas_h as f32,
            },
        );
    }

    let image_data = ctx
        .get_image_data(0.0, 0.0, atlas_w as f64, atlas_h as f64)
        .unwrap();
    let rgba = image_data.data();

    // Extract alpha channel for R8Unorm texture
    let mut alpha = vec![0u8; (atlas_w * atlas_h) as usize];
    for i in 0..alpha.len() {
        alpha[i] = rgba[i * 4 + 3];
    }

    let size = wgpu::Extent3d {
        width: atlas_w,
        height: atlas_h,
        depth_or_array_layers: 1,
    };

    let texture = device.create_texture(&wgpu::TextureDescriptor {
        label: Some("font_atlas"),
        size,
        mip_level_count: 1,
        sample_count: 1,
        dimension: wgpu::TextureDimension::D2,
        format: wgpu::TextureFormat::R8Unorm,
        usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
        view_formats: &[],
    });

    queue.write_texture(
        wgpu::ImageCopyTexture {
            texture: &texture,
            mip_level: 0,
            origin: wgpu::Origin3d::ZERO,
            aspect: wgpu::TextureAspect::All,
        },
        &alpha,
        wgpu::ImageDataLayout {
            offset: 0,
            bytes_per_row: Some(atlas_w),
            rows_per_image: None,
        },
        size,
    );

    let atlas = FontAtlas {
        glyph_map,
        cell_w: cell_w as f32,
        cell_h: cell_h as f32,
    };
    (texture, atlas)
}

// ---------------------------------------------------------------------------
// WGSL shaders
// ---------------------------------------------------------------------------
const RECT_SHADER: &str = r#"
struct Uniforms { screen_size: vec2<f32> };
@group(0) @binding(0) var<uniform> u: Uniforms;

struct Inst {
    @location(0) pos: vec2<f32>,
    @location(1) sz: vec2<f32>,
    @location(2) col: vec4<f32>,
    @location(3) grad: f32,
};
struct Out {
    @builtin(position) pos: vec4<f32>,
    @location(0) col: vec4<f32>,
    @location(1) grad: f32,
    @location(2) uv_x: f32,
};

@vertex fn vs(@builtin(vertex_index) vi: u32, i: Inst) -> Out {
    let c = array<vec2<f32>,6>(
        vec2(0.,0.), vec2(1.,0.), vec2(0.,1.),
        vec2(1.,0.), vec2(1.,1.), vec2(0.,1.));
    let p = i.pos + c[vi] * i.sz;
    let n = vec2(p.x / u.screen_size.x * 2. - 1., 1. - p.y / u.screen_size.y * 2.);
    var o: Out;
    o.pos = vec4(n, 0., 1.);
    o.col = i.col;
    o.grad = i.grad;
    o.uv_x = c[vi].x;
    return o;
}

@fragment fn fs(i: Out) -> @location(0) vec4<f32> {
    if (i.grad > 0.5) {
        // Fade left(transparent) → right(solid)
        let t = 0.08 + i.uv_x * 0.92;
        return vec4(i.col.rgb, i.col.a * t);
    } else if (i.grad < -0.5) {
        // Fade right(transparent) → left(solid)
        let t = 0.08 + (1.0 - i.uv_x) * 0.92;
        return vec4(i.col.rgb, i.col.a * t);
    }
    return i.col;
}
"#;

const TEXT_SHADER: &str = r#"
struct Uniforms { screen_size: vec2<f32> };
@group(0) @binding(0) var<uniform> u: Uniforms;
@group(1) @binding(0) var tex: texture_2d<f32>;
@group(1) @binding(1) var samp: sampler;

struct Inst {
    @location(0) pos: vec2<f32>,
    @location(1) sz: vec2<f32>,
    @location(2) uv_pos: vec2<f32>,
    @location(3) uv_sz: vec2<f32>,
    @location(4) col: vec4<f32>,
};
struct Out {
    @builtin(position) pos: vec4<f32>,
    @location(0) uv: vec2<f32>,
    @location(1) col: vec4<f32>,
};

@vertex fn vs(@builtin(vertex_index) vi: u32, i: Inst) -> Out {
    let c = array<vec2<f32>,6>(
        vec2(0.,0.), vec2(1.,0.), vec2(0.,1.),
        vec2(1.,0.), vec2(1.,1.), vec2(0.,1.));
    let p = i.pos + c[vi] * i.sz;
    let n = vec2(p.x / u.screen_size.x * 2. - 1., 1. - p.y / u.screen_size.y * 2.);
    var o: Out;
    o.pos = vec4(n, 0., 1.);
    o.uv = i.uv_pos + c[vi] * i.uv_sz;
    o.col = i.col;
    return o;
}

@fragment fn fs(i: Out) -> @location(0) vec4<f32> {
    let a = textureSample(tex, samp, i.uv).r;
    return vec4(i.col.rgb, i.col.a * a);
}
"#;

// ---------------------------------------------------------------------------
// Renderer
// ---------------------------------------------------------------------------
struct Renderer {
    surface: wgpu::Surface<'static>,
    device: wgpu::Device,
    queue: wgpu::Queue,
    config: wgpu::SurfaceConfiguration,
    rect_pipeline: wgpu::RenderPipeline,
    text_pipeline: wgpu::RenderPipeline,
    uniform_buffer: wgpu::Buffer,
    uniform_bg: wgpu::BindGroup,
    atlas_bg: wgpu::BindGroup,
    atlas: FontAtlas,
    width: u32,
    height: u32,
}

impl Renderer {
    async fn new(canvas: web_sys::HtmlCanvasElement, width: u32, height: u32) -> Self {
        let instance = wgpu::Instance::new(wgpu::InstanceDescriptor {
            backends: wgpu::Backends::BROWSER_WEBGPU | wgpu::Backends::GL,
            ..Default::default()
        });

        let surface = instance
            .create_surface(wgpu::SurfaceTarget::Canvas(canvas))
            .expect("create_surface");

        let adapter = instance
            .request_adapter(&wgpu::RequestAdapterOptions {
                power_preference: wgpu::PowerPreference::HighPerformance,
                compatible_surface: Some(&surface),
                force_fallback_adapter: false,
            })
            .await
            .expect("no adapter");

        let (device, queue) = adapter
            .request_device(
                &wgpu::DeviceDescriptor {
                    label: Some("dev"),
                    required_features: wgpu::Features::empty(),
                    required_limits: wgpu::Limits::downlevel_webgl2_defaults(),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("device");

        let caps = surface.get_capabilities(&adapter);
        let format = caps
            .formats
            .iter()
            .find(|f| f.is_srgb())
            .copied()
            .unwrap_or(caps.formats[0]);

        let config = wgpu::SurfaceConfiguration {
            usage: wgpu::TextureUsages::RENDER_ATTACHMENT,
            format,
            width,
            height,
            present_mode: wgpu::PresentMode::AutoVsync,
            alpha_mode: caps.alpha_modes[0],
            view_formats: vec![],
            desired_maximum_frame_latency: 2,
        };
        surface.configure(&device, &config);

        // Font atlas
        let (atlas_tex, atlas) = create_font_atlas(&device, &queue);

        // Uniform buffer
        let uniform_buffer = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("uniforms"),
            contents: bytemuck::bytes_of(&Uniforms {
                screen_size: [width as f32, height as f32],
                _pad: [0.0; 2],
            }),
            usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
        });

        // Bind group layouts
        let uniform_bgl = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("uniform_bgl"),
            entries: &[wgpu::BindGroupLayoutEntry {
                binding: 0,
                visibility: wgpu::ShaderStages::VERTEX,
                ty: wgpu::BindingType::Buffer {
                    ty: wgpu::BufferBindingType::Uniform,
                    has_dynamic_offset: false,
                    min_binding_size: None,
                },
                count: None,
            }],
        });

        let atlas_bgl = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("atlas_bgl"),
            entries: &[
                wgpu::BindGroupLayoutEntry {
                    binding: 0,
                    visibility: wgpu::ShaderStages::FRAGMENT,
                    ty: wgpu::BindingType::Texture {
                        multisampled: false,
                        view_dimension: wgpu::TextureViewDimension::D2,
                        sample_type: wgpu::TextureSampleType::Float { filterable: true },
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 1,
                    visibility: wgpu::ShaderStages::FRAGMENT,
                    ty: wgpu::BindingType::Sampler(wgpu::SamplerBindingType::Filtering),
                    count: None,
                },
            ],
        });

        // Bind groups
        let uniform_bg = device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("uniform_bg"),
            layout: &uniform_bgl,
            entries: &[wgpu::BindGroupEntry {
                binding: 0,
                resource: uniform_buffer.as_entire_binding(),
            }],
        });

        let atlas_view = atlas_tex.create_view(&Default::default());
        let atlas_sampler = device.create_sampler(&wgpu::SamplerDescriptor {
            mag_filter: wgpu::FilterMode::Linear,
            min_filter: wgpu::FilterMode::Linear,
            ..Default::default()
        });
        let atlas_bg = device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("atlas_bg"),
            layout: &atlas_bgl,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: wgpu::BindingResource::TextureView(&atlas_view),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: wgpu::BindingResource::Sampler(&atlas_sampler),
                },
            ],
        });

        // Rect pipeline
        let rect_shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("rect_shader"),
            source: wgpu::ShaderSource::Wgsl(RECT_SHADER.into()),
        });
        let rect_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("rect_layout"),
            bind_group_layouts: &[&uniform_bgl],
            push_constant_ranges: &[],
        });
        let rect_pipeline = device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
            label: Some("rect_pipeline"),
            layout: Some(&rect_layout),
            vertex: wgpu::VertexState {
                module: &rect_shader,
                entry_point: Some("vs"),
                buffers: &[wgpu::VertexBufferLayout {
                    array_stride: std::mem::size_of::<RectInstance>() as u64,
                    step_mode: wgpu::VertexStepMode::Instance,
                    attributes: &[
                        wgpu::VertexAttribute { offset: 0, shader_location: 0, format: wgpu::VertexFormat::Float32x2 },
                        wgpu::VertexAttribute { offset: 8, shader_location: 1, format: wgpu::VertexFormat::Float32x2 },
                        wgpu::VertexAttribute { offset: 16, shader_location: 2, format: wgpu::VertexFormat::Float32x4 },
                        wgpu::VertexAttribute { offset: 32, shader_location: 3, format: wgpu::VertexFormat::Float32 },
                    ],
                }],
                compilation_options: Default::default(),
            },
            fragment: Some(wgpu::FragmentState {
                module: &rect_shader,
                entry_point: Some("fs"),
                targets: &[Some(wgpu::ColorTargetState {
                    format,
                    blend: Some(wgpu::BlendState::ALPHA_BLENDING),
                    write_mask: wgpu::ColorWrites::ALL,
                })],
                compilation_options: Default::default(),
            }),
            primitive: wgpu::PrimitiveState::default(),
            depth_stencil: None,
            multisample: wgpu::MultisampleState::default(),
            multiview: None,
            cache: None,
        });

        // Text pipeline
        let text_shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("text_shader"),
            source: wgpu::ShaderSource::Wgsl(TEXT_SHADER.into()),
        });
        let text_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("text_layout"),
            bind_group_layouts: &[&uniform_bgl, &atlas_bgl],
            push_constant_ranges: &[],
        });
        let text_pipeline = device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
            label: Some("text_pipeline"),
            layout: Some(&text_layout),
            vertex: wgpu::VertexState {
                module: &text_shader,
                entry_point: Some("vs"),
                buffers: &[wgpu::VertexBufferLayout {
                    array_stride: std::mem::size_of::<GlyphInstance>() as u64,
                    step_mode: wgpu::VertexStepMode::Instance,
                    attributes: &[
                        wgpu::VertexAttribute { offset: 0, shader_location: 0, format: wgpu::VertexFormat::Float32x2 },
                        wgpu::VertexAttribute { offset: 8, shader_location: 1, format: wgpu::VertexFormat::Float32x2 },
                        wgpu::VertexAttribute { offset: 16, shader_location: 2, format: wgpu::VertexFormat::Float32x2 },
                        wgpu::VertexAttribute { offset: 24, shader_location: 3, format: wgpu::VertexFormat::Float32x2 },
                        wgpu::VertexAttribute { offset: 32, shader_location: 4, format: wgpu::VertexFormat::Float32x4 },
                    ],
                }],
                compilation_options: Default::default(),
            },
            fragment: Some(wgpu::FragmentState {
                module: &text_shader,
                entry_point: Some("fs"),
                targets: &[Some(wgpu::ColorTargetState {
                    format,
                    blend: Some(wgpu::BlendState::ALPHA_BLENDING),
                    write_mask: wgpu::ColorWrites::ALL,
                })],
                compilation_options: Default::default(),
            }),
            primitive: wgpu::PrimitiveState::default(),
            depth_stencil: None,
            multisample: wgpu::MultisampleState::default(),
            multiview: None,
            cache: None,
        });

        Self {
            surface,
            device,
            queue,
            config,
            rect_pipeline,
            text_pipeline,
            uniform_buffer,
            uniform_bg,
            atlas_bg,
            atlas,
            width,
            height,
        }
    }

    fn resize(&mut self, w: u32, h: u32) {
        if w > 0 && h > 0 {
            self.width = w;
            self.height = h;
            self.config.width = w;
            self.config.height = h;
            self.surface.configure(&self.device, &self.config);
        }
    }

    fn render(&mut self, rects: &[RectInstance], glyphs: &[GlyphInstance]) {
        self.queue.write_buffer(
            &self.uniform_buffer,
            0,
            bytemuck::bytes_of(&Uniforms {
                screen_size: [self.width as f32, self.height as f32],
                _pad: [0.0; 2],
            }),
        );

        let output = match self.surface.get_current_texture() {
            Ok(t) => t,
            Err(_) => {
                self.surface.configure(&self.device, &self.config);
                return;
            }
        };
        let view = output.texture.create_view(&Default::default());
        let mut encoder = self.device.create_command_encoder(&Default::default());

        {
            let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some("main"),
                color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                    view: &view,
                    resolve_target: None,
                    ops: wgpu::Operations {
                        load: wgpu::LoadOp::Clear(wgpu::Color {
                            r: BG[0] as f64,
                            g: BG[1] as f64,
                            b: BG[2] as f64,
                            a: 1.0,
                        }),
                        store: wgpu::StoreOp::Store,
                    },
                })],
                depth_stencil_attachment: None,
                ..Default::default()
            });

            if !rects.is_empty() {
                let buf = self.device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
                    label: Some("rects"),
                    contents: bytemuck::cast_slice(rects),
                    usage: wgpu::BufferUsages::VERTEX,
                });
                pass.set_pipeline(&self.rect_pipeline);
                pass.set_bind_group(0, &self.uniform_bg, &[]);
                pass.set_vertex_buffer(0, buf.slice(..));
                pass.draw(0..6, 0..rects.len() as u32);
            }

            if !glyphs.is_empty() {
                let buf = self.device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
                    label: Some("glyphs"),
                    contents: bytemuck::cast_slice(glyphs),
                    usage: wgpu::BufferUsages::VERTEX,
                });
                pass.set_pipeline(&self.text_pipeline);
                pass.set_bind_group(0, &self.uniform_bg, &[]);
                pass.set_bind_group(1, &self.atlas_bg, &[]);
                pass.set_vertex_buffer(0, buf.slice(..));
                pass.draw(0..6, 0..glyphs.len() as u32);
            }
        }

        self.queue.submit(std::iter::once(encoder.finish()));
        output.present();
    }
}

// ---------------------------------------------------------------------------
// Text helpers
// ---------------------------------------------------------------------------
fn rect(pos: [f32; 2], size: [f32; 2], color: [f32; 4]) -> RectInstance {
    RectInstance { pos, size, color, gradient: 0.0 }
}

fn rect_grad(pos: [f32; 2], size: [f32; 2], color: [f32; 4], gradient: f32) -> RectInstance {
    RectInstance { pos, size, color, gradient }
}

fn push_text(
    glyphs: &mut Vec<GlyphInstance>,
    atlas: &FontAtlas,
    text: &str,
    x: f32,
    y: f32,
    size: f32,
    color: [f32; 4],
) {
    let factor = size / atlas.cell_h;
    let char_w = atlas.cell_w * factor;
    let char_h = atlas.cell_h * factor;
    let mut cx = x;
    for ch in text.chars() {
        if let Some(m) = atlas.glyph_map.get(&ch) {
            glyphs.push(GlyphInstance {
                pos: [cx, y],
                size: [char_w, char_h],
                uv_pos: [m.uv_x, m.uv_y],
                uv_size: [m.uv_w, m.uv_h],
                color,
            });
        }
        cx += char_w;
    }
}

fn push_text_right(
    glyphs: &mut Vec<GlyphInstance>,
    atlas: &FontAtlas,
    text: &str,
    right_x: f32,
    y: f32,
    size: f32,
    color: [f32; 4],
) {
    let factor = size / atlas.cell_h;
    let total_w = text.len() as f32 * atlas.cell_w * factor;
    push_text(glyphs, atlas, text, right_x - total_w, y, size, color);
}

fn text_width(atlas: &FontAtlas, text: &str, size: f32) -> f32 {
    let factor = size / atlas.cell_h;
    text.len() as f32 * atlas.cell_w * factor
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------
fn fmt_price(p: f64) -> String {
    if p >= 1000.0 {
        let s = format!("{:.2}", p);
        // Insert commas
        let parts: Vec<&str> = s.split('.').collect();
        let int_part = parts[0];
        let dec_part = parts[1];
        let mut result = String::new();
        for (i, ch) in int_part.chars().rev().enumerate() {
            if i > 0 && i % 3 == 0 {
                result.push(',');
            }
            result.push(ch);
        }
        let int_formatted: String = result.chars().rev().collect();
        format!("{}.{}", int_formatted, dec_part)
    } else if p >= 1.0 {
        format!("{:.4}", p)
    } else {
        format!("{:.6}", p)
    }
}

fn fmt_qty(q: f64) -> String {
    if q >= 1000.0 {
        format!("{:.2}", q)
    } else if q >= 1.0 {
        format!("{:.4}", q)
    } else {
        format!("{:.6}", q)
    }
}

fn pair_label(pair: &str) -> &str {
    match pair {
        "btcusdt" => "BTC",
        "ethusdt" => "ETH",
        "solusdt" => "SOL",
        "xrpusdt" => "XRP",
        "bnbusdt" => "BNB",
        "dogeusdt" => "DOGE",
        "trumpusdt" => "TRUMP",
        _ => pair,
    }
}

fn exchange_color(i: usize) -> [f32; 4] {
    match i {
        0 => BINANCE_CLR,
        1 => COINBASE_CLR,
        _ => KRAKEN_CLR,
    }
}

// ---------------------------------------------------------------------------
// Line chart (24h history)
// ---------------------------------------------------------------------------
fn draw_line_chart(
    rects: &mut Vec<RectInstance>,
    glyphs: &mut Vec<GlyphInstance>,
    atlas: &FontAtlas,
    data: &VecDeque<Candle>,
    x: f32, y: f32, w: f32, h: f32,
    color: [f32; 4],
) {
    if data.len() < 2 { return; }

    let prices: Vec<f64> = data.iter().map(|c| c.close).collect();
    let min_p = prices.iter().copied().fold(f64::MAX, f64::min);
    let max_p = prices.iter().copied().fold(f64::MIN, f64::max);
    let range = if (max_p - min_p).abs() < 1e-10 { max_p * 0.001 } else { max_p - min_p };

    // Grid lines
    for i in 1..4 {
        let gy = y + (i as f32 / 4.0) * h;
        rects.push(rect([x, gy], [w, 1.0], [BORDER[0], BORDER[1], BORDER[2], 0.3]));
        let price = max_p - (i as f64 / 4.0) * range;
        push_text_right(glyphs, atlas, &fmt_price(price), x + w - 4.0, gy + 2.0, 9.0, TEXT_DIM);
    }

    let n = prices.len();
    let price_to_y = |p: f64| -> f32 {
        y + (1.0 - (p - min_p) / range) as f32 * h
    };

    // Precompute screen positions for each data point
    let step = w / (n - 1) as f32;
    let pts: Vec<(f32, f32)> = prices.iter().enumerate().map(|(i, &p)| {
        (x + i as f32 * step, price_to_y(p))
    }).collect();

    // Area fill: vertical strips from line to bottom
    let bottom = y + h;
    let fill_color = [color[0], color[1], color[2], 0.08];
    for i in 0..n - 1 {
        let x1 = pts[i].0;
        let x2 = pts[i + 1].0;
        let top = pts[i].1.min(pts[i + 1].1);
        let sw = (x2 - x1).max(1.0);
        let sh = bottom - top;
        if sh > 0.0 {
            rects.push(rect([x1, top], [sw, sh], fill_color));
        }
    }

    // Line: interpolate pixel columns between data points
    let line_color = [color[0], color[1], color[2], 0.9];
    for i in 0..n - 1 {
        let (x1, y1) = pts[i];
        let (x2, y2) = pts[i + 1];
        let dx = (x2 - x1).ceil().max(1.0) as i32;
        for j in 0..=dx {
            let t = j as f32 / dx as f32;
            let px = x1 + t * (x2 - x1);
            let py = y1 + t * (y2 - y1);
            rects.push(rect([px, py - 1.0], [1.0, 2.0], line_color));
        }
    }

    // Current price label
    if let Some(&last_p) = prices.last() {
        let ly = price_to_y(last_p);
        push_text_right(glyphs, atlas, &fmt_price(last_p), x + w - 4.0, ly - 12.0, 10.0, color);
    }
}

// ---------------------------------------------------------------------------
// Candlestick chart
// ---------------------------------------------------------------------------
fn draw_candles(
    rects: &mut Vec<RectInstance>,
    glyphs: &mut Vec<GlyphInstance>,
    atlas: &FontAtlas,
    candles: &VecDeque<Candle>,
    x: f32, y: f32, w: f32, h: f32,
) {
    if candles.len() < 2 { return; }

    let min_price = candles.iter().map(|c| c.low).fold(f64::MAX, f64::min);
    let max_price = candles.iter().map(|c| c.high).fold(f64::MIN, f64::max);
    let range = if (max_price - min_price).abs() < 1e-10 { max_price * 0.001 } else { max_price - min_price };

    // Grid lines (4 horizontal)
    for i in 1..4 {
        let gy = y + (i as f32 / 4.0) * h;
        rects.push(rect([x, gy], [w, 1.0], [BORDER[0], BORDER[1], BORDER[2], 0.3]));
        let price = max_price - (i as f64 / 4.0) * range;
        let label = fmt_price(price);
        push_text_right(glyphs, atlas, &label, x + w - 4.0, gy + 2.0, 9.0, TEXT_DIM);
    }

    let n = candles.len();
    let candle_w = w / n as f32;
    let gap = (candle_w * 0.15).max(1.0);
    let body_w = (candle_w - gap * 2.0).max(2.0);
    let wick_w = (candle_w * 0.1).max(1.0);

    for (i, candle) in candles.iter().enumerate() {
        let cx = x + i as f32 * candle_w + candle_w / 2.0;

        let is_green = candle.close >= candle.open;
        let body_color = if is_green {
            [GREEN[0], GREEN[1], GREEN[2], 0.9]
        } else {
            [RED[0], RED[1], RED[2], 0.9]
        };
        let wick_color = if is_green {
            [GREEN[0], GREEN[1], GREEN[2], 0.5]
        } else {
            [RED[0], RED[1], RED[2], 0.5]
        };

        let price_to_y = |p: f64| -> f32 {
            y + (1.0 - (p - min_price) / range) as f32 * h
        };

        let high_y = price_to_y(candle.high);
        let low_y = price_to_y(candle.low);
        let open_y = price_to_y(candle.open);
        let close_y = price_to_y(candle.close);

        // Wick
        rects.push(rect(
            [cx - wick_w / 2.0, high_y],
            [wick_w, (low_y - high_y).max(1.0)],
            wick_color,
        ));

        // Body
        let body_top = if is_green { close_y } else { open_y };
        let body_bot = if is_green { open_y } else { close_y };
        let body_h = (body_bot - body_top).max(1.0);
        rects.push(rect(
            [cx - body_w / 2.0, body_top],
            [body_w, body_h],
            body_color,
        ));
    }

    // Current price line
    if let Some(last) = candles.back() {
        let ly = y + (1.0 - (last.close - min_price) / range) as f32 * h;
        let line_color = if last.close >= last.open { GREEN } else { RED };
        // Dashed line effect
        let dash_w = 6.0;
        let mut dx = x;
        while dx < x + w {
            rects.push(rect(
                [dx, ly],
                [dash_w, 1.0],
                [line_color[0], line_color[1], line_color[2], 0.4],
            ));
            dx += dash_w * 2.0;
        }
    }
}

// ---------------------------------------------------------------------------
// Frame building — layout + GPU data generation
// ---------------------------------------------------------------------------
fn build_frame(
    state: &AppState,
    atlas: &FontAtlas,
    w: f32,
    h: f32,
) -> (Vec<RectInstance>, Vec<GlyphInstance>) {
    let mut rects = Vec::with_capacity(256);
    let mut glyphs = Vec::with_capacity(4096);

    // Outer margin
    let margin = 12.0_f32;
    let iw = w - margin * 2.0;

    // Header
    rects.push(rect([margin, margin], [iw, HEADER_H], SURFACE));
    rects.push(rect([margin, margin + HEADER_H - 1.0], [iw, 1.0], BORDER));
    push_text(&mut glyphs, atlas, "Crypto", margin + 24.0, margin + 18.0, 18.0, TEXT);
    push_text(&mut glyphs, atlas, "Order Book", margin + 24.0 + text_width(atlas, "Crypto ", 18.0), margin + 18.0, 18.0, ACCENT);

    // Connection status
    let status = if state.connected { "connected" } else { "connecting..." };
    let status_color = if state.connected { GREEN } else { TEXT_DIM };
    push_text_right(&mut glyphs, atlas, status, margin + iw - 24.0, margin + 20.0, 14.0, status_color);
    // Status dot
    if state.connected {
        let dot_x = margin + iw - 24.0 - text_width(atlas, status, 14.0) - 16.0;
        rects.push(rect([dot_x, margin + 22.0], [10.0, 10.0], GREEN));
    }

    // Pair tabs
    let tabs_y = margin + HEADER_H;
    rects.push(rect([margin, tabs_y], [iw, TABS_H], SURFACE));
    rects.push(rect([margin, tabs_y + TABS_H - 1.0], [iw, 1.0], BORDER));

    let tab_w = 100.0;
    for (i, pair) in PAIRS.iter().enumerate() {
        let x = margin + 24.0 + i as f32 * (tab_w + 4.0);
        let y = tabs_y + 6.0;
        let active = *pair == state.active_pair;

        if active {
            rects.push(rect([x, y], [tab_w, 42.0], ACCENT_BG));
        }

        let label = pair_label(pair);
        let lbl_color = if active { ACCENT } else { TEXT_DIM };
        push_text(&mut glyphs, atlas, label, x + 10.0, y + 4.0, 16.0, lbl_color);

        if let Some(price) = state.best_price(pair) {
            let ps = format!("${:.2}", price);
            push_text(&mut glyphs, atlas, &ps, x + 10.0, y + 24.0, 12.0, TEXT_DIM);
        }
    }

    // Exchange columns
    let pad = 8.0_f32;
    let content_y = tabs_y + TABS_H;
    let content_h = h - content_y - STATS_H - margin;
    let col_w = iw / 3.0;

    for (ei, exchange) in EXCHANGES.iter().enumerate() {
        let x = margin + ei as f32 * col_w;
        let y = content_y;

        // Panel background
        rects.push(rect([x, y], [col_w, content_h], SURFACE));
        if ei > 0 {
            rects.push(rect([x, y], [1.0, content_h], BORDER));
        }

        // Exchange header
        rects.push(rect([x, y + EXCH_HDR_H - 1.0], [col_w, 1.0], BORDER));
        push_text(
            &mut glyphs, atlas,
            &exchange.to_uppercase(),
            x + pad * 2.0, y + 12.0, 15.0,
            exchange_color(ei),
        );

        // Spread
        if let Some(book) = state.get_book(exchange) {
            if let (Some(bid), Some(ask)) = (book.bids.first(), book.asks.first()) {
                let sp = ask.price - bid.price;
                let pct = sp / ask.price * 100.0;
                let ss = format!("spread: {:.5} ({:.3}%)", sp, pct);
                push_text_right(&mut glyphs, atlas, &ss, x + col_w - pad, y + 14.0, 12.0, TEXT_DIM);
            }
        }

        // Column headers
        let bh_y = y + EXCH_HDR_H;
        push_text(&mut glyphs, atlas, "PRICE", x + pad, bh_y + 6.0, 12.0, TEXT_DIM);
        let half = col_w / 2.0;
        push_text_right(&mut glyphs, atlas, "QTY", x + half - pad, bh_y + 6.0, 12.0, TEXT_DIM);
        push_text(&mut glyphs, atlas, "QTY", x + half + pad, bh_y + 6.0, 12.0, TEXT_DIM);
        push_text_right(&mut glyphs, atlas, "PRICE", x + col_w - pad, bh_y + 6.0, 12.0, TEXT_DIM);
        rects.push(rect([x, bh_y + BOOK_HDR_H - 1.0], [col_w, 1.0], BORDER));

        let rows_y = bh_y + BOOK_HDR_H;

        if let Some(book) = state.get_book(exchange) {
            let anim_key = format!("{}/{}", exchange, state.active_pair);
            let anim = state.bar_anim.get(&anim_key);
            let max_bid = book.bids.iter().take(DEPTH).map(|l| l.qty).fold(0.001_f64, f64::max);
            let max_ask = book.asks.iter().take(DEPTH).map(|l| l.qty).fold(0.001_f64, f64::max);

            // Bids (gradient: transparent left → solid green right)
            for (j, level) in book.bids.iter().take(DEPTH).enumerate() {
                let ry = rows_y + j as f32 * ROW_H;
                let anim_qty = anim.map(|a| a.0[j]).unwrap_or(level.qty);
                let bar_pct = (anim_qty / max_bid) as f32;
                let bar_w = (half * bar_pct).max(2.0);
                rects.push(rect_grad(
                    [x + half - bar_w, ry], [bar_w, ROW_H],
                    [GREEN[0], GREEN[1], GREEN[2], 0.25],
                    1.0, // fade left→right
                ));

                if state.mouse_y >= ry && state.mouse_y < ry + ROW_H
                    && state.mouse_x >= x && state.mouse_x < x + half
                {
                    rects.push(rect([x, ry], [half, ROW_H], SURFACE2));
                }

                push_text(&mut glyphs, atlas, &fmt_price(level.price), x + pad, ry + 5.0, 13.0, GREEN);
                push_text_right(&mut glyphs, atlas, &fmt_qty(level.qty), x + half - pad, ry + 5.0, 13.0, TEXT_DIM);
            }

            // Asks (gradient: solid red left → transparent right)
            for (j, level) in book.asks.iter().take(DEPTH).enumerate() {
                let ry = rows_y + j as f32 * ROW_H;
                let ask_x = x + half;
                let anim_qty = anim.map(|a| a.1[j]).unwrap_or(level.qty);
                let bar_pct = (anim_qty / max_ask) as f32;
                let bar_w = (half * bar_pct).max(2.0);
                rects.push(rect_grad(
                    [ask_x, ry], [bar_w, ROW_H],
                    [RED[0], RED[1], RED[2], 0.25],
                    -1.0, // fade right→left
                ));

                if state.mouse_y >= ry && state.mouse_y < ry + ROW_H
                    && state.mouse_x >= ask_x && state.mouse_x < ask_x + half
                {
                    rects.push(rect([ask_x, ry], [half, ROW_H], SURFACE2));
                }

                push_text(&mut glyphs, atlas, &fmt_qty(level.qty), ask_x + pad, ry + 5.0, 13.0, TEXT_DIM);
                push_text_right(&mut glyphs, atlas, &fmt_price(level.price), ask_x + half - pad, ry + 5.0, 13.0, RED);
            }
        } else {
            push_text(
                &mut glyphs, atlas,
                "Waiting for data...",
                x + half - text_width(atlas, "Waiting for data...", 14.0) / 2.0,
                rows_y + 80.0, 14.0, TEXT_DIM,
            );
        }

        // --- Candlestick chart ---
        let gx = x + pad;
        let gw = col_w - pad * 2.0;
        let chart_y = rows_y + DEPTH as f32 * ROW_H + pad;

        push_text(&mut glyphs, atlas, "PRICE", gx + pad, chart_y + 3.0, 12.0, TEXT_DIM);
        if let Some(candles) = state.get_candles(exchange) {
            if let Some(last) = candles.back() {
                let ps = fmt_price(last.close);
                let clr = if last.close >= last.open { GREEN } else { RED };
                push_text_right(&mut glyphs, atlas, &ps, gx + gw - pad, chart_y + 3.0, 12.0, clr);
            }
        }
        let chart_area_y = chart_y + CHART_LABEL_H;
        rects.push(rect([gx, chart_area_y], [gw, CHART_H], CHART_BG));
        if let Some(candles) = state.get_candles(exchange) {
            draw_candles(&mut rects, &mut glyphs, atlas, candles, gx, chart_area_y, gw, CHART_H);
        }

        // --- 24h line chart ---
        let chart24_y = chart_area_y + CHART_H + pad;
        push_text(&mut glyphs, atlas, "24H", gx + pad, chart24_y + 3.0, 12.0, TEXT_DIM);
        if let Some(candles24) = state.get_candles_24h(exchange) {
            if let Some(last) = candles24.back() {
                let ps = fmt_price(last.close);
                let first_close = candles24.front().map(|c| c.close).unwrap_or(last.close);
                let clr = if last.close >= first_close { GREEN } else { RED };
                push_text_right(&mut glyphs, atlas, &ps, gx + gw - pad, chart24_y + 3.0, 12.0, clr);
            }
        }
        let chart24_area_y = chart24_y + CHART_LABEL_H;
        rects.push(rect([gx, chart24_area_y], [gw, CHART_24H_H], CHART_BG));
        if let Some(candles24) = state.get_candles_24h(exchange) {
            let first_close = candles24.front().map(|c| c.close).unwrap_or(0.0);
            let last_close = candles24.back().map(|c| c.close).unwrap_or(0.0);
            let line_clr = if last_close >= first_close { GREEN } else { RED };
            draw_line_chart(&mut rects, &mut glyphs, atlas, candles24, gx, chart24_area_y, gw, CHART_24H_H, line_clr);
        }
    }

    // Loading overlay
    if state.loading {
        // Semi-transparent overlay
        rects.push(rect([0.0, 0.0], [w, h], [0.0, 0.0, 0.0, 0.5]));

        // Spinner: rotating blocks around center
        let cx = w / 2.0;
        let cy = h / 2.0;
        let now = js_sys::Date::now() / 1000.0;
        let num_blocks = 8;
        let radius = 30.0_f32;
        let block_size = 8.0_f32;
        let angle_offset = (now * 4.0) % (2.0 * std::f64::consts::PI);

        for i in 0..num_blocks {
            let angle = (i as f64 / num_blocks as f64) * 2.0 * std::f64::consts::PI + angle_offset;
            let bx = cx + (angle.cos() as f32) * radius - block_size / 2.0;
            let by = cy + (angle.sin() as f32) * radius - block_size / 2.0;
            let alpha = (i as f32 + 1.0) / num_blocks as f32;
            rects.push(rect(
                [bx, by],
                [block_size, block_size],
                [ACCENT[0], ACCENT[1], ACCENT[2], alpha],
            ));
        }

        // "Loading..." text
        let label = "Loading historical data...";
        let tw = text_width(atlas, label, 16.0);
        push_text(&mut glyphs, atlas, label, cx - tw / 2.0, cy + 50.0, 16.0, TEXT);
    }

    // Stats bar
    let sy = h - STATS_H - margin;
    rects.push(rect([margin, sy], [iw, STATS_H], SURFACE));
    rects.push(rect([margin, sy], [iw, 1.0], BORDER));

    let elapsed = (js_sys::Date::now() / 1000.0 - state.start_time) as u32;
    let m = elapsed / 60;
    let s = elapsed % 60;
    let up = if m > 0 { format!("{}m {}s", m, s) } else { format!("{}s", s) };
    let stats = format!("msgs: {}   uptime: {}   renderer: wgpu", state.msg_count, up);
    push_text(&mut glyphs, atlas, &stats, margin + 24.0, sy + 10.0, 13.0, TEXT_DIM);

    (rects, glyphs)
}

// ---------------------------------------------------------------------------
// WebSocket
// ---------------------------------------------------------------------------
fn connect_ws(state: Rc<RefCell<AppState>>) {
    let window = web_sys::window().unwrap();
    let document = window.document().unwrap();

    // Check for data-ws attribute on canvas
    let ws_url = document
        .get_element_by_id("canvas")
        .and_then(|el| el.get_attribute("data-ws"))
        .unwrap_or_else(|| {
            let loc = window.location();
            let proto = if loc.protocol().unwrap() == "https:" { "wss" } else { "ws" };
            format!("{}://{}/ws", proto, loc.host().unwrap())
        });

    let ws = match web_sys::WebSocket::new(&ws_url) {
        Ok(w) => w,
        Err(e) => {
            log::error!("WebSocket create failed: {:?}", e);
            return;
        }
    };

    let s = state.clone();
    let onopen = Closure::wrap(Box::new(move |_: web_sys::Event| {
        s.borrow_mut().connected = true;
    }) as Box<dyn FnMut(_)>);
    ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));
    onopen.forget();

    let s = state.clone();
    let onmessage = Closure::wrap(Box::new(move |e: web_sys::MessageEvent| {
        if let Ok(txt) = e.data().dyn_into::<js_sys::JsString>() {
            let data: String = txt.into();
            s.borrow_mut().handle_message(&data);
        }
    }) as Box<dyn FnMut(_)>);
    ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
    onmessage.forget();

    let s = state.clone();
    let onclose = Closure::wrap(Box::new(move |_: web_sys::CloseEvent| {
        s.borrow_mut().connected = false;
        let s2 = s.clone();
        let retry = Closure::once(move || connect_ws(s2));
        web_sys::window()
            .unwrap()
            .set_timeout_with_callback_and_timeout_and_arguments_0(
                retry.as_ref().unchecked_ref(),
                2000,
            )
            .ok();
        retry.forget();
    }) as Box<dyn FnMut(_)>);
    ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));
    onclose.forget();
}

// ---------------------------------------------------------------------------
// Historical data loader
// ---------------------------------------------------------------------------
#[derive(Deserialize)]
struct HistoryDoc {
    bid_top: Option<f64>,
    ask_top: Option<f64>,
}

#[derive(Deserialize)]
struct FindResponse {
    ok: Option<bool>,
    data: Option<Vec<HistoryDoc>>,
}

fn get_api_base() -> String {
    let window = web_sys::window().unwrap();
    let document = window.document().unwrap();
    // Derive API base from data-api attribute, or from data-ws (same host as WebSocket)
    if let Some(api) = document
        .get_element_by_id("canvas")
        .and_then(|el| el.get_attribute("data-api"))
    {
        return api;
    }
    if let Some(ws) = document
        .get_element_by_id("canvas")
        .and_then(|el| el.get_attribute("data-ws"))
    {
        // ws://host:port/ws → http://host:port
        let base = ws.replace("wss://", "https://").replace("ws://", "http://");
        return base.trim_end_matches("/ws").to_string();
    }
    let loc = window.location();
    let proto = loc.protocol().unwrap();
    format!("{}//{}", proto, loc.host().unwrap())
}

async fn fetch_history(exchange: &str, pair: &str) -> Vec<f64> {
    let base = get_api_base();
    let url = format!("{}/api/history?exchange={}&pair={}", base, exchange, pair);

    let resp = match wasm_bindgen_futures::JsFuture::from(
        web_sys::window().unwrap().fetch_with_str(&url),
    )
    .await
    {
        Ok(r) => r,
        Err(_) => return vec![],
    };

    let resp: web_sys::Response = match resp.dyn_into() {
        Ok(r) => r,
        Err(_) => return vec![],
    };

    let json = match wasm_bindgen_futures::JsFuture::from(resp.json().unwrap_or_else(|_| js_sys::Promise::resolve(&JsValue::NULL))).await {
        Ok(j) => j,
        Err(_) => return vec![],
    };

    let text = js_sys::JSON::stringify(&json)
        .map(|s| String::from(s))
        .unwrap_or_default();

    let mut mids = Vec::new();
    if let Ok(resp) = serde_json::from_str::<FindResponse>(&text) {
        if let Some(docs) = resp.data {
            for doc in docs.iter() {
                if let (Some(bid), Some(ask)) = (doc.bid_top, doc.ask_top) {
                    if bid > 0.0 && ask > 0.0 {
                        mids.push((bid + ask) / 2.0);
                    }
                }
            }
        }
    }
    mids
}

fn load_history_for_pair(state: Rc<RefCell<AppState>>, pair: String) {
    if state.borrow().loaded_pairs.contains(&pair) {
        return;
    }

    wasm_bindgen_futures::spawn_local(async move {
        state.borrow_mut().loading = true;
        state.borrow_mut().loaded_pairs.insert(pair.clone());
        for exchange in EXCHANGES {
            if !exchange_has_pair(exchange, &pair) { continue; }
            let key = format!("{}/{}", exchange, pair);
            if state.borrow().candles.contains_key(&key) {
                continue;
            }
            let mids = fetch_history(exchange, &pair).await;
            if !mids.is_empty() {
                log::info!("loaded {} historical points for {}/{}", mids.len(), exchange, pair);
                seed_candles(&mut state.borrow_mut(), exchange, &pair, mids);
            }
        }
        state.borrow_mut().loading = false;
    });
}

fn seed_candles(state: &mut AppState, exchange: &str, pair: &str, mids: Vec<f64>) {
    if mids.is_empty() {
        return;
    }
    let key = format!("{}/{}", exchange, pair);
    state.candle_buf.remove(&key); // reset partial buffer
    let candles = state.candles.entry(key).or_default();
    candles.clear(); // replace any real-time candles with full history
    for chunk in mids.chunks(CANDLE_TICKS) {
        if chunk.len() < 2 {
            continue;
        }
        candles.push_back(Candle {
            open: chunk[0],
            close: *chunk.last().unwrap(),
            high: chunk.iter().copied().fold(f64::MIN, f64::max),
            low: chunk.iter().copied().fold(f64::MAX, f64::min),
        });
        if candles.len() > MAX_CANDLES {
            candles.pop_front();
        }
    }
}

// ---------------------------------------------------------------------------
// 24h OHLC data loader
// ---------------------------------------------------------------------------
#[derive(Deserialize)]
struct OhlcDoc {
    bid_open: Option<f64>,
    bid_close: Option<f64>,
    bid_high: Option<f64>,
    bid_low: Option<f64>,
    ask_open: Option<f64>,
    ask_close: Option<f64>,
    ask_high: Option<f64>,
    ask_low: Option<f64>,
}

#[derive(Deserialize)]
struct OhlcResponse {
    data: Option<Vec<OhlcDoc>>,
}

async fn fetch_ohlc(exchange: &str, pair: &str) -> Vec<Candle> {
    let base = get_api_base();
    let url = format!("{}/api/ohlc?exchange={}&pair={}", base, exchange, pair);

    let resp = match wasm_bindgen_futures::JsFuture::from(
        web_sys::window().unwrap().fetch_with_str(&url),
    )
    .await
    {
        Ok(r) => r,
        Err(_) => return vec![],
    };

    let resp: web_sys::Response = match resp.dyn_into() {
        Ok(r) => r,
        Err(_) => return vec![],
    };

    let json = match wasm_bindgen_futures::JsFuture::from(
        resp.json().unwrap_or_else(|_| js_sys::Promise::resolve(&JsValue::NULL)),
    )
    .await
    {
        Ok(j) => j,
        Err(_) => return vec![],
    };

    let text = js_sys::JSON::stringify(&json)
        .map(|s| String::from(s))
        .unwrap_or_default();

    let mut candles = Vec::new();
    if let Ok(resp) = serde_json::from_str::<OhlcResponse>(&text) {
        if let Some(docs) = resp.data {
            // docs come newest-first, reverse for chronological order
            for doc in docs.iter().rev() {
                let bo = doc.bid_open.unwrap_or(0.0);
                let ao = doc.ask_open.unwrap_or(0.0);
                let bc = doc.bid_close.unwrap_or(0.0);
                let ac = doc.ask_close.unwrap_or(0.0);
                let bh = doc.bid_high.unwrap_or(0.0);
                let ah = doc.ask_high.unwrap_or(0.0);
                let bl = doc.bid_low.unwrap_or(0.0);
                let al = doc.ask_low.unwrap_or(0.0);
                if bo > 0.0 && ao > 0.0 {
                    candles.push(Candle {
                        open: (bo + ao) / 2.0,
                        close: (bc + ac) / 2.0,
                        high: (bh + ah) / 2.0,
                        low: (bl + al) / 2.0,
                    });
                }
            }
        }
    }
    candles
}

fn load_24h_for_pair(state: Rc<RefCell<AppState>>, pair: String) {
    let key_24h = format!("24h/{}", pair);
    if state.borrow().loaded_pairs.contains(&key_24h) {
        return;
    }

    wasm_bindgen_futures::spawn_local(async move {
        state.borrow_mut().loaded_pairs.insert(key_24h);
        for exchange in EXCHANGES {
            if !exchange_has_pair(exchange, &pair) { continue; }
            let key = format!("{}/{}", exchange, pair);
            if state.borrow().history_24h.contains_key(&key) {
                continue;
            }
            let candles = fetch_ohlc(exchange, &pair).await;
            if !candles.is_empty() {
                log::info!("loaded {} 24h candles for {}/{}", candles.len(), exchange, pair);
                let mut st = state.borrow_mut();
                let deque: VecDeque<Candle> = candles.into_iter().collect();
                st.history_24h.insert(key, deque);
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
#[wasm_bindgen(start)]
pub async fn start() -> Result<(), JsValue> {
    console_error_panic_hook::set_once();
    console_log::init_with_level(log::Level::Info).ok();

    let window = web_sys::window().unwrap();
    let document = window.document().unwrap();
    let canvas: web_sys::HtmlCanvasElement = document
        .get_element_by_id("canvas")
        .expect("no #canvas")
        .dyn_into()?;

    let dpr = window.device_pixel_ratio();
    let cw = window.inner_width()?.as_f64().unwrap();
    let ch = window.inner_height()?.as_f64().unwrap();
    let pw = (cw * dpr) as u32;
    let ph = (ch * dpr) as u32;
    canvas.set_width(pw);
    canvas.set_height(ph);
    let html_el: &web_sys::HtmlElement = canvas.unchecked_ref();
    html_el.style().set_property("width", &format!("{}px", cw))?;
    html_el.style().set_property("height", &format!("{}px", ch))?;

    let state = Rc::new(RefCell::new(AppState::new()));
    let renderer = Rc::new(RefCell::new(Renderer::new(canvas.clone(), pw, ph).await));

    connect_ws(state.clone());

    // Load historical candle data from OxiDB (async, with loading indicator)
    {
        let s = state.clone();
        wasm_bindgen_futures::spawn_local(async move {
            s.borrow_mut().loading = true;
            for exchange in EXCHANGES {
                for pair in PAIRS {
                    if !exchange_has_pair(exchange, pair) { continue; }
                    let mids = fetch_history(exchange, pair).await;
                    if !mids.is_empty() {
                        log::info!("loaded {} historical points for {}/{}", mids.len(), exchange, pair);
                        seed_candles(&mut s.borrow_mut(), exchange, pair, mids);
                    }
                    // Also load 24h OHLC
                    let candles = fetch_ohlc(exchange, pair).await;
                    if !candles.is_empty() {
                        log::info!("loaded {} 24h candles for {}/{}", candles.len(), exchange, pair);
                        let key = format!("{}/{}", exchange, pair);
                        let deque: VecDeque<Candle> = candles.into_iter().collect();
                        s.borrow_mut().history_24h.insert(key, deque);
                    }
                }
            }
            s.borrow_mut().loading = false;
        });
    }

    // Refresh 24h chart every 60 seconds
    {
        let s = state.clone();
        let cb = Closure::wrap(Box::new(move || {
            let s2 = s.clone();
            wasm_bindgen_futures::spawn_local(async move {
                let pair = s2.borrow().active_pair.clone();
                for exchange in EXCHANGES {
                    if !exchange_has_pair(exchange, &pair) { continue; }
                    let candles = fetch_ohlc(exchange, &pair).await;
                    if !candles.is_empty() {
                        let key = format!("{}/{}", exchange, pair);
                        let deque: VecDeque<Candle> = candles.into_iter().collect();
                        s2.borrow_mut().history_24h.insert(key, deque);
                    }
                }
            });
        }) as Box<dyn FnMut()>);
        web_sys::window()
            .unwrap()
            .set_interval_with_callback_and_timeout_and_arguments_0(cb.as_ref().unchecked_ref(), 60_000)?;
        cb.forget();
    }

    // Mouse events (in CSS pixels → convert to physical)
    {
        let s = state.clone();
        let d = dpr as f32;
        let cb = Closure::wrap(Box::new(move |e: web_sys::MouseEvent| {
            let mut st = s.borrow_mut();
            st.mouse_x = e.client_x() as f32 * d;
            st.mouse_y = e.client_y() as f32 * d;
        }) as Box<dyn FnMut(_)>);
        canvas
            .add_event_listener_with_callback("mousemove", cb.as_ref().unchecked_ref())?;
        cb.forget();
    }

    // Click → tab selection + load history if needed
    {
        let s = state.clone();
        let d = dpr as f32;
        let cb = Closure::wrap(Box::new(move |e: web_sys::MouseEvent| {
            let mx = e.client_x() as f32 * d;
            let my = e.client_y() as f32 * d;
            let margin = 12.0_f32;
            let tabs_y = margin + HEADER_H;
            let tabs_h = TABS_H;
            if my >= tabs_y && my < tabs_y + tabs_h {
                let tab_w = 100.0_f32;
                for (i, pair) in PAIRS.iter().enumerate() {
                    let tx = margin + 24.0 + i as f32 * (tab_w + 4.0);
                    if mx >= tx && mx < tx + tab_w {
                        let pair_str = pair.to_string();
                        s.borrow_mut().active_pair = pair_str.clone();
                        // Load history if candles are empty for this pair
                        load_history_for_pair(s.clone(), pair_str.clone());
                        load_24h_for_pair(s.clone(), pair_str);
                        break;
                    }
                }
            }
        }) as Box<dyn FnMut(_)>);
        canvas.add_event_listener_with_callback("click", cb.as_ref().unchecked_ref())?;
        cb.forget();
    }

    // Resize
    {
        let r = renderer.clone();
        let c = canvas.clone();
        let cb = Closure::wrap(Box::new(move |_: web_sys::Event| {
            let win = web_sys::window().unwrap();
            let d = win.device_pixel_ratio();
            let cw = win.inner_width().unwrap().as_f64().unwrap();
            let ch = win.inner_height().unwrap().as_f64().unwrap();
            let pw = (cw * d) as u32;
            let ph = (ch * d) as u32;
            c.set_width(pw);
            c.set_height(ph);
            let el: &web_sys::HtmlElement = c.unchecked_ref();
            let _ = el.style().set_property("width", &format!("{}px", cw));
            let _ = el.style().set_property("height", &format!("{}px", ch));
            r.borrow_mut().resize(pw, ph);
        }) as Box<dyn FnMut(_)>);
        window.add_event_listener_with_callback("resize", cb.as_ref().unchecked_ref())?;
        cb.forget();
    }

    // Render loop via requestAnimationFrame
    let f: Rc<RefCell<Option<Closure<dyn FnMut()>>>> = Rc::new(RefCell::new(None));
    let g = f.clone();
    let r = renderer.clone();
    let s = state.clone();

    *g.borrow_mut() = Some(Closure::new(move || {
        let r = r.clone();
        let s = s.clone();
        let f = f.clone();

        let mut renderer = r.borrow_mut();
        let mut st = s.borrow_mut();
        st.update_animations();
        let (rects, glyphs) = build_frame(&*st, &renderer.atlas, renderer.width as f32, renderer.height as f32);
        renderer.render(&rects, &glyphs);
        drop(st);
        drop(renderer);

        web_sys::window()
            .unwrap()
            .request_animation_frame(
                f.borrow().as_ref().unwrap().as_ref().unchecked_ref(),
            )
            .ok();
    }));

    web_sys::window()
        .unwrap()
        .request_animation_frame(g.borrow().as_ref().unwrap().as_ref().unchecked_ref())?;

    Ok(())
}
