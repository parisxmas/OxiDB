use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use clap::Parser;
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Cell, List, ListItem, Paragraph, Row, Scrollbar, ScrollbarOrientation,
    ScrollbarState, Table,
};
use serde_json::{Value, json};

#[derive(Parser)]
#[command(
    name = "oxidb-tail",
    about = "Interactive TUI log viewer for OxiDB",
    long_about = "Real-time interactive log viewer for OxiDB with colored output.\n\n\
        Connects to an OxiDB server via TCP and streams log entries from a\n\
        GELF collection in a columnar table layout with automatic field detection.\n\n\
        Features:\n\
        - Color-coded log levels (ERR=red, WRN=yellow, INF=green, DBG=gray)\n\
        - Automatic column detection from structured GELF fields\n\
        - Full-text search across all fields with / key\n\
        - Level filtering (cycle with l key)\n\
        - Scrollable log history with keyboard navigation\n\
        - Detail panel for inspecting individual log entries (Enter key)\n\
        - Stats sidebar with host distribution (Tab key)\n\
        - Live/paused mode toggle\n\n\
        Example:\n\
          oxidb-tail --host 127.0.0.1 --port 4444\n\
          oxidb-tail --host db.example.com --port 4444 --collection app_logs\n\
          oxidb-tail --host 192.0.2.6 --port 4444 --interval 1000\n\n\
        Keyboard shortcuts:\n\
          q         Quit\n\
          /         Search/filter logs\n\
          l         Cycle level filter (ALL → ERR → WRN → INF → DBG)\n\
          ↑↓ / jk   Scroll through logs (pauses live mode)\n\
          PgUp/PgDn Fast scroll\n\
          Enter     Toggle detail panel for selected log\n\
          Tab       Toggle stats sidebar\n\
          f         Resume live mode (follow new logs)\n\
          Esc       Clear filter or resume live mode"
)]
struct Cli {
    /// OxiDB server hostname or IP address
    #[arg(
        long,
        help = "OxiDB server hostname or IP address (e.g. 127.0.0.1, db.example.com)"
    )]
    host: String,

    /// OxiDB server TCP port
    #[arg(long, help = "OxiDB server TCP port number (e.g. 4444)")]
    port: u16,

    /// Name of the collection to stream logs from
    #[arg(
        long,
        short,
        default_value = "_gelf_logs",
        help = "Collection to tail [default: _gelf_logs]"
    )]
    collection: String,

    /// How often to poll the server for new logs, in milliseconds
    #[arg(
        long,
        default_value_t = 500,
        help = "Poll interval in milliseconds [default: 500]"
    )]
    interval: u64,
}

// ─── Wire protocol ──────────────────────────────────────────

fn send_recv(stream: &mut TcpStream, payload: &Value) -> std::io::Result<Value> {
    let bytes = payload.to_string().into_bytes();
    let len = (bytes.len() as u32).to_le_bytes();
    stream.write_all(&len)?;
    stream.write_all(&bytes)?;
    stream.flush()?;
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let resp_len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; resp_len];
    stream.read_exact(&mut buf)?;
    serde_json::from_slice(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

// ─── Log entry ──────────────────────────────────────────────

#[derive(Clone)]
struct LogEntry {
    id: u64,
    timestamp: String,
    level: u8,
    host: String,
    message: String,
    facility: String,
    extra: Vec<(String, String)>,
}

impl LogEntry {
    fn from_doc(doc: &Value) -> Option<Self> {
        let id = doc.get("_id")?.as_u64()?;
        let level = doc.get("level").and_then(|v| v.as_u64()).unwrap_or(6) as u8;
        let host = doc
            .get("host")
            .and_then(|v| v.as_str())
            .unwrap_or("-")
            .to_string();
        let message = doc
            .get("short_message")
            .and_then(|v| v.as_str())
            .unwrap_or("-")
            .to_string();
        let facility = doc
            .get("facility")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let timestamp = if let Some(ts) = doc.get("_ts").and_then(|v| v.as_str()) {
            if let Ok(dt) = ts.parse::<DateTime<Utc>>() {
                dt.format("%H:%M:%S%.3f").to_string()
            } else {
                "??:??:??".to_string()
            }
        } else {
            "??:??:??".to_string()
        };

        let skip = [
            "_id",
            "_ts",
            "_version",
            "timestamp",
            "host",
            "short_message",
            "full_message",
            "level",
            "facility",
        ];
        let mut extra = Vec::new();
        if let Some(obj) = doc.as_object() {
            for (k, v) in obj {
                if skip.contains(&k.as_str()) {
                    continue;
                }
                let val = match v {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    _ => continue,
                };
                extra.push((k.clone(), val));
            }
        }

        Some(Self {
            id,
            timestamp,
            level,
            host,
            message,
            facility,
            extra,
        })
    }

    fn level_label(&self) -> &'static str {
        match self.level {
            0 => "EMR",
            1 => "ALR",
            2 => "CRT",
            3 => "ERR",
            4 => "WRN",
            5 => "NTC",
            6 => "INF",
            7 => "DBG",
            _ => "???",
        }
    }

    fn level_color(&self) -> Color {
        match self.level {
            0 | 1 | 2 | 3 => Color::Red,
            4 => Color::Yellow,
            5 => Color::Cyan,
            6 => Color::Green,
            7 => Color::DarkGray,
            _ => Color::White,
        }
    }
}

// ─── App state ──────────────────────────────────────────────

struct App {
    logs: Vec<LogEntry>,
    scroll_offset: usize,
    auto_scroll: bool,
    filter_text: String,
    filter_active: bool,
    level_filter: Option<u8>,
    extra_columns: Vec<String>,
    columns_locked: bool,
    total_count: u64,
    error_count: u64,
    warn_count: u64,
    msg_per_sec: f64,
    host_counts: HashMap<String, u64>,
    last_id: u64,
    last_fetch: Instant,
    last_count: usize,
    show_detail: bool,
    show_stats: bool,
    selected: usize,
}

impl App {
    fn new() -> Self {
        Self {
            logs: Vec::new(),
            scroll_offset: 0,
            auto_scroll: true,
            filter_text: String::new(),
            filter_active: false,
            level_filter: None,
            extra_columns: Vec::new(),
            columns_locked: false,
            total_count: 0,
            error_count: 0,
            warn_count: 0,
            msg_per_sec: 0.0,
            host_counts: HashMap::new(),
            last_id: 0,
            last_fetch: Instant::now(),
            last_count: 0,
            show_detail: false,
            show_stats: false,
            selected: 0,
        }
    }

    fn filtered_logs(&self) -> Vec<&LogEntry> {
        self.logs
            .iter()
            .filter(|log| {
                if let Some(max_level) = self.level_filter {
                    if log.level > max_level {
                        return false;
                    }
                }
                if !self.filter_text.is_empty() {
                    let ft = self.filter_text.to_lowercase();
                    let matches = log.message.to_lowercase().contains(&ft)
                        || log.host.to_lowercase().contains(&ft)
                        || log.facility.to_lowercase().contains(&ft)
                        || log.extra.iter().any(|(k, v)| {
                            k.to_lowercase().contains(&ft) || v.to_lowercase().contains(&ft)
                        });
                    if !matches {
                        return false;
                    }
                }
                true
            })
            .collect()
    }

    fn add_log(&mut self, entry: LogEntry) {
        if entry.level <= 3 {
            self.error_count += 1;
        } else if entry.level == 4 {
            self.warn_count += 1;
        }
        *self.host_counts.entry(entry.host.clone()).or_insert(0) += 1;
        if entry.id > self.last_id {
            self.last_id = entry.id;
        }
        self.total_count += 1;
        self.logs.push(entry);

        // Cap at 10K entries in memory
        if self.logs.len() > 10_000 {
            self.logs.drain(0..1000);
        }
    }

    fn update_rate(&mut self) {
        let elapsed = self.last_fetch.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            let new_count = self.logs.len();
            let delta = new_count.saturating_sub(self.last_count);
            self.msg_per_sec = delta as f64 / elapsed;
            self.last_count = new_count;
            self.last_fetch = Instant::now();
        }
    }

    fn level_filter_label(&self) -> &'static str {
        match self.level_filter {
            None => "ALL",
            Some(0) => "≥EMR",
            Some(1) => "≥ALR",
            Some(2) => "≥CRT",
            Some(3) => "≥ERR",
            Some(4) => "≥WRN",
            Some(5) => "≥NTC",
            Some(6) => "≥INF",
            Some(7) => "≥DBG",
            _ => "ALL",
        }
    }
}

// ─── UI rendering ───────────────────────────────────────────

fn render(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    app: &App,
    collection: &str,
    host: &str,
    port: u16,
) {
    let _ = terminal.draw(|frame| {
        let size = frame.area();

        // Main layout: [header] [body] [footer]
        let main_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3), // header
                Constraint::Min(5),    // body
                Constraint::Length(3), // footer
            ])
            .split(size);

        // Header
        let header_spans = vec![
            Span::styled(
                " oxidb-tail ",
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(format!("{host}:{port}"), Style::default().fg(Color::Cyan)),
            Span::raw("  "),
            Span::styled(collection, Style::default().fg(Color::Yellow)),
            Span::raw("  │  "),
            Span::styled(
                format!("{}", app.total_count),
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" total", Style::default().fg(Color::DarkGray)),
            Span::raw("  "),
            Span::styled(
                format!("{}", app.error_count),
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            ),
            Span::styled(" err", Style::default().fg(Color::DarkGray)),
            Span::raw("  "),
            Span::styled(
                format!("{}", app.warn_count),
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" wrn", Style::default().fg(Color::DarkGray)),
            Span::raw("  │  "),
            Span::styled(
                format!("{:.0}/s", app.msg_per_sec),
                Style::default().fg(Color::Green),
            ),
        ];
        let header = Paragraph::new(Line::from(header_spans)).block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(Style::default().fg(Color::DarkGray)),
        );
        frame.render_widget(header, main_chunks[0]);

        // Body: vertical split if detail open, otherwise horizontal
        let body_with_detail = if app.show_detail {
            Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Percentage(55), // logs + stats
                    Constraint::Percentage(45), // detail panel
                ])
                .split(main_chunks[1])
        } else {
            Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Percentage(100), Constraint::Length(0)])
                .split(main_chunks[1])
        };

        let body_chunks = if app.show_stats {
            Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Min(60), Constraint::Length(30)])
                .split(body_with_detail[0])
        } else {
            Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(100), Constraint::Length(0)])
                .split(body_with_detail[0])
        };

        // Log table (newest first — reversed, columnar layout)
        let filtered = app.filtered_logs();
        let log_area = body_chunks[0];
        let visible_height = log_area.height.saturating_sub(3) as usize; // header + borders

        let scroll = if app.auto_scroll {
            0
        } else {
            app.scroll_offset
                .min(filtered.len().saturating_sub(visible_height))
        };

        // Use locked columns
        let common_extra_keys = app.extra_columns.clone();

        let rows: Vec<Row> = filtered
            .iter()
            .rev()
            .skip(scroll)
            .take(visible_height)
            .enumerate()
            .map(|(i, log)| {
                let idx = scroll + i;
                let level_style = Style::default()
                    .fg(log.level_color())
                    .add_modifier(Modifier::BOLD);
                let is_selected = !app.auto_scroll && idx == app.selected;

                let mut cells = vec![
                    Cell::from(log.timestamp.clone()).style(Style::default().fg(Color::DarkGray)),
                    Cell::from(log.level_label()).style(level_style),
                    Cell::from(log.host.clone()).style(Style::default().fg(Color::Blue)),
                    Cell::from(truncate_str(&log.message, 50))
                        .style(Style::default().fg(Color::White)),
                ];

                // Dynamic extra columns
                for key in &common_extra_keys {
                    let val = log
                        .extra
                        .iter()
                        .find(|(k, _)| k == key)
                        .map(|(_, v)| truncate_str(v, 18))
                        .unwrap_or_default();
                    cells.push(Cell::from(val).style(Style::default().fg(Color::Cyan)));
                }

                let row = Row::new(cells);
                if is_selected {
                    row.style(Style::default().bg(Color::DarkGray))
                } else {
                    row
                }
            })
            .collect();

        // Build header
        let mut header_cells = vec![
            Cell::from("Time").style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Cell::from("Lvl").style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Cell::from("Host").style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Cell::from("Message").style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
        ];
        for key in &common_extra_keys {
            header_cells.push(
                Cell::from(key.as_str()).style(
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
            );
        }
        let header = Row::new(header_cells)
            .style(Style::default().bg(Color::Rgb(30, 30, 30)))
            .height(1);

        // Column widths
        let mut widths = vec![
            Constraint::Length(12), // Time
            Constraint::Length(3),  // Lvl
            Constraint::Length(18), // Host
            Constraint::Min(30),    // Message (flexible)
        ];
        for _ in &common_extra_keys {
            widths.push(Constraint::Length(19));
        }

        let log_block = Block::default()
            .title(format!(" Logs ({}) ", filtered.len()))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray));

        let table = Table::new(rows, widths)
            .header(header)
            .block(log_block)
            .row_highlight_style(Style::default().bg(Color::DarkGray));
        frame.render_widget(table, log_area);

        // Scrollbar
        if filtered.len() > visible_height {
            let mut scrollbar_state = ScrollbarState::new(filtered.len())
                .position(scroll)
                .viewport_content_length(visible_height);
            frame.render_stateful_widget(
                Scrollbar::new(ScrollbarOrientation::VerticalRight),
                log_area,
                &mut scrollbar_state,
            );
        }

        // Stats sidebar (toggle with Tab)
        if app.show_stats {
            let mut stats_lines: Vec<Line> = Vec::new();
            stats_lines.push(Line::from(vec![Span::styled(
                "Hosts",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )]));

            let mut sorted_hosts: Vec<(&String, &u64)> = app.host_counts.iter().collect();
            sorted_hosts.sort_by(|a, b| b.1.cmp(a.1));
            for (host, count) in sorted_hosts.iter().take(8) {
                stats_lines.push(Line::from(vec![
                    Span::styled(format!("{:>6} ", count), Style::default().fg(Color::White)),
                    Span::styled(
                        if host.len() > 18 { &host[..18] } else { host },
                        Style::default().fg(Color::Cyan),
                    ),
                ]));
            }

            stats_lines.push(Line::raw(""));
            stats_lines.push(Line::from(vec![
                Span::styled("Level Filter: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    app.level_filter_label(),
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
            ]));

            if app.auto_scroll {
                stats_lines.push(Line::from(Span::styled(
                    "● LIVE",
                    Style::default()
                        .fg(Color::Green)
                        .add_modifier(Modifier::BOLD),
                )));
            } else {
                stats_lines.push(Line::from(Span::styled(
                    "○ PAUSED",
                    Style::default().fg(Color::Yellow),
                )));
            }

            let stats_block = Block::default()
                .title(" Stats ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray));
            let stats = Paragraph::new(stats_lines).block(stats_block);
            frame.render_widget(stats, body_chunks[1]);
        }

        // Detail panel (full width, below logs)
        if app.show_detail && app.selected < filtered.len() {
            let sel = &filtered[filtered
                .len()
                .saturating_sub(1)
                .saturating_sub(app.selected)];
            let mut detail_lines: Vec<Line> = Vec::new();

            // Message line
            detail_lines.push(Line::from(vec![
                Span::styled("  Message   ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    &sel.message,
                    Style::default()
                        .fg(Color::White)
                        .add_modifier(Modifier::BOLD),
                ),
            ]));

            // Host + facility + level
            detail_lines.push(Line::from(vec![
                Span::styled("  Host      ", Style::default().fg(Color::DarkGray)),
                Span::styled(&sel.host, Style::default().fg(Color::Blue)),
                Span::styled("    Facility  ", Style::default().fg(Color::DarkGray)),
                Span::styled(&sel.facility, Style::default().fg(Color::Cyan)),
                Span::styled("    Level  ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    sel.level_label(),
                    Style::default()
                        .fg(sel.level_color())
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled("    Time  ", Style::default().fg(Color::DarkGray)),
                Span::styled(&sel.timestamp, Style::default().fg(Color::White)),
            ]));

            detail_lines.push(Line::from(Span::styled(
                "  ─────────────────────────────────────────",
                Style::default().fg(Color::DarkGray),
            )));

            // All extra fields — two columns
            let extras = &sel.extra;
            let mut i = 0;
            while i < extras.len() {
                let mut spans = Vec::new();
                spans.push(Span::raw("  "));

                // First column
                let (k1, v1) = &extras[i];
                spans.push(Span::styled(
                    format!("{:<20}", k1),
                    Style::default().fg(Color::Cyan),
                ));
                spans.push(Span::styled(
                    format!("{:<30}", v1),
                    Style::default().fg(Color::White),
                ));

                // Second column
                if i + 1 < extras.len() {
                    let (k2, v2) = &extras[i + 1];
                    spans.push(Span::styled(
                        format!("{:<20}", k2),
                        Style::default().fg(Color::Cyan),
                    ));
                    spans.push(Span::styled(v2.as_str(), Style::default().fg(Color::White)));
                }

                detail_lines.push(Line::from(spans));
                i += 2;
            }

            let detail_block = Block::default()
                .title(" Detail (Enter to close) ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Yellow));
            let detail = Paragraph::new(detail_lines).block(detail_block);
            frame.render_widget(detail, body_with_detail[1]);
        }

        // Footer
        let filter_display = if app.filter_active {
            format!("Filter: {}█", app.filter_text)
        } else if !app.filter_text.is_empty() {
            format!("Filter: {}", app.filter_text)
        } else {
            String::new()
        };

        let footer_spans = vec![
            Span::styled(
                " q",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" quit  ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                "/",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" filter  ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                "l",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" level  ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                "↑↓",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" scroll  ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                "↵",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" detail  ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                "Tab",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" stats  ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                "f",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" follow  ", Style::default().fg(Color::DarkGray)),
            Span::raw("  "),
            Span::styled(&filter_display, Style::default().fg(Color::White)),
        ];
        let footer = Paragraph::new(Line::from(footer_spans)).block(
            Block::default()
                .borders(Borders::TOP)
                .border_style(Style::default().fg(Color::DarkGray)),
        );
        frame.render_widget(footer, main_chunks[2]);
    });
}

// ─── Helpers ────────────────────────────────────────────────

fn truncate_str(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max.saturating_sub(1)])
    }
}

/// Find the N most common extra field keys across filtered logs.
fn find_common_extra_keys(logs: &[&LogEntry], n: usize) -> Vec<String> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for log in logs.iter().take(200) {
        for (k, _) in &log.extra {
            *counts.entry(k.clone()).or_insert(0) += 1;
        }
    }
    let mut sorted: Vec<(String, usize)> = counts.into_iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));
    sorted.into_iter().take(n).map(|(k, _)| k).collect()
}

// ─── Data fetcher ───────────────────────────────────────────

fn fetch_new_logs(stream: &mut TcpStream, collection: &str, last_id: u64) -> Vec<LogEntry> {
    let query = if last_id > 0 {
        json!({"_id": {"$gt": last_id}})
    } else {
        json!({})
    };
    let limit = if last_id > 0 { 200 } else { 100 };

    let resp = match send_recv(
        stream,
        &json!({
            "cmd": "find",
            "collection": collection,
            "query": query,
            "sort": {"_id": 1},
            "limit": limit,
        }),
    ) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };

    let mut entries = Vec::new();
    if let Some(docs) = resp.get("data").and_then(|v| v.as_array()) {
        for doc in docs {
            if let Some(entry) = LogEntry::from_doc(doc) {
                entries.push(entry);
            }
        }
    }
    entries
}

// ─── Main ───────────────────────────────────────────────────

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let mut stream = TcpStream::connect((&*cli.host, cli.port))?;
    let _ = stream.set_nodelay(true);

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new();
    let poll_interval = Duration::from_millis(cli.interval);
    let mut last_poll = Instant::now() - poll_interval; // trigger immediate first fetch

    loop {
        // Poll for new data
        if last_poll.elapsed() >= poll_interval {
            let new_logs = fetch_new_logs(&mut stream, &cli.collection, app.last_id);
            for entry in new_logs {
                app.add_log(entry);
            }
            app.update_rate();
            // Lock columns after first batch of logs
            if !app.columns_locked && app.logs.len() >= 10 {
                let filtered = app.filtered_logs();
                app.extra_columns = find_common_extra_keys(&filtered, 4);
                app.columns_locked = true;
            }
            last_poll = Instant::now();
        }

        // Render
        render(&mut terminal, &app, &cli.collection, &cli.host, cli.port);

        // Handle input (non-blocking, 50ms timeout)
        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                if app.filter_active {
                    // Filter input mode
                    match key.code {
                        KeyCode::Esc | KeyCode::Enter => {
                            app.filter_active = false;
                        }
                        KeyCode::Backspace => {
                            app.filter_text.pop();
                        }
                        KeyCode::Char(c) => {
                            app.filter_text.push(c);
                        }
                        _ => {}
                    }
                } else {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Char('Q') => break,
                        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            break;
                        }
                        KeyCode::Char('/') => {
                            app.filter_active = true;
                        }
                        KeyCode::Char('l') => {
                            // Cycle level filter: ALL → ERR → WRN → INF → DBG → ALL
                            app.level_filter = match app.level_filter {
                                None => Some(3),
                                Some(3) => Some(4),
                                Some(4) => Some(6),
                                Some(6) => Some(7),
                                _ => None,
                            };
                        }
                        KeyCode::Char('f') | KeyCode::Char('F') => {
                            app.auto_scroll = true;
                            app.show_detail = false;
                        }
                        KeyCode::Up | KeyCode::Char('k') => {
                            app.auto_scroll = false;
                            let filtered = app.filtered_logs();
                            if app.selected > 0 {
                                app.selected -= 1;
                            }
                            app.scroll_offset = app.selected.saturating_sub(5);
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            app.auto_scroll = false;
                            let filtered_len = app.filtered_logs().len();
                            if app.selected + 1 < filtered_len {
                                app.selected += 1;
                            }
                            app.scroll_offset = app.selected.saturating_sub(5);
                        }
                        KeyCode::PageUp => {
                            app.auto_scroll = false;
                            app.selected = app.selected.saturating_sub(20);
                            app.scroll_offset = app.selected.saturating_sub(5);
                        }
                        KeyCode::PageDown => {
                            app.auto_scroll = false;
                            let filtered_len = app.filtered_logs().len();
                            app.selected = (app.selected + 20).min(filtered_len.saturating_sub(1));
                            app.scroll_offset = app.selected.saturating_sub(5);
                        }
                        KeyCode::Home => {
                            app.auto_scroll = false;
                            app.selected = 0;
                            app.scroll_offset = 0;
                        }
                        KeyCode::Enter => {
                            if !app.auto_scroll {
                                app.show_detail = !app.show_detail;
                            }
                        }
                        KeyCode::Tab => {
                            app.show_stats = !app.show_stats;
                        }
                        KeyCode::End => {
                            app.auto_scroll = true;
                            app.show_detail = false;
                        }
                        KeyCode::Esc => {
                            if !app.filter_text.is_empty() {
                                app.filter_text.clear();
                            } else {
                                app.auto_scroll = true;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    Ok(())
}
