# oxidb-tail

Real-time interactive TUI log viewer for OxiDB. Streams structured GELF logs from an OxiDB server and displays them in a columnar table with color-coded severity levels.

## Installation

```bash
cargo build --release -p oxidb-tail
```

Binary: `target/release/oxidb-tail`

## Usage

```bash
oxidb-tail --host 127.0.0.1 --port 4444
oxidb-tail --host db.example.com --port 4444 --collection app_logs
oxidb-tail --host 192.0.2.6 --port 4444 --interval 1000
```

### Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--host` | Yes | — | OxiDB server hostname or IP address |
| `--port` | Yes | — | OxiDB server TCP port |
| `-c, --collection` | No | `_gelf_logs` | Collection to stream logs from |
| `--interval` | No | `500` | Poll interval in milliseconds |

## Features

- **Columnar table layout** — Time, Level, Host, Message, and auto-detected extra fields displayed in aligned columns
- **Color-coded levels** — ERR (red), WRN (yellow), INF (green), NTC (cyan), DBG (gray)
- **Auto-detected columns** — Automatically picks the most common extra fields (e.g. Username, ClientIp, RequestPath) as table columns
- **Live streaming** — New logs appear at the top in real-time
- **Full-text search** — Filter logs by typing any keyword
- **Level filtering** — Cycle through severity levels to focus on errors or warnings
- **Detail panel** — Expand any log entry to see all fields
- **Stats sidebar** — Host distribution and live/paused indicator
- **Scrollable history** — Navigate through log history with keyboard

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `q` | Quit |
| `/` | Search/filter logs (type to filter, Enter/Esc to close) |
| `l` | Cycle level filter: ALL → ERR → WRN → INF → DBG → ALL |
| `↑` `↓` / `j` `k` | Scroll through logs (pauses live mode) |
| `PgUp` `PgDn` | Fast scroll (20 lines) |
| `Enter` | Toggle detail panel for the selected log entry |
| `Tab` | Toggle stats sidebar |
| `f` | Resume live mode (follow new logs) |
| `Esc` | Clear search filter or resume live mode |
| `Home` | Jump to oldest log |
| `End` | Jump to newest log and resume live mode |

## Screenshot

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ oxidb-tail  192.0.2.6:4444  _gelf_logs  │  1523 total  42 err  87 wrn  12/s│
├─ Logs (1523) ───────────────────────────────────────────────────────────────┤
│ Time         Lvl Host             Message                    Username  ... │
│ 21:30:05.123 ERR example-api-01   Auth failed: hacker        hacker    ... │
│ 21:30:05.456 INF example-web-01   Request POST /Login 42ms   johndoe   ... │
│ 21:30:05.789 WRN example-worker   Rate limit: 192.0.2.17     admin     ... │
│ 21:30:06.012 DBG example-sched    Job SendEmail: 1250ms      fatma     ... │
│ 21:30:06.345 INF example-api-02   Order #54321 2500 TRY      baris     ... │
├─────────────────────────────────────────────────────────────────────────────┤
│ q quit  / filter  l level  ↑↓ scroll  ↵ detail  Tab stats  f follow       │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Requirements

- OxiDB server running with GELF ingestion enabled (`OXIDB_GELF_PORT`)
- TCP connectivity to the OxiDB server

## How It Works

1. Connects to OxiDB via TCP using the length-prefixed JSON wire protocol
2. Polls the GELF collection for new documents using `find` with `_id > last_id`
3. Auto-detects the most common extra fields from the first batch and locks them as table columns
4. Renders the TUI using [ratatui](https://github.com/ratatui/ratatui) with [crossterm](https://github.com/crossterm-rs/crossterm) backend
