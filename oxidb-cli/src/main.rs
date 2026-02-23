mod display;
mod executor;
mod parser;

use std::path::PathBuf;

use clap::Parser;

use executor::{ClientExecutor, CommandExecutor, EmbeddedExecutor};

#[derive(Parser)]
#[command(name = "oxidb", about = "OxiDB interactive shell and CLI")]
struct Cli {
    /// Database directory (embedded mode)
    #[arg(long)]
    data: Option<PathBuf>,

    /// Server host (client mode)
    #[arg(long)]
    host: Option<String>,

    /// Server port (client mode, default 4444)
    #[arg(long, default_value_t = 4444)]
    port: u16,

    /// Execute expression and exit
    #[arg(long)]
    eval: Option<String>,

    /// Output raw JSON (no pretty-printing)
    #[arg(long)]
    json: bool,

    /// Encryption key file path (embedded mode)
    #[arg(long)]
    encryption_key: Option<PathBuf>,
}

fn main() {
    let cli = Cli::parse();

    let mut executor: Box<dyn CommandExecutor> = if let Some(host) = &cli.host {
        // Client mode
        match ClientExecutor::connect(host, cli.port) {
            Ok(e) => Box::new(e),
            Err(e) => {
                eprintln!("Error: {e}");
                std::process::exit(1);
            }
        }
    } else if let Some(data) = &cli.data {
        // Embedded mode
        let enc_key = cli.encryption_key.as_deref().map(|p| {
            oxidb::EncryptionKey::load_from_file(p)
                .unwrap_or_else(|e| {
                    eprintln!("Error loading encryption key: {e}");
                    std::process::exit(1);
                })
        });
        match EmbeddedExecutor::open(data, enc_key) {
            Ok(e) => Box::new(e),
            Err(e) => {
                eprintln!("Error: {e}");
                std::process::exit(1);
            }
        }
    } else {
        eprintln!("Error: specify --data <PATH> (embedded) or --host <HOST> (client)");
        std::process::exit(1);
    };

    if let Some(expr) = &cli.eval {
        // One-shot mode
        run_one(&mut *executor, expr, cli.json);
    } else {
        // REPL mode
        run_repl(&mut *executor, cli.json);
    }
}

fn run_one(executor: &mut dyn CommandExecutor, expr: &str, raw_json: bool) {
    let cmd = match parser::parse(expr) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Parse error: {e}");
            std::process::exit(1);
        }
    };
    match executor.execute(cmd) {
        Ok(response) => {
            println!("{}", display::format_response(&response, raw_json));
        }
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }
}

fn run_repl(executor: &mut dyn CommandExecutor, raw_json: bool) {
    let mut rl = match rustyline::DefaultEditor::new() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to initialize readline: {e}");
            std::process::exit(1);
        }
    };

    display::print_banner(env!("CARGO_PKG_VERSION"));

    loop {
        let prompt = "oxidb> ";
        let line = match rl.readline(prompt) {
            Ok(line) => line,
            Err(rustyline::error::ReadlineError::Interrupted | rustyline::error::ReadlineError::Eof) => {
                break;
            }
            Err(e) => {
                eprintln!("Error: {e}");
                break;
            }
        };

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let _ = rl.add_history_entry(line);

        let cmd = match parser::parse(line) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Parse error: {e}");
                continue;
            }
        };

        // Handle built-in commands
        if cmd.get("cmd").and_then(|v| v.as_str()) == Some("exit") {
            break;
        }
        if cmd.get("cmd").and_then(|v| v.as_str()) == Some("help") {
            display::print_help();
            continue;
        }

        match executor.execute(cmd) {
            Ok(response) => {
                println!("{}", display::format_response(&response, raw_json));
            }
            Err(e) => {
                eprintln!("Error: {e}");
            }
        }
    }
}
