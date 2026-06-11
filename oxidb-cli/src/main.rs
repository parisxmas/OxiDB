mod display;
mod executor;
mod migrate;
mod parser;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};

use executor::{ClientExecutor, CommandExecutor, EmbeddedExecutor};

#[derive(Parser)]
#[command(name = "oxidb", about = "OxiDB interactive shell and CLI", version)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Database directory (embedded mode, no-subcommand REPL)
    #[arg(long, global = false)]
    data: Option<PathBuf>,

    /// Server host (client mode, no-subcommand REPL)
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

#[derive(Subcommand)]
enum Command {
    /// On-disk format inspection and data migration (1.0 scaffold)
    Migrate(MigrateArgs),
}

#[derive(Args)]
struct MigrateArgs {
    #[command(subcommand)]
    action: MigrateAction,
}

#[derive(Subcommand)]
enum MigrateAction {
    /// Walk a data directory and report each file's on-disk format version
    Inspect {
        /// Database directory to inspect
        #[arg(long)]
        data: PathBuf,

        /// Emit JSON instead of a table
        #[arg(long)]
        json: bool,
    },
    /// Validate format versions and apply any pending migrations
    ///
    /// Today this is a validate-only no-op: every shipped format is at v1.
    /// The dispatch structure is in place for when a v2 format lands.
    Run {
        /// Database directory to migrate
        #[arg(long)]
        data: PathBuf,

        /// Don't actually mutate anything; print what would happen
        #[arg(long)]
        dry_run: bool,

        /// Skip the pre-mutation backup copy (default: backup is on)
        #[arg(long)]
        no_backup: bool,

        /// Mutate in-place instead of copy-out
        #[arg(long, conflicts_with = "out")]
        in_place: bool,

        /// Write migrated data to this directory (default: copy-out next to source)
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    if let Some(Command::Migrate(args)) = cli.command {
        return run_migrate(args);
    }

    run_shell(cli)
}

fn run_migrate(args: MigrateArgs) -> ExitCode {
    match args.action {
        MigrateAction::Inspect { data, json } => match migrate::inspect(&data) {
            Ok(reports) => {
                let mut stdout = std::io::stdout().lock();
                if let Err(e) = migrate::print_inspect(&reports, json, &mut stdout) {
                    eprintln!("Error: {e}");
                    return ExitCode::from(1);
                }
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("Error: {e}");
                ExitCode::from(1)
            }
        },
        MigrateAction::Run {
            data,
            dry_run,
            no_backup,
            in_place,
            out,
        } => {
            let opts = migrate::RunOptions {
                dry_run,
                no_backup,
                in_place,
                out,
            };
            match migrate::run(&data, &opts) {
                Ok(result) => {
                    println!(
                        "migrate: current={} older={} newer={} legacy={} unreadable={}",
                        result.current,
                        result.older,
                        result.newer,
                        result.legacy,
                        result.unreadable
                    );
                    if result.older == 0 && result.newer == 0 {
                        println!("no migration needed");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("Error: {e}");
                    ExitCode::from(1)
                }
            }
        }
    }
}

fn run_shell(cli: Cli) -> ExitCode {
    let mut executor: Box<dyn CommandExecutor> = if let Some(host) = &cli.host {
        // Client mode
        match ClientExecutor::connect(host, cli.port) {
            Ok(e) => Box::new(e),
            Err(e) => {
                eprintln!("Error: {e}");
                return ExitCode::from(1);
            }
        }
    } else if let Some(data) = &cli.data {
        // Embedded mode
        let enc_key = cli.encryption_key.as_deref().map(|p| {
            oxidb::EncryptionKey::load_from_file(p).unwrap_or_else(|e| {
                eprintln!("Error loading encryption key: {e}");
                std::process::exit(1);
            })
        });
        match EmbeddedExecutor::open(data, enc_key) {
            Ok(e) => Box::new(e),
            Err(e) => {
                eprintln!("Error: {e}");
                return ExitCode::from(1);
            }
        }
    } else {
        eprintln!(
            "Error: specify --data <PATH> (embedded), --host <HOST> (client), or a subcommand (try `oxidb migrate --help`)"
        );
        return ExitCode::from(1);
    };

    if let Some(expr) = &cli.eval {
        // One-shot mode
        run_one(&mut *executor, expr, cli.json)
    } else {
        // REPL mode
        run_repl(&mut *executor, cli.json);
        ExitCode::SUCCESS
    }
}

fn run_one(executor: &mut dyn CommandExecutor, expr: &str, raw_json: bool) -> ExitCode {
    let cmd = match parser::parse(expr) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Parse error: {e}");
            return ExitCode::from(1);
        }
    };
    match executor.execute(cmd) {
        Ok(response) => {
            println!("{}", display::format_response(&response, raw_json));
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("Error: {e}");
            ExitCode::from(1)
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
            Err(
                rustyline::error::ReadlineError::Interrupted | rustyline::error::ReadlineError::Eof,
            ) => {
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
