//! cobra-run — decode, validate and execute a portable compiled Cobra file.
//!
//! Usage: cobra-run [--no-validate] file.cobrac
//!
//! The program's `print` output goes to stdout. Exit codes:
//!   0  clean run
//!   1  runtime error ("runtime error: <msg>" on stderr)
//!   2  usage / decode / validation failure

use std::process::ExitCode;

fn main() -> ExitCode {
    let mut validate = true;
    let mut path: Option<String> = None;
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--no-validate" => validate = false,
            _ if path.is_none() => path = Some(arg),
            _ => {
                eprintln!("usage: cobra-run [--no-validate] file.cobrac");
                return ExitCode::from(2);
            }
        }
    }
    let Some(path) = path else {
        eprintln!("usage: cobra-run [--no-validate] file.cobrac");
        return ExitCode::from(2);
    };

    let data = match std::fs::read(&path) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: cannot read {path}: {e}");
            return ExitCode::from(2);
        }
    };
    let bytecode = match oxidb_cobra::decode(&data) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };
    if validate && let Err(e) = oxidb_cobra::validate(&bytecode) {
        eprintln!("rejected: {e}");
        return ExitCode::from(2);
    }

    let mut vm = oxidb_cobra::vm::Vm::new(&bytecode);
    let result = vm.run();
    print!("{}", vm.output());
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("runtime error: {}", e.message);
            ExitCode::from(1)
        }
    }
}
