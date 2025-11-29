use std::process;

fn main() {
    if let Err(err) = tyrion::run_cli() {
        eprintln!("{err}");
        process::exit(1);
    }
}
