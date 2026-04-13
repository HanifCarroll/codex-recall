fn main() {
    if let Err(error) = codex_recall::run() {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}
