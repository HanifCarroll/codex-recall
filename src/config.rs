use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub index_path: PathBuf,
    pub source_roots: Vec<PathBuf>,
}
