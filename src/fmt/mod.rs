pub mod cli;
pub mod run;
pub use run::run;

const DEFAULT_DELIMITER: char = '\t';
const DEFAULT_BUFFER_LINES: usize = 1024;
