use std::path::PathBuf;

use crate::fmt::DEFAULT_BUFFER_LINES;
use clap::{command, ArgAction, Args};
use lazy_static::lazy_static;

lazy_static! {
    static ref DEFAULT_BUFFER_LINES_STR: String = DEFAULT_BUFFER_LINES.to_string();
}

#[derive(Debug, Clone, Args)]
#[command(flatten_help = true)]
pub struct FmtArgs {
    /// Strip extra spaces around the values instead of padding them
    #[arg(short, long, action=ArgAction::SetTrue)]
    pub strip: bool,

    /// Delimiter to use to parse the tabular data. Default='\t' for any file other than .csv
    #[arg(short, long, required = false, value_parser=parse_char)]
    pub delimiter: Option<char>,

    /// Lines starting with this character will be skipped
    #[arg(short, long, default_value_t = '#', required = false, value_parser=parse_char)]
    pub comment_char: char,

    /// Do not skip comments, i.e. lines starting with --comment-char value
    #[arg(long, default_value_t = false, required = false, action=ArgAction::SetTrue)]
    pub no_skip_comments: bool,

    /// quote character
    #[arg(short, long, default_value_t = '"', required = false, value_parser=parse_char)]
    pub quote_char: char,

    /// Choose when to quote output
    #[arg(short, long, default_value = "necessary", required = false, value_parser=["necessary", "always", "never"])]
    pub quote_style: String,

    /// Exit if the rows do not have the same number of columns.
    #[arg(long="no-flexible", default_value_t = false, required = false, action=ArgAction::SetFalse)]
    pub flexible: bool,

    /// Do not format the whole file at one time but buffer the first 16384 lines to get the max width per column,
    /// then format line by line incrementing the max width when a bigger colums is found.
    /// You can specify the number of lines to buffer. Use 0 to format line by line from the start..
    #[arg(short, long, default_missing_value = DEFAULT_BUFFER_LINES_STR.as_str(), required=false, require_equals=true, num_args=0..=1, value_parser = clap::value_parser!(usize))]
    pub buffer_fmt: Option<usize>,

    /// Apply the formatting in place. Works only if an input is provided.
    /// Keep in mind that a new temporary file will be created, and then renamed.
    #[arg(short, long, action=ArgAction::SetTrue)]
    pub in_place: bool,

    /// Needed to properly format UTF-8 data. The default is to treat the data as just bytes
    /// because it's much faster
    #[arg(short, long, action=ArgAction::SetTrue)]
    pub utf8: bool,

    // Save the output to a file
    #[arg(short, long, value_parser=clap::value_parser!(PathBuf))]
    pub output: Option<PathBuf>,

    #[arg(value_name = "FILE")]
    pub input: Option<PathBuf>,
}

impl FmtArgs {
    pub fn check_args(&self) -> anyhow::Result<()> {
        if self.output.is_some() && self.in_place {
            anyhow::bail!("You cannot pass both --output and --in-place");
        }
        if self.in_place && self.input.is_none() {
            anyhow::bail!("You cannot pass --in-place flag without giving a file to format");
        }
        Ok(())
    }
}

fn parse_char(s: &str) -> Result<char, &'static str> {
    match s {
        "\\t" => Ok('\t'),
        s if s.chars().count() == 1 => Ok(s.chars().next().unwrap()),
        _ => Err("Delimiter must be a single character"),
    }
}

#[cfg(test)]
mod tests {

    use std::str::FromStr;

    use super::*;

    #[test]
    fn check_arguments() {
        let mut fmt_args = FmtArgs {
            strip: false,
            in_place: true,
            flexible: true,
            delimiter: Some(','),
            comment_char: '#',
            quote_char: '"',
            utf8: false,
            buffer_fmt: None,
            output: None,
            input: None,
            no_skip_comments: false,
        };

        // Input cannot be None if in_place is true
        fmt_args.input = None;
        fmt_args.output = None;
        fmt_args.in_place = true;
        assert!(fmt_args.check_args().is_err());

        // Output cannot be Some if in_place is true
        fmt_args.input = Some(PathBuf::from_str("test").unwrap());
        fmt_args.output = Some(PathBuf::from_str("test").unwrap());
        fmt_args.in_place = true;
        assert!(fmt_args.check_args().is_err());

        // Input is some and in_place is true -> Ok
        fmt_args.input = Some(PathBuf::from_str("test").unwrap());
        fmt_args.output = None;
        fmt_args.in_place = true;
        assert!(fmt_args.check_args().is_ok());
    }
}
