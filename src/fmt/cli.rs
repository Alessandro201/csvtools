use std::path::PathBuf;

use crate::fmt::DEFAULT_BUFFER_LINES;
use clap::{ArgAction, Args, ValueEnum};
use lazy_static::lazy_static;

lazy_static! {
    static ref DEFAULT_BUFFER_LINES_STR: String = DEFAULT_BUFFER_LINES.to_string();
}

#[derive(ValueEnum, Clone, Debug)]
pub enum Color {
    /// Always use colors
    Always,
    /// Never use colors
    Never,
    /// Use colors only when printing to terminal
    Auto,
}

impl Color {
    fn parse(s: &str) -> Result<Color, &'static str> {
        match s {
            "a" | "auto" => Ok(Color::Auto),
            "y" | "yes" | "always" | "t" | "true" => Ok(Color::Always),
            "n" | "no" | "never" | "f" | "false" => Ok(Color::Never),
            _ => Err("Delimiter must be a single character"),
        }
    }
}

#[derive(Args, Debug, Clone)]
#[command(flatten_help = true)]
pub struct FmtArgs {
    /// Strip extra spaces around the values instead of padding them
    #[arg(short, long, action=ArgAction::SetTrue)]
    pub strip: bool,

    /// Column delimited. By default ',' for .csv files and '\t' for anything else
    #[arg(short, long, required = false, value_parser=parse_char)]
    pub delimiter: Option<char>,

    /// Lines starting with this character will be skipped
    #[arg(long, default_value_t = '#', required = false, value_parser=parse_char)]
    pub comment_char: char,

    /// Do not skip comments, i.e. lines starting with --comment-char value
    #[arg(long, default_value_t = false, required = false, action=ArgAction::SetTrue)]
    pub no_skip_comments: bool,

    /// quote character
    #[arg(short, long, default_value_t = '"', required = false, value_parser=parse_char)]
    pub quote_char: char,

    /// Choose when to quote output
    #[arg(long, default_value = "necessary", required = false, value_parser=parse_quote_style)]
    pub quote_style: csv::QuoteStyle,

    /// Disable double quote escaping.
    ///
    /// By default double quotes inside a field are escaped with a double quote. If disabled they
    /// will be escaped with a backslash '\'.
    /// eg. text"wow  --enabled-> "text""wow" |  --disabled-> "text\"wow"
    #[arg(verbatim_doc_comment)]
    #[arg(long="no-double-quote", default_value_t = true, required = false, action=ArgAction::SetFalse)]
    pub double_quote: bool,

    /// Choose the line terminator. The default matches any '\r', '\n', '\r\n'.
    ///
    /// The available options are:
    /// - 'CRLF': matches any '\r', '\n', '\r\n'.
    /// - Exactly one character or an escapted code from ['\n', '\r', '\t']
    #[arg(verbatim_doc_comment)]
    #[arg(long, default_value = "CRLF", required = false, value_parser=parse_terminator)]
    pub terminator: csv::Terminator,

    /// Exit if the rows do not have the same number of columns.
    #[arg(long="no-flexible", default_value_t = true, required = false, action=ArgAction::SetFalse)]
    pub flexible: bool,

    /// Buffer and format the first lines, then proceed line by line incrementing the column width
    /// when a bigger one is found.
    ///
    /// You can specify the number of lines to buffer. Use 0 to format line by line from the start..
    #[arg(verbatim_doc_comment)]
    #[arg(short, long, default_missing_value = DEFAULT_BUFFER_LINES_STR.as_str(), required=false, require_equals=true, num_args=0..=1, value_parser = clap::value_parser!(usize))]
    pub buffer_fmt: Option<usize>,

    /// Apply the formatting in place. Works only if an input is provided.
    #[arg(short, long, action=ArgAction::SetTrue)]
    pub in_place: bool,

    /// Print the output colored by column
    #[arg(short, long, value_enum, default_value_t=Color::Auto, value_parser=Color::parse)]
    pub color: Color,

    /// Procees data as UTF-8 encoded. By default data is treated as bytes because it's much faster
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
            anyhow::bail!("You cannot use both --output and --in-place");
        }
        if self.in_place && self.input.is_none() {
            anyhow::bail!("You cannot use --in-place flag without a file to format");
        }
        Ok(())
    }
}

fn parse_char(s: &str) -> Result<char, &'static str> {
    match s {
        "\\t" => Ok('\t'),
        s if s.chars().count() == 1 => Ok(s.chars().next().unwrap()),
        _ => Err("Delimiter must be a single character or '\t'"),
    }
}

fn parse_terminator(s: &str) -> Result<csv::Terminator, &'static str> {
    let s: Vec<u8> = s.bytes().collect();
    if s == b"CRLF" {
        return Ok(csv::Terminator::CRLF);
    }
    if s.len() == 1 {
        return Ok(csv::Terminator::Any(s[0]));
    }
    if s.len() == 2 {
        if s[0] == b'\\' {
            match s[1] {
                b't' => return Ok(csv::Terminator::Any(b'\t')),
                b'n' => return Ok(csv::Terminator::Any(b'\n')),
                b'r' => return Ok(csv::Terminator::Any(b'\r')),
                _ => {
                    return Err("The terminator must be 'CRLF', a single ascii character or one of '\\n', '\\t', '\\r'")
                }
            }
        }
    }
    return Err("The terminator must be 'CRLF', a single ascii character or one of ['\\n', '\\t', '\\r']");
}

fn parse_quote_style(s: &str) -> Result<csv::QuoteStyle, &'static str> {
    match s.to_ascii_lowercase().as_str() {
        "necessary" => Ok(csv::QuoteStyle::Necessary),
        "always" => Ok(csv::QuoteStyle::Always),
        "never" => Ok(csv::QuoteStyle::Never),
        "non-numeric" | "nonnumeric" => Ok(csv::QuoteStyle::NonNumeric),
        _ => Err("Quote style can only be one of ['necessary', 'always', 'never', 'non-numeric']"),
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
            color: Color::Auto,
            flexible: true,
            delimiter: Some(','),
            comment_char: '#',
            quote_char: '"',
            double_quote: true,
            terminator: csv::Terminator::CRLF,
            utf8: false,
            buffer_fmt: None,
            output: None,
            input: None,
            no_skip_comments: false,
            quote_style: csv::QuoteStyle::Necessary,
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
