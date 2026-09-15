use crate::fmt::cli::Color;
use crate::fmt::cli::FmtArgs;
use crate::fmt::DEFAULT_BUFFER_LINES;
use crate::fmt::DEFAULT_DELIMITER;

use anyhow::{Context, Result};
use csv::{self, ByteRecord, QuoteStyle, StringRecord, Terminator};
use rand::{distributions::Alphanumeric, Rng};
use simplelog::debug;
use std::io::stdout;
use std::io::IsTerminal;
use std::io::{self};
use std::os::fd::AsFd;
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    process::exit,
};

const DEFAULT_TMP_VEC_SIZE: usize = 100;

const RESET_STYLE: &[u8; 4] = b"\x1b[0m";
const COLORS: [[u8; 5]; 6] = [
    *b"\x1b[31m", // red
    *b"\x1b[32m", // green
    *b"\x1b[33m", // yellow
    *b"\x1b[34m", // blue
    *b"\x1b[35m", // magenta
    *b"\x1b[36m", // cyan
];

// SAFETY: These are ANSI color codes, convertible to utf8. The unsafe allows to avoid
// str::from_utf8() and calls to .unwrap() which cannot be done in const statements.
const COLORS_STR: [&str; 6] = unsafe {
    [
        str::from_utf8_unchecked(&COLORS[0]),
        str::from_utf8_unchecked(&COLORS[1]),
        str::from_utf8_unchecked(&COLORS[2]),
        str::from_utf8_unchecked(&COLORS[3]),
        str::from_utf8_unchecked(&COLORS[4]),
        str::from_utf8_unchecked(&COLORS[5]),
    ]
};
const RESET_STYLE_STR: &str = unsafe { str::from_utf8_unchecked(RESET_STYLE) };

#[derive(Debug)]
struct CsvFormatter {
    delimiter: char,
    comment_char: Option<char>,
    quote_char: char,
    quote_style: QuoteStyle,
    double_quote: bool,
    flexible: bool,
    utf8: bool,
    use_color: bool,
    terminator: Terminator,
    buffer: Option<usize>,
}

impl Default for CsvFormatter {
    fn default() -> Self {
        Self {
            delimiter: '\t',
            comment_char: None,
            quote_char: '"',
            quote_style: QuoteStyle::Necessary,
            double_quote: true,
            flexible: true,
            utf8: false,
            use_color: false,
            terminator: Terminator::CRLF,
            buffer: None,
        }
    }
}

impl CsvFormatter {
    pub fn set_delimiter(self, delimiter: char) -> Self {
        CsvFormatter { delimiter, ..self }
    }
    pub fn set_comment_char(self, comment_char: Option<char>) -> Self {
        CsvFormatter { comment_char, ..self }
    }
    pub fn set_quote_char(self, quote_char: char) -> Self {
        CsvFormatter { quote_char, ..self }
    }
    pub fn set_quote_style(self, quote_style: QuoteStyle) -> Self {
        CsvFormatter { quote_style, ..self }
    }
    pub fn set_flexible(self, flexible: bool) -> Self {
        CsvFormatter { flexible, ..self }
    }
    pub fn set_utf8(self, utf8: bool) -> Self {
        CsvFormatter { utf8, ..self }
    }
    pub fn set_terminator(self, terminator: Terminator) -> Self {
        CsvFormatter { terminator, ..self }
    }
    pub fn set_double_quote(self, double_quote: bool) -> Self {
        CsvFormatter { double_quote, ..self }
    }
    pub fn set_buffer(self, buffer: Option<usize>) -> Self {
        CsvFormatter { buffer, ..self }
    }
    pub fn set_use_color(self, use_color: bool) -> Self {
        CsvFormatter { use_color, ..self }
    }
    pub fn set_use_color_inferred(self, color: Color, istty: Option<bool>) -> Self {
        match color {
            Color::Always => self.set_use_color(true),
            Color::Never => self.set_use_color(false),
            Color::Auto => self.set_use_color(istty.is_some_and(|tty| tty == true)),
        }
    }
    pub fn from_args(fmt_args: &FmtArgs) -> Self {
        Self::default()
            .set_delimiter(get_delimiter(fmt_args))
            .set_comment_char({
                if fmt_args.no_skip_comments {
                    None
                } else {
                    Some(fmt_args.comment_char)
                }
            })
            .set_flexible(fmt_args.flexible)
            .set_quote_char(fmt_args.quote_char)
            .set_quote_style(fmt_args.quote_style)
            .set_double_quote(fmt_args.double_quote)
            .set_terminator(fmt_args.terminator)
            .set_utf8(fmt_args.utf8)
            .set_buffer(fmt_args.buffer_fmt)
            .set_quote_char(fmt_args.quote_char)
            .set_use_color_inferred(fmt_args.color.clone(), None)
    }
}

impl CsvFormatter {
    fn _build_csv_reader<'r, R: io::Read>(&self, in_stream: &'r mut R) -> csv::Reader<&'r mut R> {
        csv::ReaderBuilder::new()
            .delimiter(self.delimiter as u8)
            .has_headers(false)
            .flexible(self.flexible)
            .quote(self.quote_char as u8)
            .double_quote(self.double_quote)
            .comment(None)
            .terminator(self.terminator)
            .buffer_capacity(256 * 1024)
            .from_reader(in_stream)
    }

    fn _build_csv_writer<'w, W: io::Write>(&self, out_stream: &'w mut W) -> csv::Writer<&'w mut W> {
        csv::WriterBuilder::new()
            .delimiter(self.delimiter as u8)
            .has_headers(false)
            .flexible(self.flexible)
            .quote(self.quote_char as u8)
            .double_quote(self.double_quote)
            .quote_style(self.quote_style)
            .comment(None)
            // .terminator(self.terminator)
            .buffer_capacity(256 * 1024)
            .from_writer(out_stream)
    }

    pub fn strip<R, W>(&self, in_stream: &mut R, out_stream: &mut W) -> Result<()>
    where
        R: io::Read,
        W: io::Write,
    {
        let rdr = self._build_csv_reader(in_stream);
        let wrt = self._build_csv_writer(out_stream);

        if self.utf8 {
            self._strip(rdr, wrt)?
        } else {
            self._strip_bytes(rdr, wrt)?
        }

        Ok(())
    }

    pub fn format<R, W>(&self, in_stream: &mut R, out_stream: &mut W) -> Result<()>
    where
        R: io::Read,
        W: io::Write,
    {
        let rdr = self._build_csv_reader(in_stream);
        let wrt = self._build_csv_writer(out_stream);

        if self.utf8 {
            self._format(rdr, wrt)?
        } else {
            self._format_byte(rdr, wrt)?
        }

        Ok(())
    }

    pub fn format_file<P, W>(self, file: P, out_stream: &mut W) -> Result<()>
    where
        P: AsRef<Path>,
        W: io::Write,
    {
        let mut file = fs::File::open(file)?;
        self.format(&mut file, out_stream)
    }
}

impl CsvFormatter {
    fn _format_byte<R: io::Read, W: io::Write>(
        &self,
        mut rdr: csv::Reader<&mut R>,
        mut wrt: csv::Writer<&mut W>,
    ) -> Result<()> {
        if let Some(buffer_lines) = self.buffer {
            let cols_width = pad_and_write_buffered_byte(
                &mut wrt,
                &mut rdr,
                Some(buffer_lines),
                self.comment_char.map(|c| c as u8),
                self.use_color,
            )?;
            wrt.flush()?;

            pad_and_write_unbuffered_byte(
                &mut wrt,
                &mut rdr,
                cols_width,
                self.comment_char.map(|c| c as u8),
                self.use_color,
            )?;
            wrt.flush()?;
        } else {
            pad_and_write_buffered_byte(
                &mut wrt,
                &mut rdr,
                None,
                self.comment_char.map(|c| c as u8),
                self.use_color,
            )?;
            wrt.flush()?;
        }

        Ok(())
    }

    fn _format<R: io::Read, W: io::Write>(
        &self,
        mut rdr: csv::Reader<&mut R>,
        mut wrt: csv::Writer<&mut W>,
    ) -> Result<()> {
        let mut buffer: Vec<StringRecord> = Vec::with_capacity(DEFAULT_BUFFER_LINES);
        if let Some(buffer_lines) = self.buffer {
            for (line_n, record) in rdr.records().enumerate() {
                if line_n == buffer_lines {
                    buffer.push(record?);
                    break;
                }
                buffer.push(record?);
            }

            let cols_width = pad_and_write_buffered(&mut wrt, &buffer, self.comment_char, self.use_color)?;
            wrt.flush()?;

            pad_and_write_unbuffered(&mut wrt, &mut rdr, self.comment_char, cols_width, self.use_color)?;
            wrt.flush()?;
        } else {
            // TODO: Add saving the buffer to a file in case it exceed the memory available
            for record in rdr.records() {
                let record = record?;
                buffer.push(record);
            }
            pad_and_write_buffered(&mut wrt, &buffer, self.comment_char, self.use_color)?;
            wrt.flush()?;
        }

        Ok(())
    }

    fn _strip<R, W>(&self, mut rdr: csv::Reader<&mut R>, mut wrt: csv::Writer<&mut W>) -> Result<()>
    where
        R: io::Read,
        W: io::Write,
    {
        let mut raw_record: StringRecord = StringRecord::new();
        while rdr.read_record(&mut raw_record)? {
            if self.comment_char.is_some() && is_comment(&raw_record, self.comment_char.unwrap()) {
                wrt.write_record(&raw_record)?;
                continue;
            }

            for field in raw_record.iter() {
                wrt.write_field(field.trim())?
            }

            wrt.write_record(None::<&[u8]>)?
        }
        wrt.flush()?;
        Ok(())
    }

    fn _strip_bytes<R, W>(&self, mut rdr: csv::Reader<&mut R>, mut wrt: csv::Writer<&mut W>) -> Result<()>
    where
        R: io::Read,
        W: io::Write,
    {
        let mut raw_record: ByteRecord = ByteRecord::new();
        while rdr.read_byte_record(&mut raw_record)? {
            if self.comment_char.is_some() && is_comment_byte(&raw_record, self.comment_char.unwrap() as u8) {
                wrt.write_byte_record(&raw_record)?;
                continue;
            }

            for field in raw_record.iter() {
                wrt.write_field(field.trim_ascii())?
            }

            wrt.write_record(None::<&[u8]>)?
        }
        wrt.flush()?;
        Ok(())
    }
}

#[inline(always)]
fn is_comment(record: &csv::StringRecord, comment_char: char) -> bool {
    record.get(0).unwrap_or("").starts_with(comment_char)
}

#[inline(always)]
fn is_comment_byte(record: &csv::ByteRecord, comment_char: u8) -> bool {
    record.get(0).unwrap_or(b"").starts_with(&[comment_char])
}

// Get the delimiter either from the FmtArgs, from the file extension or the default one.
fn get_delimiter(fmt_args: &FmtArgs) -> char {
    if let Some(delimiter) = fmt_args.delimiter {
        return delimiter;
    }

    match fmt_args.input.as_ref().map(|path| path.extension()).flatten() {
        Some(ext) if ext == "csv" => ',',
        Some(ext) if ext == "tsv" => '\t',
        Some(ext) if ext == "tab" => '\t',
        _ => DEFAULT_DELIMITER,
    }
}

pub fn pad_and_write_unbuffered<W, R>(
    wrt: &mut csv::Writer<W>,
    rdr: &mut csv::Reader<R>,
    comment_char: Option<char>,
    mut cols_width: Vec<usize>,
    use_color: bool,
) -> Result<Vec<usize>>
where
    W: io::Write,
    R: io::Read,
{
    let mut tmp_spaces = " ".repeat(*cols_width.iter().max().unwrap_or(&1));
    let mut tmp_field = String::with_capacity(cols_width.iter().sum());
    let mut tmp_record = StringRecord::with_capacity(cols_width.iter().sum(), cols_width.len());
    let mut record = StringRecord::new();

    while rdr.read_record(&mut record)? {
        if comment_char.is_some_and(|c| is_comment(&record, c)) {
            wrt.write_record(&record)?;
            continue;
        }

        // Skip empty lines or filled with only spaces
        if record.len() <= 1 && record.get(0).unwrap_or("").trim().is_empty() {
            tmp_record.clear();
            wrt.write_record(&tmp_record)?;
            continue;
        }

        if record.len() > cols_width.len() {
            cols_width.resize(record.len(), 0);
        }

        tmp_record.clear();
        for (col, field) in record.iter().map(|field| field.trim_end()).enumerate() {
            // Trimmed and added 1 for the the space at the end
            let field_width = field.chars().count();
            if cols_width[col] < field_width {
                cols_width[col] = field_width;
                tmp_spaces = " ".repeat(tmp_spaces.len().max(field_width));
            }

            tmp_field.clear();

            if use_color {
                tmp_field.push_str(COLORS_STR[col % COLORS.len()]);
            }
            tmp_field.push_str(field);

            // if the field is not the last, and the max_width is not 0 then add a space at the end
            if col != record.len() - 1 && cols_width[col] != 0 {
                tmp_field.push_str(&tmp_spaces[0..(cols_width[col] - field_width + 1)]);
            }
            if use_color {
                tmp_field.push_str(RESET_STYLE_STR);
            }
            tmp_record.push_field(&tmp_field);
        }
        wrt.write_record(&tmp_record)?;
    }

    Ok(cols_width)
}

pub fn pad_and_write_buffered<W>(
    wrt: &mut csv::Writer<W>,
    buffer: &[StringRecord],
    comment_char: Option<char>,
    use_color: bool,
) -> Result<Vec<usize>>
where
    W: io::Write,
{
    let mut cols_width: Vec<usize> = Vec::new();

    for record in buffer.iter() {
        if comment_char.is_some_and(|c| is_comment(record, c)) {
            continue;
        }

        if cols_width.len() < record.len() {
            cols_width.resize(record.len(), 0);
        }

        // Each field is trimmed
        for (col, field_width) in record.iter().map(|field| field.trim_end().chars().count()).enumerate() {
            if cols_width[col] < field_width {
                cols_width[col] = field_width
            }
        }
    }

    let tmp_spaces = " ".repeat(*cols_width.iter().max().unwrap_or(&1));
    let mut tmp_field = String::with_capacity(cols_width.iter().max().unwrap_or(&0) + DEFAULT_TMP_VEC_SIZE);
    for record in buffer.iter() {
        if comment_char.is_some_and(|c| is_comment(record, c)) {
            wrt.write_record(record)?;
            continue;
        }

        // Skip empty lines or filled with only spaces
        if record.len() <= 1 && record.get(0).unwrap_or("").trim().is_empty() {
            wrt.write_record(None::<&[u8]>)?;
            continue;
        }

        for (col, field) in record.iter().map(|field| field.trim_end()).enumerate() {
            tmp_field.clear();
            if use_color {
                tmp_field.push_str(COLORS_STR[col % COLORS.len()]);
            }
            tmp_field.push_str(field);

            // if the field is not the last, and the max_width is not 0 then add a space at the end
            if col != record.len() - 1 && cols_width[col] != 0 {
                tmp_field.push_str(&tmp_spaces[0..(cols_width[col] - field.chars().count() + 1)]);
            }
            if use_color {
                tmp_field.push_str(RESET_STYLE_STR);
            }
            wrt.write_field(&tmp_field)?;
        }
        wrt.write_record(None::<&[u8]>)?;
    }
    Ok(cols_width)
}

pub fn pad_and_write_unbuffered_byte<W, R>(
    wrt: &mut csv::Writer<W>,
    rdr: &mut csv::Reader<R>,
    mut cols_width: Vec<usize>,
    comment_char: Option<u8>,
    add_color: bool,
) -> Result<Vec<usize>>
where
    W: io::Write,
    R: io::Read,
{
    let mut tmp_field = Vec::with_capacity(cols_width.iter().max().unwrap_or(&0) + DEFAULT_TMP_VEC_SIZE);
    let mut raw_record = ByteRecord::new();

    while rdr.read_byte_record(&mut raw_record)? {
        if comment_char.is_some_and(|c| is_comment_byte(&raw_record, c)) {
            wrt.write_byte_record(&raw_record)?;
            continue;
        }

        // Skip empty lines or filled with only spaces
        if raw_record.len() <= 1 && raw_record.get(0).unwrap_or(b"").trim_ascii().is_empty() {
            wrt.write_record(None::<&[u8]>)?;
            continue;
        }

        if raw_record.len() > cols_width.len() {
            cols_width.resize(raw_record.len(), 0);
        }

        for (col, field) in raw_record.iter().map(|field| field.trim_ascii_end()).enumerate() {
            if cols_width[col] < field.len() {
                cols_width[col] = field.len();
            }

            // if the field is not the last, and the max_width is not 0 then add a space at the end
            let padding = if col != raw_record.len() - 1 && cols_width[col] != 0 {
                cols_width[col] - field.len() + 1
            } else {
                0
            };

            // Fast path for the last column in case --color=never
            if !add_color && padding == 0 {
                wrt.write_field(field)?;
                continue;
            }

            tmp_field.clear();

            if add_color {
                tmp_field.extend_from_slice(&COLORS[col % COLORS.len()]);
            }

            tmp_field.extend_from_slice(field);
            tmp_field.resize(tmp_field.len() + padding, b' ');

            if add_color {
                tmp_field.extend_from_slice(RESET_STYLE);
            }

            wrt.write_field(&tmp_field)?;
        }
        wrt.write_record(None::<&[u8]>)?;
    }

    Ok(cols_width)
}

pub fn pad_and_write_buffered_byte<W, R>(
    wrt: &mut csv::Writer<W>,
    rdr: &mut csv::Reader<R>,
    buffer_lines: Option<usize>,
    comment_char: Option<u8>,
    add_color: bool,
) -> Result<Vec<usize>>
where
    W: io::Write,
    R: io::Read,
{
    let mut cols_width: Vec<usize> = Vec::new();
    let mut buffer: Vec<ByteRecord>;
    let iterator;

    if let Some(buffer_lines) = buffer_lines {
        buffer = Vec::with_capacity(buffer_lines);
        iterator = rdr.byte_records().take(buffer_lines);
    } else {
        buffer = Vec::new();
        iterator = rdr.byte_records().take(usize::MAX);
    };

    for record in iterator {
        let record = record?;
        if !comment_char.is_some_and(|c| is_comment_byte(&record, c)) {
            if cols_width.len() < record.len() {
                cols_width.resize(record.len(), 0);
            }

            // Each field is trimmed
            for (col, field_width) in record.iter().map(|field| field.trim_ascii_end().len()).enumerate() {
                if cols_width[col] < field_width {
                    cols_width[col] = field_width
                }
            }
        }

        buffer.push(record);
    }

    let mut tmp_field = Vec::with_capacity(cols_width.iter().max().unwrap_or(&0) + DEFAULT_TMP_VEC_SIZE);
    for record in buffer.iter() {
        if comment_char.is_some_and(|c| is_comment_byte(record, c)) {
            wrt.write_byte_record(record)?;
            continue;
        }

        // Skip empty lines or filled with only spaces
        if record.len() <= 1 && record.get(0).unwrap_or(b"").trim_ascii().is_empty() {
            wrt.write_record(None::<&[u8]>)?;
            continue;
        }

        for (col, field) in record.iter().map(|field| field.trim_ascii_end()).enumerate() {
            // if the field is not the last, and the max_width is not 0 then add a space at the end
            let padding = if col != record.len() - 1 && cols_width[col] != 0 {
                cols_width[col] - field.len() + 1
            } else {
                0
            };

            // Fast path for the last column in case --color=never
            if !add_color && padding == 0 {
                wrt.write_field(field)?;
                continue;
            }

            tmp_field.clear();

            if add_color {
                tmp_field.extend_from_slice(&COLORS[col % COLORS.len()]);
            }

            tmp_field.extend_from_slice(field);
            tmp_field.resize(tmp_field.len() + padding, b' ');

            if add_color {
                tmp_field.extend_from_slice(RESET_STYLE);
            }

            wrt.write_field(&tmp_field)?;
        }
        wrt.write_record(None::<&[u8]>)?;
    }
    Ok(cols_width)
}

fn get_in_stream(in_file: Option<&PathBuf>) -> Result<Box<dyn io::Read>> {
    let in_stream: Box<dyn io::Read> = if let Some(input_file) = in_file {
        let file_handle = File::open(input_file).context(format!("Error in opening input file {:?}", input_file))?;
        Box::new(file_handle)
    } else {
        Box::new(io::stdin().lock())
    };
    Ok(in_stream)
}

/// Depending on the format_args decide if and which file is the output
fn get_output_dest(fmt_args: &FmtArgs) -> Option<PathBuf> {
    let output_file = if fmt_args.output.is_some() {
        fmt_args.output.clone()
    } else if fmt_args.input.is_some() && fmt_args.in_place {
        let mut random_name: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect();
        let mut output_file = fmt_args.input.as_ref().unwrap().parent().unwrap().join(&random_name);

        while output_file.exists() {
            random_name.push('x');
            output_file.set_file_name(&random_name);
        }
        Some(output_file)
    } else {
        None
    };

    output_file
}

/// If an output file is given open it, otherwise acquire a lock to stdout
fn get_out_stream(out_file: Option<&PathBuf>) -> Result<Box<dyn io::Write>> {
    let out_stream: Box<dyn io::Write> = if let Some(ref output_file) = out_file {
        let file_handle = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(output_file)
            .context(format!("Error in opening output file {:?}", output_file))?;
        Box::new(file_handle)
    } else {
        let owned = std::io::stdout().as_fd().try_clone_to_owned().unwrap();
        let file_handle = std::fs::File::from(owned);
        Box::new(file_handle)
    };
    Ok(out_stream)
}

pub fn run(fmt_args: FmtArgs) -> anyhow::Result<()> {
    fmt_args.check_args()?;

    let in_file: Option<PathBuf> = fmt_args.input.clone();
    let mut in_stream: Box<dyn io::Read> = get_in_stream(in_file.as_ref())?;
    let out_file: Option<PathBuf> = get_output_dest(&fmt_args);
    let mut out_stream: Box<dyn io::Write> = get_out_stream(out_file.as_ref())?;
    // Set true if writing to STDOUT and STDOUT prints to the terminal
    let istty: bool = out_file.is_none() && stdout().is_terminal();
    debug!("in_file: {:?}", &in_file);
    debug!("out_file: {:?}", &out_file);
    debug!("istty: {istty}");

    if fmt_args.in_place {
        // The arguments should have already been checked
        debug_assert!(fmt_args.output.is_none());
        debug_assert!(fmt_args.input.is_some());

        // There should be a file to write to that will then be renamed as the original file
        debug_assert!(out_file.is_some());
    }

    // Set CTRL+C signal handler. Removes temporary files if present and stop the process
    if fmt_args.in_place {
        let out_file_copy = out_file.clone().unwrap();
        ctrlc::set_handler(move || {
            fs::remove_file(&out_file_copy).expect("Unable to remove temporary file");
            exit(2)
        })
        .expect("Error setting Ctrl-C handler");
    } else {
        ctrlc::set_handler(|| exit(2)).expect("Error setting Ctrl-C handler");
    }

    let formatter = CsvFormatter::from_args(&fmt_args).set_use_color_inferred(fmt_args.color, Some(istty));
    debug!("formatter args: {:#?}", &formatter);

    if fmt_args.strip {
        formatter.strip(&mut in_stream, &mut out_stream)?
    } else if let Some(in_file) = in_file.as_ref() {
        formatter.format_file(in_file, &mut out_stream)?
    } else {
        formatter.format(&mut in_stream, &mut out_stream)?
    };

    // In case the --in-place flag was given overwrite the original file with the temporary one
    if fmt_args.in_place {
        let in_file = in_file.expect("Program logic error: Input should have been given with the --in-place flag.");
        let out_file = out_file.expect("Program logic error: Output file should have already been specified.");
        debug!(
            "Replacing input file with tmp output: {:?} -- renamed to -> {:?}",
            &out_file, &in_file
        );
        fs::rename(&out_file, &in_file)
            .context(format!(
                "Unable to overwrite the original file with the temporary formatted file. \nTmp file: {:?} --X-> input file: {:?}",
                &out_file,
                &in_file)
            )?
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    use anyhow::Result;

    use super::*;

    fn run_format(formatter: &CsvFormatter, in_stream: &[u8]) -> Result<Vec<u8>> {
        let mut out_stream: Vec<u8> = vec![];
        let mut in_stream = in_stream;
        formatter.format(&mut in_stream, &mut out_stream)?;
        Ok(out_stream)
    }

    #[test]
    fn parse_comments() -> Result<()> {
        let in_stream: &[u8] = br#"

# Comment1
# Comment2   
"#;

        let correct_out_stream: &[u8] = br#"# Comment1
# Comment2   
"#;

        let formatter = CsvFormatter::default().set_delimiter(',').set_comment_char(Some('#'));
        let out_stream = run_format(&formatter, in_stream)?;
        if out_stream != correct_out_stream {
            println!("in_stream: \n{}", String::from_utf8_lossy(in_stream));
            println!("out_stream: \n{}", String::from_utf8_lossy(&out_stream));
            println!("correct_out_stream: \n{}", String::from_utf8_lossy(correct_out_stream));
        }
        assert_eq!(out_stream, correct_out_stream);
        Ok(())
    }

    #[test]
    fn parse_extra_fields() -> Result<()> {
        let in_stream: &[u8] = br#"
ciao1,wow
ciao1 tutti,wow
ciao2,gatto,extra field
"#;

        let correct_out_stream: &[u8] = br#"ciao1       ,wow
ciao1 tutti ,wow
ciao2       ,gatto ,extra field
"#;

        let formatter = CsvFormatter::default().set_delimiter(',').set_comment_char(Some('#'));
        let out_stream = run_format(&formatter, in_stream)?;
        if out_stream != correct_out_stream {
            println!("in_stream: \n{}", String::from_utf8_lossy(in_stream));
            println!("out_stream: \n{}", String::from_utf8_lossy(&out_stream));
            println!("correct_out_stream: \n{}", String::from_utf8_lossy(correct_out_stream));
        }
        assert_eq!(out_stream, correct_out_stream);
        Ok(())
    }

    #[test]
    fn parse_extra_fields_panic() {
        let in_stream: &[u8] = br#"
ciao1,wow
ciao1 tutti,wow
ciao2,gatto,extra field
"#;

        let formatter = CsvFormatter::default()
            .set_delimiter(',')
            .set_comment_char(Some('#'))
            .set_flexible(false);
        let out_stream = run_format(&formatter, in_stream);
        assert!(out_stream.is_err())
    }

    #[test]
    fn parse_empty_fields() -> Result<()> {
        let in_stream: &[u8] = br#"
ciao1,      ,
ciao1 , ,
ciao2,,extra field
"#;

        let correct_out_stream: &[u8] = br#"ciao1 ,,
ciao1 ,,
ciao2 ,,extra field
"#;

        let formatter = CsvFormatter::default().set_delimiter(',').set_comment_char(Some('#'));
        let out_stream = run_format(&formatter, in_stream)?;
        if out_stream != correct_out_stream {
            println!("in_stream: \n{}", String::from_utf8_lossy(in_stream));
            println!("out_stream: \n{}", String::from_utf8_lossy(&out_stream));
            println!("correct_out_stream: \n{}", String::from_utf8_lossy(correct_out_stream));
        }
        assert_eq!(out_stream, correct_out_stream);
        Ok(())
    }

    #[test]
    /// Only a space should be left at the end of a line
    /// Spaces between the delimiter and the next field are preserved
    /// Spaces between the last character of a field and the next delimiter are not preserved
    /// A space should be between the last character of a field and the next delimiter
    /// Empty lines and lines with just spaces should be removed
    fn parse_spaces() -> Result<()> {
        let in_stream: &[u8] = br#"

ciao1            ,wow 
ciao1 tutti,wow 
ciao2,   gatto
ciao2,   gatto   
                        
                        
                        
"#;

        let correct_out_stream: &[u8] = br#"ciao1       ,wow
ciao1 tutti ,wow
ciao2       ,   gatto
ciao2       ,   gatto



"#;

        let formatter = CsvFormatter::default().set_delimiter(',').set_comment_char(Some('#'));
        let out_stream = run_format(&formatter, in_stream)?;
        if out_stream != correct_out_stream {
            println!("in_stream: \n{}", String::from_utf8_lossy(in_stream));
            println!("out_stream: \n{}", String::from_utf8_lossy(&out_stream));
            println!("correct_out_stream: \n{}", String::from_utf8_lossy(correct_out_stream));
        }
        assert_eq!(out_stream, correct_out_stream);
        Ok(())
    }

    #[test]
    fn parse_quotes() -> Result<()> {
        let in_stream: &[u8] = br#"
ciao1,"wow"
"ciao1 tutti,wow ",ciao
"ciao1 tutti,wow ", test
ciao2," ""  ,gatto,"
"#;

        let correct_out_stream: &[u8] = br#"ciao1              ,"wow"
"ciao1 tutti,wow " ,ciao
"ciao1 tutti,wow " , test
ciao2              ," ""  ,gatto,"
"#;

        let formatter = CsvFormatter::default().set_delimiter(',').set_comment_char(Some('#'));
        let out_stream = run_format(&formatter, in_stream)?;
        if out_stream != correct_out_stream {
            println!("in_stream: \n{}", String::from_utf8_lossy(in_stream));
            println!("out_stream: \n{}", String::from_utf8_lossy(&out_stream));
            println!("correct_out_stream: \n{}", String::from_utf8_lossy(correct_out_stream));
        }
        assert_eq!(out_stream, correct_out_stream);
        Ok(())
    }

    #[test]
    fn parse_correctly() -> Result<()> {
        let in_stream: &[u8] = br#"
# Comment1
# Comment2
ciao1              , wow      
ciao2, gatto, extra field
ciao3,,       miao_spacessss
ciao3," ,  miao_spacessss"        
ciao3,"" ,  miao_spacessss

# Comment2

"#;

        let correct_out_stream: &[u8] = br#"# Comment1
# Comment2
ciao1 , wow
ciao2 , gatto               , extra field
ciao3 ,                     ,       miao_spacessss
ciao3 ," ,  miao_spacessss"
ciao3 ,                     ,  miao_spacessss

# Comment2

"#;

        let formatter = CsvFormatter::default().set_delimiter(',').set_comment_char(Some('#'));
        let out_stream = run_format(&formatter, in_stream)?;
        if out_stream != correct_out_stream {
            println!("in_stream: \n{}", String::from_utf8_lossy(in_stream));
            println!("out_stream: \n{}", String::from_utf8_lossy(&out_stream));
            println!("correct_out_stream: \n{}", String::from_utf8_lossy(correct_out_stream));
        }
        assert_eq!(out_stream, correct_out_stream);
        Ok(())
    }
}
