use anyhow::{bail, Context, Result};
use clap::{ArgAction, Args};
use csv::{self, ByteRecord, QuoteStyle};
use lazy_static::lazy_static;
use memmap::Mmap;
use std::{
    fs::{File, OpenOptions},
    io::{self},
    path::{Path, PathBuf},
};

const DEFAULT_BUFFER_LINES: usize = 1024 * 16;
lazy_static! {
    static ref DEFAULT_BUFFER_LINES_STR: String = DEFAULT_BUFFER_LINES.to_string();
}

#[derive(Debug, Clone, Args)]
#[command(flatten_help = true)]
pub struct FmtArgs {
    /// Strip extra spaces around the values instead of padding them
    #[arg(short, long, action=ArgAction::SetTrue)]
    strip: bool,

    /// Delimiter to use to parse the tabular data
    #[arg(short, long, default_value_t = '\t', required = false, value_parser=parse_char)]
    delimiter: char,

    /// Lines starting with this character will be ingored
    #[arg(short, long, default_value_t = '#', required = false)]
    comment_char: char,

    /// quote character
    #[arg(short, long, default_value_t = '"', required = false, value_parser=parse_char)]
    quote_char: char,

    /// Do not format the whole file at one time but buffer the first 16384 lines to get the max width per column,
    /// then format line by line incrementing the max width when a bigger colums is found.
    /// You can specify the number of lines to buffer. Use 0 to format line by line from the start..
    #[arg(short, long, default_missing_value = DEFAULT_BUFFER_LINES_STR.as_str(), required=false, require_equals=true, num_args=0..=1, value_parser = clap::value_parser!(usize))]
    buffer_fmt: Option<usize>,

    /*
        /// Apply the formatting in place. Works only if an input is provided.
        /// Keep in mind that a new temporary file will be created, and then renamed
        #[arg(short, long, action=ArgAction::SetTrue)]
        in_place: bool,
    */
    /// Save the output to a file
    #[arg(short, long, value_parser=clap::value_parser!(PathBuf))]
    output: Option<PathBuf>,

    #[arg(value_name = "FILE")]
    input: Option<PathBuf>,
}

#[inline]
fn parse_char(s: &str) -> Result<char, &'static str> {
    match s {
        "\\t" => Ok('\t'),
        s if s.chars().count() == 1 => Ok(s.chars().next().unwrap()),
        _ => Err("Delimiter must be a single character"),
    }
}

#[inline(always)]
fn is_comment(record: &csv::ByteRecord, comment_char: u8) -> bool {
    record.get(0).unwrap_or(b"").first() == Some(&comment_char)
}

pub fn strip(fmt_args: FmtArgs) -> Result<()> {
    let comment_char: u8 = fmt_args.comment_char as u8;
    let delimiter: u8 = fmt_args.delimiter as u8;
    let quote_char: u8 = fmt_args.quote_char as u8;

    let in_stream: Box<dyn io::Read> = if let Some(in_path) = fmt_args.input {
        let file_handle = File::open(&in_path)
            .with_context(|| format!("Error in opening input file {:?}", in_path))?;
        Box::new(file_handle)
    } else {
        Box::new(io::stdin())
    };

    // Build the CSV reader and iterate over each record.
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .flexible(true)
        .quote(quote_char)
        .double_quote(true)
        .comment(Some(comment_char))
        // .terminator(Terminator::CRLF)
        .from_reader(in_stream);

    let out_stream: Box<dyn io::Write> = if let Some(file_path) = &fmt_args.output {
        let file_handle = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(file_path)
            .with_context(|| format!("Error in opening output file {:?}", file_path))?;
        Box::new(file_handle)
    } else {
        Box::new(io::stdout().lock())
    };

    let mut wrt = csv::WriterBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .flexible(true)
        .quote(quote_char)
        .quote_style(QuoteStyle::Never)
        .double_quote(true)
        // .terminator(Terminator::CRLF)
        .from_writer(out_stream);

    let mut raw_record: ByteRecord = ByteRecord::new();
    while rdr.read_byte_record(&mut raw_record)? {
        if is_comment(&raw_record, comment_char) {
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

pub fn pad_and_write_unchecked<W, R>(
    wrt: &mut csv::Writer<W>,
    rdr: &mut csv::Reader<R>,
    comment_char: u8,
    cols_width: Vec<usize>,
) -> Result<Vec<usize>>
where
    W: io::Write,
    R: io::Read,
{
    let tmp_spaces = [b' '].repeat(*cols_width.iter().max().unwrap_or(&1));
    let mut tmp_field = Vec::with_capacity(cols_width.iter().sum());
    let mut tmp_byte_record = ByteRecord::with_capacity(cols_width.iter().sum(), cols_width.len());
    let mut raw_record = ByteRecord::new();

    while rdr.read_byte_record(&mut raw_record)? {
        if is_comment(&raw_record, comment_char) {
            wrt.write_byte_record(&raw_record)?;
            continue;
        }

        tmp_byte_record.clear();
        for (col, value) in raw_record.iter().enumerate() {
            assert!(raw_record.len() <= cols_width.len());
            tmp_field.clear();
            tmp_field.extend_from_slice(value);
            tmp_field.extend_from_slice(&tmp_spaces[0..=(cols_width[col] - value.len())]);
            tmp_byte_record.push_field(&tmp_field);
        }
        wrt.write_byte_record(&tmp_byte_record)?
    }

    Ok(cols_width)
}

pub fn pad_and_write_unbuffered<W, R>(
    wrt: &mut csv::Writer<W>,
    rdr: &mut csv::Reader<R>,
    comment_char: u8,
    mut cols_width: Vec<usize>,
) -> Result<Vec<usize>>
where
    W: io::Write,
    R: io::Read,
{
    let mut tmp_spaces = [b' '].repeat(*cols_width.iter().max().unwrap_or(&1));
    let mut tmp_field = Vec::with_capacity(cols_width.iter().sum());
    let mut tmp_byte_record = ByteRecord::with_capacity(cols_width.iter().sum(), cols_width.len());
    let mut raw_record = ByteRecord::new();

    while rdr.read_byte_record(&mut raw_record)? {
        if is_comment(&raw_record, comment_char) {
            wrt.write_byte_record(&raw_record)?;
            continue;
        }

        if raw_record.len() > cols_width.len() {
            cols_width.resize(raw_record.len(), 0);
        }

        tmp_byte_record.clear();
        for (col, value) in raw_record.iter().enumerate() {
            if cols_width[col] < value.len() {
                cols_width[col] = value.len();
                tmp_spaces = [b' '].repeat(tmp_spaces.len().max(value.len()));
            }
            tmp_field.clear();
            tmp_field.extend_from_slice(value);
            tmp_field.extend_from_slice(&tmp_spaces[0..=(cols_width[col] - value.len())]);
            tmp_byte_record.push_field(&tmp_field);
        }
        wrt.write_byte_record(&tmp_byte_record)?;
    }

    Ok(cols_width)
}

pub fn pad_and_write_buffered<W>(
    wrt: &mut csv::Writer<W>,
    buffer: &[ByteRecord],
    comment_char: u8,
) -> Result<Vec<usize>>
where
    W: io::Write,
{
    let mut cols_width: Vec<usize> = vec![0; 1000];

    for record in buffer.iter() {
        if is_comment(record, comment_char) {
            continue;
        }

        if cols_width.len() < record.len() {
            cols_width.resize(record.len(), 0);
        }

        for (col, value) in record.iter().enumerate() {
            if cols_width[col] < value.len() {
                cols_width[col] = value.len()
            }
        }
    }

    let tmp_spaces = [b' '].repeat(*cols_width.iter().max().unwrap_or(&1));
    let mut tmp_field = Vec::with_capacity(cols_width.iter().sum());
    let mut tmp_byte_record = ByteRecord::with_capacity(cols_width.iter().sum(), cols_width.len());
    for record in buffer.iter() {
        if is_comment(record, comment_char) {
            wrt.write_byte_record(record)?;
            continue;
        }

        // for (col, value) in record.iter().enumerate() {
        //     tmp_field.clear();
        //     tmp_field.extend_from_slice(value);
        //     tmp_field.extend_from_slice(&tmp_spaces[0..(cols_width[col] - value.len())]);
        //     wrt.write_field(&tmp_field)?
        // }
        // wrt.write_record(None::<&[u8]>)?;

        tmp_byte_record.clear();
        for (col, value) in record.iter().enumerate() {
            tmp_field.clear();
            tmp_field.extend_from_slice(value);
            tmp_field.extend_from_slice(&tmp_spaces[0..=(cols_width[col] - value.len())]);
            tmp_byte_record.push_field(&tmp_field);
        }
        wrt.write_byte_record(&tmp_byte_record)?;
    }
    Ok(cols_width)
}

pub fn format_file<P: AsRef<Path>>(file_path: P, fmt_args: FmtArgs) -> Result<()> {
    let comment_char: u8 = fmt_args.comment_char as u8;
    let delimiter: u8 = fmt_args.delimiter as u8;
    let quote_char: u8 = fmt_args.quote_char as u8;

    let file_handle = File::open(&file_path)
        .with_context(|| format!("Error in opening input file {:?}", file_path.as_ref()))?;

    let mmap = unsafe {
        Mmap::map(&file_handle).unwrap_or_else(|_| panic!("Error mapping file {}",
            file_path.as_ref().display()))
    };
    let mmap_reader = io::Cursor::new(mmap);
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .flexible(true)
        .quote(quote_char)
        .double_quote(true)
        .comment(Some(comment_char))
        // .terminator(Terminator::CRLF)
        .from_reader(mmap_reader);

    let out_stream: Box<dyn io::Write> = if let Some(file_path) = &fmt_args.output {
        let file_handle = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(file_path)
            .with_context(|| format!("Error in opening output file {:?}", file_path))?;
        Box::new(file_handle)
    } else {
        Box::new(io::stdout().lock())
    };

    let mut wrt = csv::WriterBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .flexible(true)
        .quote(quote_char)
        .quote_style(QuoteStyle::Never)
        .double_quote(true)
        // .terminator(Terminator::CRLF)
        .from_writer(out_stream);

    let mut raw_record = ByteRecord::new();
    if let Some(buffer_lines) = fmt_args.buffer_fmt {
        // Buffer first x lines and check the column width on all of them,
        // then switch to write a line as soon as it's read, without keeping it in memory.
        // The column widths are increased if necessary when a longer field is found, but the
        // process doe not wait for all the file to be read.
        let mut buffer: Vec<ByteRecord> = Vec::with_capacity(buffer_lines);
        let mut line_count = 0;

        while line_count < buffer_lines && rdr.read_byte_record(&mut raw_record)? {
            buffer.push(raw_record.clone());
            line_count += 1;
        }

        let cols_width = pad_and_write_buffered(&mut wrt, &buffer, comment_char)?;
        wrt.flush()?;

        pad_and_write_unbuffered(&mut wrt, &mut rdr, comment_char, cols_width)?;
        wrt.flush()?;
        return Ok(());
    } else {
        // This way instead read the whole file in two-passes.
        // The first computes the column width and the second formats the lines and writes them
        let mut cols_width = vec![0; 1000];
        let start_pos = rdr.position().clone();
        while rdr.read_byte_record(&mut raw_record)? {
            if is_comment(&raw_record, comment_char) {
                continue;
            }

            if cols_width.len() < raw_record.len() {
                cols_width.resize(raw_record.len(), 0);
            }

            for (col, value) in raw_record.iter().enumerate() {
                if cols_width[col] < value.len() {
                    cols_width[col] = value.len()
                }
            }
        }

        rdr.seek(start_pos)?;
        pad_and_write_unchecked(&mut wrt, &mut rdr, comment_char, cols_width)?;
        wrt.flush()?;
    }
    Ok(())
}

pub fn format(fmt_args: FmtArgs) -> Result<()> {
    let comment_char: u8 = fmt_args.comment_char as u8;
    let delimiter: u8 = fmt_args.delimiter as u8;
    let quote_char: u8 = fmt_args.quote_char as u8;

    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .flexible(true)
        .quote(quote_char)
        .double_quote(true)
        .comment(Some(comment_char))
        // .terminator(Terminator::CRLF)
        .from_reader(io::stdin());

    let out_stream: Box<dyn io::Write> = if let Some(file_path) = &fmt_args.output {
        let file_handle = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(file_path)
            .with_context(|| format!("Error in opening output file {:?}", file_path))?;
        Box::new(file_handle)
    } else {
        Box::new(io::stdout().lock())
    };

    let mut wrt = csv::WriterBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .flexible(true)
        .quote(quote_char)
        .quote_style(QuoteStyle::Never)
        .double_quote(true)
        // .terminator(Terminator::CRLF)
        .from_writer(out_stream);

    let mut buffer: Vec<ByteRecord>;
    let mut raw_record = ByteRecord::new();

    if let Some(buffer_lines) = fmt_args.buffer_fmt {
        buffer = Vec::with_capacity(buffer_lines);

        let mut line_count = 0;
        while line_count < buffer_lines && rdr.read_byte_record(&mut raw_record)?
        {
            buffer.push(raw_record.clone());
            line_count += 1;
        }

        let cols_width = pad_and_write_buffered(&mut wrt, &buffer, comment_char)?;
        wrt.flush()?;

        pad_and_write_unbuffered(&mut wrt, &mut rdr, comment_char, cols_width)?;
        wrt.flush()?;
    } else {
        // TODO: Add saving the buffer to a file in case it exceed the memory available
        buffer = Vec::new();
        while rdr.read_byte_record(&mut raw_record)? {
            buffer.push(raw_record.clone())
        }
        pad_and_write_buffered(&mut wrt, &buffer, comment_char)?;
        wrt.flush()?;
    }

    Ok(())
}

pub fn process(fmt_args: FmtArgs) -> anyhow::Result<()> {
    let result = if fmt_args.strip {
        strip(fmt_args)
    } else if let Some(file_path) = &fmt_args.input.clone() {
        format_file(file_path, fmt_args)
    } else {
        format(fmt_args)
    };

    if let Err(err) = result {
        if let Some(csv_err) = err.downcast_ref::<csv::Error>() {
            match csv_err.kind() {
                csv::ErrorKind::Io(err) if err.kind() == io::ErrorKind::BrokenPipe => return Ok(()),
                _ => bail!(err),
            }
        } else {
            bail!(err)
        };
    };

    Ok(())
}
