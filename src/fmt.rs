use core::panic;
use lazy_static::lazy_static;
use std::{
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::{self},
    path::PathBuf,
    process::exit,
};

use csv::{self, ByteRecord, QuoteStyle};

use clap::{ArgAction, Args};

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

    /// Lines starting with this character will be ingored
    #[arg(short, long, default_value_t = '#', required = false)]
    comment_char: char,

    /// Delimiter to use to parse the tabular data
    #[arg(short, long, default_value_t = '\t', required = false, value_parser=parse_delimiter)]
    delimiter: char,

    /// Hybrid buffering. Buffer the first 1024 lines to get the max width per column, then format
    /// line by line incrementing the max width when a bigger colums is found
    #[arg(short, long, action=ArgAction::SetTrue)]
    // TODO: Implement this mode
    hybrid_buffer: bool,

    /// N. of lines to strip or format per chunk to avoid keeping gigabytes of data in memory.
    #[arg(short, long, default_missing_value = DEFAULT_BUFFER_LINES_STR.as_str(), required=false, require_equals=true, num_args=0..=1, value_parser = clap::value_parser!(usize))]
    buffer_lines: Option<usize>,

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
fn parse_delimiter(s: &str) -> Result<char, &'static str> {
    match s {
        "\\t" => Ok('\t'),
        s if s.chars().count() == 1 => Ok(s.chars().next().unwrap()),
        _ => Err("Delimiter must be a single character"),
    }
}

#[derive(Debug)]
pub enum MyErrors {
    CsvReadError(csv::Error),
    CsvWriteError(csv::Error),
    IoWriteError(io::Error),
}

pub fn strip<R: io::Read, W: io::Write>(in_stream: R, out_stream: W) -> Result<(), MyErrors> {
    // Build the CSV reader and iterate over each record.
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b',')
        .has_headers(false)
        .flexible(true)
        // .comment(Some(b'#'))
        .from_reader(in_stream);

    let mut wrt = csv::WriterBuilder::new()
        .delimiter(b',')
        .flexible(true)
        .from_writer(out_stream);

    let mut raw_record: ByteRecord = ByteRecord::new();

    while rdr
        .read_byte_record(&mut raw_record)
        .map_err(MyErrors::CsvReadError)?
    {
        for f in raw_record.iter() {
            wrt.write_field(f.trim_ascii())
                .map_err(MyErrors::CsvReadError)?
        }

        wrt.write_record(None::<&[u8]>)
            .map_err(MyErrors::CsvReadError)?
    }
    wrt.flush().map_err(MyErrors::IoWriteError)?;

    Ok(())
}

pub fn pad_and_write_unbuffered<W, R>(
    wrt: &mut csv::Writer<W>,
    rdr: &mut csv::Reader<R>,
    comment_char: u8,
    mut cols_width: Vec<usize>,
) -> Result<Vec<usize>, MyErrors>
where
    W: io::Write,
    R: io::Read,
{
    // let mut byte_record_buffer: ByteRecord;

    let tmp_spaces = [b' '].repeat(*cols_width.iter().max().unwrap_or(&1));
    let mut tmp_field = Vec::with_capacity(cols_width.iter().sum());
    let mut tmp_byte_record = ByteRecord::with_capacity(cols_width.iter().sum(), cols_width.len());
    let mut raw_record = ByteRecord::new();

    while rdr
        .read_byte_record(&mut raw_record)
        .map_err(MyErrors::CsvReadError)?
    {
        if raw_record
            .get(0)
            .unwrap_or(b"")
            .trim_ascii_start()
            .iter()
            .next()
            == Some(&comment_char)
        {
            wrt.write_byte_record(&raw_record)
                .map_err(MyErrors::CsvWriteError)?;
            continue;
        }

        if cols_width.len() < raw_record.len() {
            let multiplier = raw_record.len().div_ceil(cols_width.len());
            cols_width.resize(cols_width.len() * multiplier, 0);
        }

        tmp_byte_record.clear();
        for (col, value) in raw_record.iter().enumerate() {
            if cols_width[col] < value.len() {
                cols_width[col] = value.len()
            }
            tmp_field.clear();
            tmp_field.extend_from_slice(value);
            tmp_field.extend_from_slice(&tmp_spaces[0..=(cols_width[col] - value.len())]);
            tmp_byte_record.push_field(&tmp_field);
        }
        wrt.write_byte_record(&tmp_byte_record)
            .map_err(MyErrors::CsvWriteError)?;
    }

    Ok(cols_width)
}

pub fn pad_and_write_buffered<W>(
    wrt: &mut csv::Writer<W>,
    buffer: &[ByteRecord],
    comment_char: u8,
) -> Result<Vec<usize>, csv::Error>
where
    W: io::Write,
{
    let mut cols_width: Vec<usize> = vec![0; 100];
    let mut lines_commented: VecDeque<usize> = VecDeque::new();
    // let mut byte_record_buffer: ByteRecord;

    for (line, record) in buffer.iter().enumerate() {
        if record
            .get(0)
            .unwrap_or(b"")
            .trim_ascii_start()
            .iter()
            .next()
            == Some(&comment_char)
        {
            lines_commented.push_back(line);
            continue;
        }

        if cols_width.len() < record.len() {
            let multiplier = record.len().div_ceil(cols_width.len());
            cols_width.resize(cols_width.len() * multiplier, 0);
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
    for (line, record) in buffer.iter().enumerate() {
        if let Some(l) = lines_commented.front() {
            match l.cmp(&line) {
                std::cmp::Ordering::Less => panic!("This should not happen. A line inside fmt::pad_and_write::lines_commented was smaller than record"),
                std::cmp::Ordering::Equal => {
                    wrt.write_byte_record(record)?;
                    lines_commented.pop_front();
                    continue;
                },
                std::cmp::Ordering::Greater => {},
            }
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

pub fn format<R: io::Read, W: io::Write>(
    in_stream: R,
    out_stream: W,
    fmt_args: FmtArgs,
) -> Result<(), MyErrors> {
    let comment_char: u8 = fmt_args.comment_char as u8;
    let delimiter: u8 = fmt_args.delimiter as u8;

    // Build the CSV reader and iterate over each record.
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .flexible(true)
        .quote(b'"')
        .double_quote(true)
        // .terminator(Terminator::CRLF)
        .from_reader(in_stream);

    let mut wrt = csv::WriterBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .flexible(true)
        .quote(b'"')
        .quote_style(QuoteStyle::Never)
        .double_quote(true)
        // .terminator(Terminator::CRLF)
        .from_writer(out_stream);

    let mut buffer: Vec<ByteRecord>;
    let mut line_count = 0;
    let mut raw_record = ByteRecord::new();

    if fmt_args.hybrid_buffer {
        let buffer_lines = fmt_args.buffer_lines.unwrap_or(DEFAULT_BUFFER_LINES);
        buffer = Vec::with_capacity(buffer_lines);

        while rdr
            .read_byte_record(&mut raw_record)
            .map_err(MyErrors::CsvReadError)?
        {
            if line_count < buffer_lines {
                buffer.push(raw_record.clone());
                line_count += 1;
            } else {
                break;
            }
        }

        let cols_width = pad_and_write_buffered(&mut wrt, &buffer, comment_char)
            .map_err(MyErrors::CsvWriteError)?;
        wrt.flush().map_err(MyErrors::IoWriteError)?;

        pad_and_write_unbuffered(&mut wrt, &mut rdr, comment_char, cols_width)?;
    } else if fmt_args.buffer_lines.is_none_or(|b| b == 0) {
        buffer = Vec::new();
        while rdr
            .read_byte_record(&mut raw_record)
            .map_err(MyErrors::CsvReadError)?
        {
            buffer.push(raw_record.clone())
        }
        pad_and_write_buffered(&mut wrt, &buffer, comment_char).map_err(MyErrors::CsvWriteError)?;
    } else {
        let buffer_lines = fmt_args
            .buffer_lines
            .expect("Buffer lines is None, but it should have been already checked to be Some");
        buffer = Vec::with_capacity(buffer_lines);

        while rdr
            .read_byte_record(&mut raw_record)
            .map_err(MyErrors::CsvReadError)?
        {
            if line_count < buffer_lines {
                buffer.push(raw_record.clone());
                line_count += 1;
            } else {
                pad_and_write_buffered(&mut wrt, &buffer, comment_char)
                    .map_err(MyErrors::CsvWriteError)?;
                wrt.flush().map_err(MyErrors::IoWriteError)?;
                buffer.clear();
                line_count = 0;
            }
        }
        pad_and_write_buffered(&mut wrt, &buffer, comment_char).map_err(MyErrors::CsvWriteError)?;
        wrt.flush().map_err(MyErrors::IoWriteError)?;
    }

    wrt.flush().map_err(MyErrors::IoWriteError)?;
    Ok(())
}

pub fn process(fmt_args: FmtArgs) {
    // println!("fmt::process received this args: {:#?}", fmt_args);

    let in_stream: Box<dyn io::Read>;
    if let Some(file_path) = &fmt_args.input {
        // println!("Reading from file {:?}", &file_path);
        let file_handle = match File::open(file_path) {
            Ok(handle) => handle,
            Err(err) => {
                eprintln!("Error in reading file {:?}: {:?}", file_path, err);
                exit(1);
            }
        };
        in_stream = Box::new(file_handle)
    } else {
        // println!("Reading from stdin");
        in_stream = Box::new(io::stdin())
    };

    let out_stream: Box<dyn io::Write>;
    if let Some(file_path) = &fmt_args.output {
        // println!("Writing to file {:?}", &file_path);
        let file_handle = match OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(file_path)
        {
            Ok(handle) => handle,
            Err(err) => {
                eprintln!("Error in opening output file {:?}: {:?}", file_path, err);
                exit(1)
            }
        };
        out_stream = Box::new(file_handle)
    } else {
        // println!("Writing to stdout");
        out_stream = Box::new(io::stdout().lock())
    };

    let result = if fmt_args.strip {
        strip(in_stream, out_stream)
    } else {
        format(in_stream, out_stream, fmt_args)
    };

    if let Err(err) = result {
        match err {
            MyErrors::CsvReadError(err) => {
                eprintln!("Error in reading the data: {}", err);
                exit(1)
            }
            MyErrors::CsvWriteError(err) => match err.kind() {
                csv::ErrorKind::Io(err) if err.kind() == io::ErrorKind::BrokenPipe => exit(0),
                _ => {
                    eprintln!("Error in writing the data: {}", err);
                    exit(1)
                }
            },
            MyErrors::IoWriteError(err) => {
                eprintln!("Error in writing the data: {}", err);
                exit(1)
            }
        }
    }
}
