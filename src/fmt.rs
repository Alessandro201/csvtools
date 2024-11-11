use anyhow::{Context, Result};
use core::panic;
use std::{
    cmp::max,
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::{self, Read},
    path::PathBuf,
};

use csv::{self, ByteRecord, QuoteStyle};

use clap::{ArgAction, Args};

const DEFAULT_BUFFER_LINES: u64 = 1024;

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

    /// N. of lines to keep in memory to strip of format to avoid keeping gigabytes of data in memory.
    #[arg(short, long, default_value_t = DEFAULT_BUFFER_LINES, required=false, value_parser = clap::value_parser!(u64).range(0..(u64::MAX)))]
    buffer_lines: u64,

    /*
        /// Apply the formatting in place. Works only if an input is provided.
        /// Keep in mind that a new temporary file will be created, and then renamed
        #[arg(short, long, action=ArgAction::SetTrue)]
        in_place: bool,
    */
    // TODO: Check the existence of the files
    #[arg(short, long)]
    output: Option<PathBuf>,

    #[arg(value_name = "FILE")]
    input: Option<PathBuf>,
}

fn parse_delimiter(s: &str) -> Result<char, &'static str> {
    match s {
        "\\t" => Ok('\t'),
        s if s.chars().count() == 1 => Ok(s.chars().next().unwrap()),
        _ => Err("Delimiter must be a single character"),
    }
}

pub fn strip<R: io::Read, W: io::Write>(in_stream: R, out_stream: W) -> Result<()> {
    // Build the CSV reader and iterate over each record.
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b',')
        .has_headers(false)
        .flexible(true)
        // .comment(Some(b'#'))
        .trim(csv::Trim::All)
        .from_reader(in_stream);

    let mut wrt = csv::WriterBuilder::new()
        .delimiter(b',')
        .flexible(true)
        .from_writer(out_stream);

    let mut raw_record: ByteRecord = ByteRecord::new();
    while rdr.read_byte_record(&mut raw_record)? {
        wrt.write_byte_record(&raw_record)?;
    }
    wrt.flush()?;
    Ok(())
}

pub fn pad_and_write<W>(
    wrt: &mut csv::Writer<W>,
    buffer: &[ByteRecord],
    comment_char: u8,
) -> Result<()>
where
    W: io::Write,
{
    let mut cols_width: Vec<usize> = vec![0; 100];
    let mut byte_record_buffer: ByteRecord;
    let mut lines_commented: VecDeque<usize> = VecDeque::new();

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

        // byte_record_buffer = record
        //     .iter()
        //     .enumerate()
        //     .map(|(col, value)| {
        //         // Pad each field with spaces as bytes, and truncate if it exceeds pad_width
        //         let mut padded_field = Vec::from(value);
        //         padded_field.resize(cols_width[col], b' ');
        //         padded_field
        //     })
        //     .collect();
        //
        // wrt.write_byte_record(&byte_record_buffer)?;

        for (col, value) in record.iter().enumerate() {
            wrt.write_field([value, &tmp_spaces[0..(cols_width[col] - value.len())]].concat())
                .context("unable to write field")?;
        }
        wrt.write_record(None::<&[u8]>)?;
    }
    Ok(())
}

pub fn format<R: io::Read, W: io::Write>(
    in_stream: R,
    out_stream: W,
    fmt_args: FmtArgs,
) -> Result<()> {
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

    let mut buffer: Vec<ByteRecord> = Vec::with_capacity(fmt_args.buffer_lines as usize);
    let mut line_count = 0;

    // println!("Inside format");

    if fmt_args.buffer_lines == 0 || fmt_args.input.is_some() {
        buffer = rdr.into_byte_records().filter_map(|r| r.ok()).collect();
        pad_and_write(&mut wrt, &buffer, comment_char)?;
    } else {
        let mut raw_record = csv::ByteRecord::new();
        while rdr
            .read_byte_record(&mut raw_record)
            .context("Encountered error in parsing CSV file")?
        {
            // operations.....
            if line_count < fmt_args.buffer_lines {
                buffer.push(raw_record.clone());
                line_count += 1;
            } else {
                pad_and_write(&mut wrt, &buffer, comment_char)?;
                wrt.flush()?;
                buffer.clear();
                line_count = 0;
            }
        }
        pad_and_write(&mut wrt, &buffer, comment_char)?;
        wrt.flush()?;
    }

    wrt.flush()?;
    Ok(())
}

pub fn process(fmt_args: FmtArgs) -> Result<()> {
    // println!("fmt::process received this args: {:#?}", fmt_args);

    let in_stream: Box<dyn io::Read> = if let Some(file_path) = &fmt_args.input {
        // println!("Reading from file {:?}", &file_path);
        Box::new(File::open(file_path)?)
    } else {
        // println!("Reading from stdin");
        Box::new(io::stdin())
    };

    let out_stream: Box<dyn io::Write> = if let Some(file_path) = &fmt_args.output {
        // println!("Writing to file {:?}", &file_path);
        Box::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(file_path)?,
        )
    } else {
        // println!("Writing to stdout");
        Box::new(io::stdout().lock())
    };

    if fmt_args.strip {
        // println!("Stripping...\n\n");
        strip(in_stream, out_stream)?
    } else {
        // println!("Formatting...\n\n");
        format(in_stream, out_stream, fmt_args)?
    }

    Ok(())
}
