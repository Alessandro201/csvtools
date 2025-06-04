use crate::fmt::cli::FmtArgs;
use crate::fmt::DEFAULT_BUFFER_LINES;
use crate::fmt::DEFAULT_DELIMITER;

use anyhow::bail;
use anyhow::{Context, Result};
use csv::{self, ByteRecord, QuoteStyle, StringRecord, Terminator};
use memmap::Mmap;
use rand::{distributions::Alphanumeric, Rng};
use std::{
    fs::{self, File, OpenOptions},
    io::{self},
    path::{Path, PathBuf},
    process::exit,
};

#[derive(Debug)]
struct CsvFormatter {
    delimiter: char,
    comment_char: Option<char>,
    quote_char: char,
    quote_style: QuoteStyle,
    double_quote: bool,
    flexible: bool,
    utf8: bool,
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
            terminator: Terminator::CRLF,
            buffer: None,
        }
    }
}

impl CsvFormatter {
    pub fn new() -> Self {
        CsvFormatter::default()
    }
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
    pub fn from_args(fmt_args: &FmtArgs) -> Self {
        Self::default()
            .set_delimiter(fmt_args.delimiter.unwrap_or(DEFAULT_DELIMITER))
            .set_comment_char({
                if fmt_args.no_skip_comments {
                    None
                } else {
                    Some(fmt_args.comment_char)
                }
            })
            .set_flexible(fmt_args.flexible)
            .set_utf8(fmt_args.utf8)
            .set_quote_char(fmt_args.quote_char)
    }
}

impl CsvFormatter {
    fn _set_csv_reader<'r, R: io::Read>(&self, in_stream: &'r mut R) -> csv::Reader<&'r mut R> {
        csv::ReaderBuilder::new()
            .delimiter(self.delimiter as u8)
            .has_headers(false)
            .flexible(self.flexible)
            .quote(self.quote_char as u8)
            .double_quote(self.double_quote)
            .comment(None)
            // .terminator(self.terminator)
            .from_reader(in_stream)
    }

    fn _set_csv_writer<'w, W: io::Write>(&self, out_stream: &'w mut W) -> csv::Writer<&'w mut W> {
        csv::WriterBuilder::new()
            .delimiter(self.delimiter as u8)
            .has_headers(false)
            .flexible(self.flexible)
            .quote(self.quote_char as u8)
            .double_quote(self.double_quote)
            .quote_style(self.quote_style)
            .comment(None)
            // .terminator(self.terminator)
            .from_writer(out_stream)
    }

    pub fn format<R, W>(&self, in_stream: &mut R, out_stream: &mut W) -> Result<()>
    where
        R: io::Read,
        W: io::Write,
    {
        let rdr = self._set_csv_reader(in_stream);
        let wrt = self._set_csv_writer(out_stream);

        if self.utf8 {
            self._format(rdr, wrt)?
        } else {
            self._format_byte(rdr, wrt)?
        }

        Ok(())
    }

    fn _format_byte<R: io::Read, W: io::Write>(
        &self,
        mut rdr: csv::Reader<&mut R>,
        mut wrt: csv::Writer<&mut W>,
    ) -> Result<()> {
        let mut buffer: Vec<ByteRecord> = Vec::with_capacity(DEFAULT_BUFFER_LINES);
        if let Some(buffer_lines) = self.buffer {
            for (line_n, record) in rdr.byte_records().enumerate() {
                if line_n > buffer_lines {
                    break;
                }
                buffer.push(record?);
            }

            let cols_width = pad_and_write_buffered_byte(&mut wrt, &buffer, self.comment_char.map(|c| c as u8))?;
            wrt.flush()?;

            pad_and_write_unbuffered_byte(&mut wrt, &mut rdr, self.comment_char.map(|c| c as u8), cols_width)?;
            wrt.flush()?;
        } else {
            // TODO: Add saving the buffer to a file in case it exceed the memory available
            for record in rdr.byte_records() {
                let record = record?;
                buffer.push(record);
            }
            pad_and_write_buffered_byte(&mut wrt, &buffer, self.comment_char.map(|c| c as u8))?;
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
                if line_n > buffer_lines {
                    break;
                }
                buffer.push(record?);
            }

            let cols_width = pad_and_write_buffered(&mut wrt, &buffer, self.comment_char)?;
            wrt.flush()?;

            pad_and_write_unbuffered(&mut wrt, &mut rdr, self.comment_char, cols_width)?;
            wrt.flush()?;
        } else {
            // TODO: Add saving the buffer to a file in case it exceed the memory available
            for record in rdr.records() {
                let record = record?;
                buffer.push(record);
            }
            pad_and_write_buffered(&mut wrt, &buffer, self.comment_char)?;
            wrt.flush()?;
        }

        Ok(())
    }

    pub fn strip<R, W>(&self, in_stream: &mut R, out_stream: &mut W) -> Result<()>
    where
        R: io::Read,
        W: io::Write,
    {
        let rdr = self._set_csv_reader(in_stream);
        let wrt = self._set_csv_writer(out_stream);

        if self.utf8 {
            self._strip(rdr, wrt)?
        } else {
            self._strip_bytes(rdr, wrt)?
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

    pub fn format_file<'w, P, W>(self, file: P, out_stream: &'w mut W) -> Result<()>
    where
        P: AsRef<Path>,
        W: io::Write,
    {
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

// Get the delimiter either from the FmtArgs, from the file extension or the default
// DEFAULT_DELIMITER
fn get_delimiter(fmt_args: &FmtArgs) -> char {
    if let Some(delimiter) = fmt_args.delimiter {
        delimiter
    } else if fmt_args
        .input
        .as_ref()
        .is_some_and(|path| path.extension().is_some_and(|ext| ext == "csv"))
    {
        ','
    } else if fmt_args
        .input
        .as_ref()
        .is_some_and(|path| path.extension().is_some_and(|ext| ext == "tsv"))
    {
        '\t'
    } else {
        DEFAULT_DELIMITER
    }
}

pub fn pad_and_write_unchecked<W, R>(
    wrt: &mut csv::Writer<W>,
    rdr: &mut csv::Reader<R>,
    comment_char: char,
    cols_width: Vec<usize>,
) -> Result<Vec<usize>>
where
    W: io::Write,
    R: io::Read,
{
    let tmp_spaces = " ".repeat(*cols_width.iter().max().unwrap_or(&1));
    let mut tmp_field = String::with_capacity(cols_width.iter().sum());
    let mut tmp_record = StringRecord::with_capacity(cols_width.iter().sum(), cols_width.len());
    let mut record = StringRecord::new();

    while rdr.read_record(&mut record)? {
        if is_comment(&record, comment_char) {
            wrt.write_record(&record)?;
            continue;
        }
        // Skip empty lines or filled with only spaces
        if record.len() <= 1 && record.get(0).unwrap_or("").trim().is_empty() {
            continue;
        }

        tmp_record.clear();
        for (col, field) in record.iter().map(|field| field.trim_end()).enumerate() {
            let field_width = field.chars().count();
            debug_assert!(record.len() <= cols_width.len());
            debug_assert!(field_width < cols_width[col]);

            tmp_field.clear();
            tmp_field.push_str(field);
            if col != record.len() - 1 {
                tmp_field.push_str(&tmp_spaces[0..(cols_width[col] - field_width)]);
            }
            tmp_record.push_field(&tmp_field);
        }
        wrt.write_record(&tmp_record)?
    }

    Ok(cols_width)
}

pub fn pad_and_write_unbuffered<W, R>(
    wrt: &mut csv::Writer<W>,
    rdr: &mut csv::Reader<R>,
    comment_char: Option<char>,
    mut cols_width: Vec<usize>,
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
            continue;
        }

        if record.len() > cols_width.len() {
            cols_width.resize(record.len(), 0);
        }

        tmp_record.clear();
        for (col, field) in record.iter().map(|field| field.trim_end()).enumerate() {
            // Trimmed and added 1 for the the space at the end
            let field_width = field.chars().count();
            if cols_width[col] < field_width + 1 {
                cols_width[col] = field_width + 1;
                tmp_spaces = " ".repeat(tmp_spaces.len().max(field_width));
            }
            tmp_field.clear();
            tmp_field.push_str(field);
            // if the field is the last, just add a space at the end
            if col != record.len() - 1 {
                tmp_field.push_str(&tmp_spaces[0..(cols_width[col] - field_width)]);
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
) -> Result<Vec<usize>>
where
    W: io::Write,
{
    let mut cols_width: Vec<usize> = vec![0; 1000];

    for record in buffer.iter() {
        if comment_char.is_some_and(|c| is_comment(record, c)) {
            continue;
        }

        if cols_width.len() < record.len() {
            cols_width.resize(record.len(), 0);
        }

        // Each field is trimmed and added 1 for the the space at the end
        for (col, field_width) in record
            .iter()
            .map(|field| field.trim_end().chars().count() + 1)
            .enumerate()
        {
            if cols_width[col] < field_width {
                cols_width[col] = field_width
            }
        }
    }

    let tmp_spaces = " ".repeat(*cols_width.iter().max().unwrap_or(&1));
    let mut tmp_field = String::with_capacity(cols_width.iter().sum());
    let mut tmp_record = StringRecord::with_capacity(cols_width.iter().sum(), cols_width.len());
    for record in buffer.iter() {
        if comment_char.is_some_and(|c| is_comment(record, c)) {
            wrt.write_record(record)?;
            continue;
        }

        // Skip empty lines or filled with only spaces
        if record.len() <= 1 && record.get(0).unwrap_or("").trim().is_empty() {
            continue;
        }

        // for (col, value) in record.iter().enumerate() {
        //     tmp_field.clear();
        //     tmp_field.extend_from_slice(value);
        //     tmp_field.extend_from_slice(&tmp_spaces[0..(cols_width[col] - value.len())]);
        //     wrt.write_field(&tmp_field)?
        // }
        // wrt.write_record(None::<&[u8]>)?;

        tmp_record.clear();
        for (col, field) in record.iter().map(|field| field.trim_end()).enumerate() {
            tmp_field.clear();
            tmp_field.push_str(field);
            // if the field is the last, just add a space at the end
            if col != record.len() - 1 {
                tmp_field.push_str(&tmp_spaces[0..(cols_width[col] - field.chars().count())]);
            }
            tmp_record.push_field(&tmp_field);
        }
        wrt.write_record(&tmp_record)?;
    }
    Ok(cols_width)
}

pub fn pad_and_write_unchecked_byte<W, R>(
    wrt: &mut csv::Writer<W>,
    rdr: &mut csv::Reader<R>,
    comment_char: Option<u8>,
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
        if comment_char.is_some_and(|c| is_comment_byte(&raw_record, c)) {
            wrt.write_byte_record(&raw_record)?;
            continue;
        }
        // Skip empty lines or filled with only spaces
        if raw_record.len() <= 1 && raw_record.get(0).unwrap_or(b"").trim_ascii().is_empty() {
            continue;
        }

        tmp_byte_record.clear();
        for (col, field) in raw_record.iter().map(|field| field.trim_ascii_end()).enumerate() {
            debug_assert!(raw_record.len() <= cols_width.len());
            tmp_field.clear();
            tmp_field.extend_from_slice(field);
            if col != raw_record.len() - 1 {
                tmp_field.extend_from_slice(&tmp_spaces[0..(cols_width[col] - field.len())]);
            }
            tmp_byte_record.push_field(&tmp_field);
        }
        wrt.write_byte_record(&tmp_byte_record)?
    }

    Ok(cols_width)
}

pub fn pad_and_write_unbuffered_byte<W, R>(
    wrt: &mut csv::Writer<W>,
    rdr: &mut csv::Reader<R>,
    comment_char: Option<u8>,
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
        if comment_char.is_some_and(|c| is_comment_byte(&raw_record, c)) {
            wrt.write_byte_record(&raw_record)?;
            continue;
        }

        // Skip empty lines or filled with only spaces
        if raw_record.len() <= 1 && raw_record.get(0).unwrap_or(b"").trim_ascii().is_empty() {
            continue;
        }

        if raw_record.len() > cols_width.len() {
            cols_width.resize(raw_record.len(), 0);
        }

        tmp_byte_record.clear();
        for (col, field) in raw_record.iter().map(|field| field.trim_ascii_end()).enumerate() {
            // Trimmed and added 1 for the the space at the end
            let field_width = field.len();
            if cols_width[col] < field_width + 1 {
                cols_width[col] = field_width + 1;
                tmp_spaces = [b' '].repeat(tmp_spaces.len().max(field_width));
            }
            tmp_field.clear();
            tmp_field.extend_from_slice(field);
            // if the field is the last, just add a space at the end
            if col != raw_record.len() - 1 {
                tmp_field.extend_from_slice(&tmp_spaces[0..(cols_width[col] - field.len())]);
            }
            tmp_byte_record.push_field(&tmp_field);
        }
        wrt.write_byte_record(&tmp_byte_record)?;
    }

    Ok(cols_width)
}

pub fn pad_and_write_buffered_byte<W>(
    wrt: &mut csv::Writer<W>,
    buffer: &[ByteRecord],
    comment_char: Option<u8>,
) -> Result<Vec<usize>>
where
    W: io::Write,
{
    let mut cols_width: Vec<usize> = vec![0; 1000];

    for record in buffer.iter() {
        if comment_char.is_some_and(|c| is_comment_byte(record, c)) {
            continue;
        }

        if cols_width.len() < record.len() {
            cols_width.resize(record.len(), 0);
        }

        // Each field is trimmed and added 1 for the the space at the end
        for (col, field_width) in record.iter().map(|field| field.trim_ascii_end().len() + 1).enumerate() {
            if cols_width[col] < field_width {
                cols_width[col] = field_width
            }
        }
    }

    let tmp_spaces = [b' '].repeat(*cols_width.iter().max().unwrap_or(&1));
    let mut tmp_field = Vec::with_capacity(cols_width.iter().sum());
    let mut tmp_byte_record = ByteRecord::with_capacity(cols_width.iter().sum(), cols_width.len());
    for record in buffer.iter() {
        if comment_char.is_some_and(|c| is_comment_byte(record, c)) {
            wrt.write_byte_record(record)?;
            continue;
        }

        // Skip empty lines or filled with only spaces
        if record.len() <= 1 && record.get(0).unwrap_or(b"").trim_ascii().is_empty() {
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
        for (col, field) in record.iter().map(|field| field.trim_ascii_end()).enumerate() {
            tmp_field.clear();
            tmp_field.extend_from_slice(field);
            // if the field is the last, just add a space at the end
            if col != record.len() - 1 {
                tmp_field.extend_from_slice(&tmp_spaces[0..(cols_width[col] - field.len())]);
            }
            tmp_byte_record.push_field(&tmp_field);
        }
        wrt.write_byte_record(&tmp_byte_record)?;
    }
    Ok(cols_width)
}

pub fn format_file<P: AsRef<Path>, W: io::Write>(fmt_args: &FmtArgs, file_path: P, out_stream: &mut W) -> Result<()> {
    let comment_char = fmt_args.comment_char as u8;
    let quote_char = fmt_args.quote_char as u8;
    let delimiter = fmt_args.delimiter.unwrap_or(DEFAULT_DELIMITER) as u8;

    let file_handle =
        File::open(&file_path).context(format!("Error in opening input file {:?}", file_path.as_ref()))?;

    let reader = io::BufReader::new(file_handle);
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .flexible(true)
        .quote(quote_char)
        .double_quote(true)
        .comment(None)
        // .terminator(Terminator::CRLF)
        .from_reader(reader);

    let mut wrt = csv::WriterBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .flexible(true)
        .quote(quote_char)
        .quote_style(QuoteStyle::Necessary)
        .double_quote(true)
        // .terminator(Terminator::CRLF)
        .from_writer(out_stream);

    if fmt_args.utf8 {
        let mut raw_record = StringRecord::new();
        if let Some(buffer_lines) = fmt_args.buffer_fmt {
            // Buffer first x lines and check the column width on all of them,
            // then switch to write a line as soon as it's read, without keeping it in memory.
            // The column widths are increased if necessary when a longer field is found, but the
            // process doe not wait for all the file to be read.
            let mut buffer: Vec<StringRecord> = Vec::with_capacity(buffer_lines);
            let mut line_n = 0;

            while line_n < buffer_lines && rdr.read_record(&mut raw_record)? {
                buffer.push(raw_record.clone());
                line_n += 1;
            }

            let cols_width = pad_and_write_buffered(&mut wrt, &buffer, Some(comment_char as char))?;
            wrt.flush()?;

            pad_and_write_unbuffered(&mut wrt, &mut rdr, Some(comment_char as char), cols_width)?;
            wrt.flush()?;
            return Ok(());
        } else {
            // This way instead read the whole file in two-passes.
            // The first computes the column width and the second formats the lines and writes them
            let mut cols_width = vec![0; 1000];
            let start_pos = rdr.position().clone();
            while rdr.read_record(&mut raw_record)? {
                if is_comment(&raw_record, comment_char as char) {
                    continue;
                }

                if cols_width.len() < raw_record.len() {
                    cols_width.resize(raw_record.len(), 0);
                }

                // Trimmed and added 1 for the the space at the end
                for (col, field_width) in raw_record.iter().map(|field| field.trim_end().len() + 1).enumerate() {
                    if cols_width[col] < field_width {
                        cols_width[col] = field_width
                    }
                }
            }

            rdr.seek(start_pos)?;
            pad_and_write_unchecked(&mut wrt, &mut rdr, comment_char as char, cols_width)?;
            wrt.flush()?;
        }
    } else {
        let mut raw_record = ByteRecord::new();
        if let Some(buffer_lines) = fmt_args.buffer_fmt {
            // Buffer first x lines and check the column width on all of them,
            // then switch to write a line as soon as it's read, without keeping it in memory.
            // The column widths are increased if necessary when a longer field is found, but the
            // process doe not wait for all the file to be read.
            let mut buffer: Vec<ByteRecord> = Vec::with_capacity(buffer_lines);
            let mut line_n = 0;

            while line_n < buffer_lines && rdr.read_byte_record(&mut raw_record)? {
                buffer.push(raw_record.clone());
                line_n += 1;
            }

            let cols_width = pad_and_write_buffered_byte(&mut wrt, &buffer, Some(comment_char))?;
            wrt.flush()?;

            pad_and_write_unbuffered_byte(&mut wrt, &mut rdr, Some(comment_char), cols_width)?;
            wrt.flush()?;
            return Ok(());
        } else {
            // This way instead read the whole file in two-passes.
            // The first computes the column width and the second formats the lines and writes them
            let mut cols_width = vec![0; 1000];
            let start_pos = rdr.position().clone();
            while rdr.read_byte_record(&mut raw_record)? {
                if is_comment_byte(&raw_record, comment_char) {
                    continue;
                }

                if cols_width.len() < raw_record.len() {
                    cols_width.resize(raw_record.len(), 0);
                }

                // Trimmed and added 1 for the the space at the end
                for (col, field_width) in raw_record
                    .iter()
                    .map(|field| field.trim_ascii_end().len() + 1)
                    .enumerate()
                {
                    if cols_width[col] < field_width {
                        cols_width[col] = field_width
                    }
                }
            }

            rdr.seek(start_pos)?;
            pad_and_write_unchecked_byte(&mut wrt, &mut rdr, Some(comment_char), cols_width)?;
            wrt.flush()?;
        }
    }

    Ok(())
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
        Box::new(io::stdout().lock())
    };
    Ok(out_stream)
}

pub fn run(fmt_args: FmtArgs) -> anyhow::Result<()> {
    fmt_args.check_args()?;

    let in_file: Option<PathBuf> = fmt_args.input.clone();
    let mut in_stream: Box<dyn io::Read> = get_in_stream(in_file.as_ref())?;
    let out_file: Option<PathBuf> = get_output_dest(&fmt_args);
    let mut out_stream: Box<dyn io::Write> = get_out_stream(out_file.as_ref())?;

    if fmt_args.in_place {
        // The arguments should have already been checked
        assert!(fmt_args.output.is_none());
        assert!(fmt_args.input.is_some());

        // There should be a file to write to that will then be renamed as the original file
        assert!(out_file.is_some());
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

    let delimiter = get_delimiter(&fmt_args);
    let quote_style = match fmt_args.quote_style.as_str() {
        "necessary" => QuoteStyle::Necessary,
        "always" => QuoteStyle::Always,
        "never" => QuoteStyle::Never,
        "non-numeric" => QuoteStyle::NonNumeric,
        _ => bail!("This --quote-style option is not available"),
    };

    let formatter = CsvFormatter::new()
        .set_delimiter(delimiter)
        .set_flexible(fmt_args.flexible)
        .set_quote_char(fmt_args.quote_char)
        .set_quote_style(quote_style)
        .set_comment_char(Some(fmt_args.comment_char));

    if fmt_args.strip {
        formatter.strip(&mut in_stream, &mut out_stream)?
    } else if let Some(in_file) = in_file {
        formatter.format_file(in_file, &mut out_stream)?
    } else {
        formatter.format(&mut in_stream, &mut out_stream)?
    };

    // if fmt_args.strip {
    //     strip(&fmt_args, &mut in_stream, &mut out_stream)?
    // } else if let Some(in_file) = in_file {
    //     format_file(&fmt_args, in_file, &mut out_stream)?
    // } else {
    //     format(&fmt_args, &mut in_stream, &mut out_stream)?
    // };

    // In case the --in-place flag was given overwrite the original file with the temporary one
    if fmt_args.in_place {
        let in_file = &fmt_args
            .input
            .expect("Program logic error: Input should have been given with the --in-place flag.");
        let out_file = out_file.expect("Program logic error: Output file should have already been specified.");
        fs::rename(&out_file, in_file)
            .context(format!(
                "Unable to overwrite the original file with the temporary formatted file. \nTmp file: {:?} -X-> input file: {:?}",
                out_file,
                in_file)
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
