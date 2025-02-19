use anyhow::{bail, Context, Result};
use clap::{ArgAction, Args};
use csv::{self, ByteRecord, QuoteStyle};
use lazy_static::lazy_static;
use log::debug;
use memmap::Mmap;
use rand::{distributions::Alphanumeric, Rng};
use std::{
    fs::{self, File, OpenOptions},
    io::{self},
    path::{Path, PathBuf},
};

const DEFAULT_DELIMITER: char = '\t';
const DEFAULT_BUFFER_LINES: usize = 1024;
lazy_static! {
    static ref DEFAULT_BUFFER_LINES_STR: String = DEFAULT_BUFFER_LINES.to_string();
}

#[derive(Debug, Clone, Args)]
#[command(flatten_help = true)]
pub struct FmtArgs {
    /// Strip extra spaces around the values instead of padding them
    #[arg(short, long, action=ArgAction::SetTrue)]
    strip: bool,

    /// Delimiter to use to parse the tabular data. Default=\t for any file other than .csv
    #[arg(short, long, required = false, value_parser=parse_char)]
    delimiter: Option<char>,

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

    /// Apply the formatting in place. Works only if an input is provided.
    /// Keep in mind that a new temporary file will be created, and then renamed.
    #[arg(short, long, action=ArgAction::SetTrue)]
    in_place: bool,

    /// Needed to properly format UTF-8 data. The default is to treat the data as just bytes
    /// because it's much faster
    #[arg(short, long, action=ArgAction::SetTrue)]
    utf8: bool,

    // Save the output to a file
    #[arg(short, long, value_parser=clap::value_parser!(PathBuf))]
    output: Option<PathBuf>,

    #[arg(value_name = "FILE")]
    input: Option<PathBuf>,
}

impl FmtArgs {
    fn check_args(&self) -> anyhow::Result<()> {
        if self.output.is_some() && self.in_place {
            bail!("You cannot pass both --output and --in-place");
        }
        if self.in_place && self.input.is_none() {
            bail!("You cannot pass --in-place flag if the input is not a file");
        }
        Ok(())
    }
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

pub fn strip(fmt_args: &FmtArgs) -> Result<()> {
    let comment_char: u8 = fmt_args.comment_char as u8;
    let delimiter: u8 = fmt_args.delimiter.unwrap_or(DEFAULT_DELIMITER) as u8;
    let quote_char: u8 = fmt_args.quote_char as u8;

    let in_stream: Box<dyn io::Read> = if let Some(in_path) = fmt_args.input.clone() {
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
        .comment(None)
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
        .quote_style(QuoteStyle::Necessary)
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
        // Skip empty lines or filled with only spaces
        if raw_record.len() <= 1 && raw_record.get(1).unwrap_or(b"").trim_ascii().is_empty() {
            continue;
        }

        tmp_byte_record.clear();
        for (col, field) in raw_record
            .iter()
            .map(|field| field.trim_ascii_end())
            .enumerate()
        {
            assert!(raw_record.len() <= cols_width.len());
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

        // Skip empty lines or filled with only spaces
        if raw_record.len() <= 1 && raw_record.get(1).unwrap_or(b"").trim_ascii().is_empty() {
            continue;
        }

        if raw_record.len() > cols_width.len() {
            cols_width.resize(raw_record.len(), 0);
        }

        tmp_byte_record.clear();
        for (col, field) in raw_record
            .iter()
            .map(|field| field.trim_ascii_end())
            .enumerate()
        {
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

        // Each field is trimmed and added 1 for the the space at the end
        for (col, field_width) in record
            .iter()
            .map(|field| field.trim_ascii_end().len() + 1)
            .enumerate()
        {
            if cols_width[col] < field_width {
                cols_width[col] = field_width
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

        // Skip empty lines or filled with only spaces
        if record.len() <= 1 && record.get(1).unwrap_or(b"").trim_ascii().is_empty() {
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
        for (col, field) in record
            .iter()
            .map(|field| field.trim_ascii_end())
            .enumerate()
        {
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

pub fn format_file<P: AsRef<Path>>(file_path: P, fmt_args: &FmtArgs) -> Result<()> {
    let comment_char: u8 = fmt_args.comment_char as u8;
    let quote_char: u8 = fmt_args.quote_char as u8;
    let delimiter: u8;

    if fmt_args.delimiter.is_some() {
        delimiter = fmt_args.delimiter.unwrap() as u8;
    } else if file_path.as_ref().extension().is_some_and(|e| e == "csv") {
        delimiter = ',' as u8;
    } else {
        delimiter = DEFAULT_DELIMITER as u8;
    }

    let file_handle = File::open(&file_path)
        .with_context(|| format!("Error in opening input file {:?}", file_path.as_ref()))?;

    let mmap = unsafe {
        Mmap::map(&file_handle)
            .unwrap_or_else(|_| panic!("Error mapping file {}", file_path.as_ref().display()))
    };
    let mmap_reader = io::Cursor::new(mmap);
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .flexible(true)
        .quote(quote_char)
        .double_quote(true)
        .comment(None)
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
        .quote_style(QuoteStyle::Necessary)
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
        pad_and_write_unchecked(&mut wrt, &mut rdr, comment_char, cols_width)?;
        wrt.flush()?;
    }
    Ok(())
}

pub fn format<R: io::Read, W: io::Write>(
    fmt_args: &FmtArgs,
    in_stream: &mut R,
    out_stream: &mut W,
) -> Result<()> {
    let comment_char: u8 = fmt_args.comment_char as u8;
    let quote_char: u8 = fmt_args.quote_char as u8;
    let delimiter: u8 = fmt_args.delimiter.unwrap_or(DEFAULT_DELIMITER) as u8;

    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .flexible(true)
        .quote(quote_char)
        .double_quote(true)
        .comment(None)
        // .terminator(Terminator::CRLF)
        .from_reader(in_stream);

    let mut wrt = csv::WriterBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .flexible(true)
        .quote(quote_char)
        .quote_style(QuoteStyle::Necessary)
        .double_quote(true)
        // .terminator(Terminator::CRLF)
        .from_writer(out_stream);

    let mut buffer: Vec<ByteRecord> = Vec::with_capacity(DEFAULT_BUFFER_LINES);

    if let Some(buffer_lines) = fmt_args.buffer_fmt {
        for (line_count, record) in rdr.byte_records().enumerate() {
            if line_count > buffer_lines {
                break;
            }
            buffer.push(record?);
        }

        let cols_width = pad_and_write_buffered(&mut wrt, &buffer, comment_char)?;
        wrt.flush()?;

        pad_and_write_unbuffered(&mut wrt, &mut rdr, comment_char, cols_width)?;
        wrt.flush()?;
    } else {
        // TODO: Add saving the buffer to a file in case it exceed the memory available
        for record in rdr.byte_records() {
            let record = record?;
            buffer.push(record);
        }
        pad_and_write_buffered(&mut wrt, &buffer, comment_char)?;
        wrt.flush()?;
    }

    Ok(())
}

pub fn process(fmt_args: FmtArgs) -> anyhow::Result<()> {
    fmt_args.check_args()?;
    debug!("{:#?}", &fmt_args);

    let output_file: Option<PathBuf> = if fmt_args.output.is_some() {
        fmt_args.output.clone()
    } else if fmt_args.input.is_some() && fmt_args.in_place {
        let mut random_name: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect();
        let mut output_file = fmt_args
            .input
            .as_ref()
            .unwrap()
            .parent()
            .unwrap()
            .join(&random_name);

        while output_file.exists() {
            random_name.push('x');
            output_file.set_file_name(&random_name);
        }
        Some(output_file)
    } else {
        None
    };

    let mut out_stream: Box<dyn io::Write> = if let Some(ref output_file) = output_file {
        let file_handle = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(output_file)
            .with_context(|| format!("Error in opening output file {:?}", output_file))?;
        Box::new(file_handle)
    } else {
        Box::new(io::stdout().lock())
    };

    let result = if fmt_args.strip {
        strip(&fmt_args)
    } else if let Some(file_path) = &fmt_args.input.clone() {
        format_file(file_path, &fmt_args)
    } else {
        let mut in_stream = io::stdin();
        format(&fmt_args, &mut in_stream, &mut out_stream)
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

    if fmt_args.input.is_some() && fmt_args.in_place {
        let res = fs::rename(
            output_file.expect("Output_file should have been set before"),
            fmt_args.input.clone().unwrap(),
        );
        if let Err(err) = res {
            bail!(err)
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use anyhow::Result;

    use super::*;

    #[test]
    fn check_arguments() -> Result<()> {
        let mut fmt_args = FmtArgs {
            strip: false,
            in_place: true,
            delimiter: Some(','),
            comment_char: '#',
            quote_char: '"',
            buffer_fmt: None,
            output: None,
            input: None,
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

        Ok(())
    }

    fn run_format(fmt_args: FmtArgs, in_stream: &[u8]) -> Result<Vec<u8>> {
        let mut out_stream: Vec<u8> = vec![];
        let mut in_stream = in_stream;
        format(&fmt_args, &mut in_stream, &mut out_stream)?;
        Ok(out_stream)
    }

    #[test]
    fn parse_comments() -> Result<()> {
        let fmt_args = FmtArgs {
            strip: false,
            in_place: false,
            delimiter: Some(','),
            comment_char: '#',
            quote_char: '"',
            buffer_fmt: None,
            output: None,
            input: None,
        };
        let in_stream: &[u8] = br#"

# Comment1
# Comment2   
"#;

        let correct_out_stream: &[u8] = br#"# Comment1
# Comment2   
"#;

        let out_stream = run_format(fmt_args, in_stream)?;
        if out_stream != correct_out_stream {
            println!("in_stream: \n{}", String::from_utf8_lossy(in_stream));
            println!("out_stream: \n{}", String::from_utf8_lossy(&out_stream));
            println!(
                "correct_out_stream: \n{}",
                String::from_utf8_lossy(correct_out_stream)
            );
        }
        assert_eq!(out_stream, correct_out_stream);
        Ok(())
    }

    #[test]
    fn parse_extra_fields() -> Result<()> {
        let fmt_args = FmtArgs {
            strip: false,
            in_place: false,
            delimiter: Some(','),
            comment_char: '#',
            quote_char: '"',
            buffer_fmt: None,
            output: None,
            input: None,
        };
        let in_stream: &[u8] = br#"
ciao1,wow
ciao1 tutti,wow
ciao2,gatto,extra field
"#;

        let correct_out_stream: &[u8] = br#"ciao1       ,wow
ciao1 tutti ,wow
ciao2       ,gatto ,extra field
"#;

        let out_stream = run_format(fmt_args, in_stream)?;
        if out_stream != correct_out_stream {
            println!("in_stream: \n{}", String::from_utf8_lossy(in_stream));
            println!("out_stream: \n{}", String::from_utf8_lossy(&out_stream));
            println!(
                "correct_out_stream: \n{}",
                String::from_utf8_lossy(correct_out_stream)
            );
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
        let fmt_args = FmtArgs {
            strip: false,
            in_place: false,
            delimiter: Some(','),
            comment_char: '#',
            quote_char: '"',
            buffer_fmt: None,
            output: None,
            input: None,
        };
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

        let out_stream = run_format(fmt_args, in_stream)?;
        if out_stream != correct_out_stream {
            println!("in_stream: \n{}", String::from_utf8_lossy(in_stream));
            println!("out_stream: \n{}", String::from_utf8_lossy(&out_stream));
            println!(
                "correct_out_stream: \n{}",
                String::from_utf8_lossy(correct_out_stream)
            );
        }
        assert_eq!(out_stream, correct_out_stream);
        Ok(())
    }

    #[test]
    fn parse_quotes() -> Result<()> {
        let fmt_args = FmtArgs {
            strip: false,
            in_place: false,
            delimiter: Some(','),
            comment_char: '#',
            quote_char: '"',
            buffer_fmt: None,
            output: None,
            input: None,
        };
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

        let out_stream = run_format(fmt_args, in_stream)?;
        if out_stream != correct_out_stream {
            println!("in_stream: \n{}", String::from_utf8_lossy(in_stream));
            println!("out_stream: \n{}", String::from_utf8_lossy(&out_stream));
            println!(
                "correct_out_stream: \n{}",
                String::from_utf8_lossy(correct_out_stream)
            );
        }
        assert_eq!(out_stream, correct_out_stream);
        Ok(())
    }

    #[test]
    fn parse_correctly() -> Result<()> {
        let fmt_args = FmtArgs {
            strip: false,
            in_place: false,
            delimiter: Some(','),
            comment_char: '#',
            quote_char: '"',
            buffer_fmt: None,
            output: None,
            input: None,
        };
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

        let out_stream = run_format(fmt_args, in_stream)?;
        if out_stream != correct_out_stream {
            println!("in_stream: \n{}", String::from_utf8_lossy(in_stream));
            println!("out_stream: \n{}", String::from_utf8_lossy(&out_stream));
            println!(
                "correct_out_stream: \n{}",
                String::from_utf8_lossy(correct_out_stream)
            );
        }
        assert_eq!(out_stream, correct_out_stream);
        Ok(())
    }
}
