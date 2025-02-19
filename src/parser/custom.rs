use std::io::{self, BufRead};

#[derive(Debug, Copy, Clone)]
pub struct Position {
    line: u64,
    chr: u64,
    index: u64,
}

pub struct Field<'a> {
    pub pos: Position,
    pub text: &'a [u8],
}

pub struct ReaderBuilder {
    pub pos: Position,
    pub delimiter: char,
    pub has_headers: bool,
    pub comment_char: char,
}

impl Default for ReaderBuilder {
    fn default() -> ReaderBuilder {
        ReaderBuilder {
            pos: Position {
                line: 0,
                chr: 0,
                index: 0,
            },
            delimiter: ',',
            has_headers: true,
            comment_char: '#',
        }
    }
}

pub struct Reader<R> {
    builder: ReaderBuilder,
    rdr: io::BufReader<R>,
    buf: Vec<u8>,
}

impl<R: io::Read> Reader<R> {
    pub fn from(rdr: R) -> Reader<R> {
        let builder = ReaderBuilder::default();
        Reader {
            builder,
            rdr: io::BufReader::new(rdr),
            buf: Vec::new(),
        }
    }

    pub fn read_record(&mut self) {
        for c in self.rdr.read_until(b'\t', &mut self.buf) {}
    }

    fn next_line_position() -> u8 {
        0
    }
}
