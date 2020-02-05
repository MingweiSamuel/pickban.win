use std::fs::{ File, OpenOptions };
use std::path::Path;

use flate2::Compression;
use flate2::write::GzEncoder;
use flate2::read::GzDecoder;


#[allow(dead_code)]
pub fn reader<P: AsRef<Path>>(path: P) -> std::io::Result<csv::Reader<GzDecoder<File>>> {
    let file    = File::open(path)?;
    let decoder = GzDecoder::new(file);
    let reader  = csv::Reader::from_reader(decoder);
    Ok(reader)
}

#[allow(dead_code)]
pub fn writer_or_appender<P: AsRef<Path>>(path: P) -> std::io::Result<csv::Writer<GzEncoder<File>>> {
    if path.as_ref().exists() {
        appender(path)
    }
    else {
        writer(path)
    }
}

pub fn writer<P: AsRef<Path>>(path: P) -> std::io::Result<csv::Writer<GzEncoder<File>>> {
    let file    = File::create(path)?;
    let encoder = GzEncoder::new(file, Compression::default());
    let writer  = csv::Writer::from_writer(encoder);
    Ok(writer)
}

pub fn appender<P: AsRef<Path>>(path: P) -> std::io::Result<csv::Writer<GzEncoder<File>>> {
    let file    = OpenOptions::new().write(true).append(true).open(path)?;
    let encoder = GzEncoder::new(file, Compression::default());
    let writer  = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(encoder);
    Ok(writer)
}
