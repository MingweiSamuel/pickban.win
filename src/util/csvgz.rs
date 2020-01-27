use std::fs::File;
use std::io::Error;
use std::path::Path;

use flate2::Compression;
use flate2::write::GzEncoder;
use flate2::read::GzDecoder;

#[allow(dead_code)]
pub fn writer<P: AsRef<Path>>(path: P) -> Result<csv::Writer<GzEncoder<File>>, Error> {
    let file    = File::create(path)?;
    let encoder = GzEncoder::new(file, Compression::default());
    let writer  = csv::Writer::from_writer(encoder);
    Ok(writer)
}

#[allow(dead_code)]
pub fn reader<P: AsRef<Path>>(path: P) -> Result<csv::Reader<GzDecoder<File>>, Error> {
    let file    = File::create(path)?;
    let decoder = GzDecoder::new(file);
    let reader  = csv::Reader::from_reader(decoder);
    Ok(reader)
}
