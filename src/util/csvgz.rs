use std::error::Error;
use std::io;
use std::io::prelude::*;
use std::fs::File;
use std::path::Path;
use std::process;

use flate2::Compression;
use flate2::write::GzEncoder;
use flate2::read::GzDecoder;

pub fn csvgz_writer<P: AsRef<Path>>(path: P) -> Result<csv::Writer<GzEncoder<File>>, Box<dyn Error>> {
    let file    = File::create(path)?;
    let encoder = GzEncoder::new(file, Compression::default());
    let writer  = csv::Writer::from_writer(encoder);
    Ok(writer)
}

pub fn csvgz_reader<P: AsRef<Path>>(path: P) -> Result<csv::Reader<GzDecoder<File>>, Box<dyn Error>> {
    let file    = File::create(path)?;
    let decoder = GzDecoder::new(file);
    let reader  = csv::Reader::from_reader(decoder);
    Ok(reader)
}
