// Dataflow description:
//
//  +----------------------------+              +-------------------------------+
//  | match.na.2020-02-26.csv.gz |              | summoner.na.2020-02-26.csv.gz |
//  +-------------+--------------+              +---------------+---------------+
//                |                                             |
//                V                                             |
//        +-------+-------+                                     V
//        |    matchId    |                           +---------+----------+ 
//        | bitset filter |                           | summoner selection |
//        +-------+-------+                           +---------+----------+
//                |                                             |
//                |                +--------------<-------------+
//                |                |                            |
//                |                V                            V
//                |     +----------+-----------+         +------+------+
//                |     | recent matchId fetch +---+     | rank  fetch |
//                |     +----------+-----------+   |     +------+------+
//                |                |               |            |
//                |     +-----<----+               +------>-----+
//                |     |                                       |
//                V     V                                       V
//       +--------+-----+--------+              +---------------+---------------+
//       | filter seen  matchIds |              |        append / add to        |
//       +-----------+-----------+              | summoner.na.2020-02-26.csv.gz |
//                   |                          +-------------------------------+
//                   V
//       +-----------+-----------+           +-------------------+
//       | streaming match fetch |           | stats files .CSVs |
//       +-----------+-----------+           +---------+---------+
//                   |                                 |
//                   +----------------->---------------+
//                   |                                 |
//                   V                                 V
//     +-------------+--------------+      +-----------+-------------+
//     | match.na.2020-02-26.csv.gz |      | streaming stats updates |
//     +----------------------------+      +-------------------------+
//
//
//

mod util;
mod model;

// use util::hybitset::HyBitSet;
use model::SummonerModel;

use rand::Rng;

//use std::collections::HashMap;
use fnv::FnvHashMap;

const SPAN: usize = 3_651_972_316_usize - 3_617_178_774_usize;

// pub fn main3() {

//     let mut map: FnvHashMap<usize, u32> = FnvHashMap::default(); // HashMap::new();

//     let mut rng = rand::thread_rng();
//     for _ in 0..(SPAN / 6) {
//         let val = rng.gen_range(3_617_178_774_usize, 3_651_972_316_usize);
//         map.insert(val, 1234_u32);
//         // println!("Contains {}? {}.", val, had);
//     }
//     println!("Size: {}.", map.len());
// }

// pub fn main2() {
//     println!("Hello world!");

//     let mut bs = HyBitSet::new();
//     // bs.insert(3_617_178_774_usize);
//     // bs.insert(3_651_972_316_usize);
    

//     println!("Span: {}.", SPAN);

//     // println!("Contains: {}.", bs.contains(3_651_972_316_usize));
//     // println!("Contains: {}.", bs.contains(0_usize));

//     let mut rng = rand::thread_rng();
//     for _ in 0..(SPAN / 6) {
//         let val = rng.gen_range(3_617_178_774_usize, 3_651_972_316_usize);
//         let _had = bs.insert(val);
//         // println!("Contains {}? {}.", val, had);
//     }
// }

use std::error::Error;
use std::io;
use std::io::prelude::*;
use std::fs::File;
use std::process;

use serde::Serialize;

use flate2::Compression;
use flate2::write::GzEncoder;

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct Record<'a> {
    city: &'a str,
    state: &'a str,
    population: Option<u64>,
    latitude: f64,
    longitude: f64,
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut file = File::create("foo.csv.gz")?;
    let mut encoder = GzEncoder::new(file, Compression::default());
    let mut wtr = csv::Writer::from_writer(encoder);

    wtr.serialize(Record {
        city: "Davidsons Landing",
        state: "AK",
        population: None,
        latitude: 65.2419444,
        longitude: -165.2716667,
    })?;
    wtr.serialize(Record {
        city: "Kenai",
        state: "AK",
        population: Some(7610),
        latitude: 60.5544444,
        longitude: -151.2583333,
    })?;
    wtr.serialize(Record {
        city: "Oakman",
        state: "AL",
        population: None,
        latitude: 33.7133333,
        longitude: -87.3886111,
    })?;

    wtr.flush()?;
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        println!("{}", err);
        process::exit(1);
    }
}

