#[macro_use] extern crate lazy_static;
#[macro_use] extern crate tokio;
// #[macro_use] extern crate scan_fmt;

mod util;
mod model;
mod pipeline;

use std::vec::Vec;
use std::error::Error;
use std::path::PathBuf;
use std::sync::Mutex;
use std::collections::HashSet;

use chrono::{ DateTime, Duration };
use chrono::offset::Utc;
use futures::future::join_all;
use riven::RiotApi;
use riven::consts::Region;
use riven::consts::Queue;
use riven::consts::QueueType;
use riven::consts::Tier;
use tokio::task;

use model::summoner::{ Summoner, SummonerOldest };
use util::file_find;
use util::csvgz;
use pipeline::filter;
use pipeline::source;

async fn main_async() -> Result<(), Box<dyn Error>> {
    println!("Hello world");

    let region = Region::NA;
    // let update_size: usize = 500;
    // let lookbehind = Duration::hours(4);
    // let starttime = Utc::now() - lookbehind;

    let summoner_path = file_find::find_latest(region, "summoner", "csv.gz")
        .expect("Error finding latest csvgz.")
        .expect("No csvgz found.");
    let mut summoner_reader = csvgz::reader(summoner_path)?;
    let league_ids = summoner_reader
        .deserialize()
        .filter_map(|summoner| summoner.ok())
        .map(|summoner: Summoner| summoner.league_id)
        .collect::<HashSet<String>>();

    println!("League IDs length: {}.", league_ids.len());
    Ok(())
}

pub fn main() {
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(main_async())
        .unwrap_or_else(|e| panic!("Failed to complete: {}", e));
}
