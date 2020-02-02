use std::path::PathBuf;

use chrono::DateTime;
use chrono::offset::Utc;
use riven::consts::Region;

use crate::util::hybitset::HyBitSet;
use crate::util::csvgz;
use crate::util::file_find;
use crate::util::time;
use crate::model::r#match::Match;
use crate::model::summoner::{ Summoner, SummonerOldest, SummonerHighestRanked };
use super::filter;

#[allow(dead_code)]
pub fn get_match_hybitset(region: Region, starttime: DateTime<Utc>) -> HyBitSet {
    let mut hbs = HyBitSet::new();

    for path in file_find::find_after_datetime(region, "match", "tar.gz", starttime) {
        let mut match_reader = csvgz::reader(path).expect("Failed to read.");
        for mat in match_reader.deserialize() {
            let mat: Match = mat.expect("Failed to deserialize match.");
            hbs.insert(mat.match_id as usize);
        }
    }

    hbs
}

#[allow(dead_code)]
pub fn get_all_summoners(region: Region) -> Result<impl Iterator<Item = Summoner>, std::io::Error> {
    let summoner_path = file_find::find_latest(region, "summoner", "csv.gz")
        .expect("Failed to find latest csvgz");
    let summoner_reader = csvgz::reader(summoner_path)?
        .into_deserialize()
        .map(|summoner_res| summoner_res.expect("Failed to parse summoner."));
    Ok(summoner_reader)
}

#[allow(dead_code)]
pub fn get_oldest_summoners(region: Region, update_size: usize) -> std::io::Result<impl Iterator<Item = Summoner>> {
    let summoner_reader = get_all_summoners(region)?
        .map(SummonerOldest);

    let oldest_summoners = filter::filter_min_n(update_size, summoner_reader);
    Ok(oldest_summoners.into_iter().map(|s| s.0))
}

pub fn write_summoners(region: Region, summoners: impl Iterator<Item = Summoner>) -> std::io::Result<()> {
    
    let path_match_out: PathBuf = [
        "data",
        &format!("{:?}", region).to_lowercase(),
        &format!("summoner.{}.csv.gz", time::datetimestamp()),
    ].iter().collect();

    let mut writer = csvgz::writer(path_match_out).expect("Failed to write xd.");

    // Do a bit of one-pass sorting to keep higher ranks first, improves compression.
    {
        use std::collections::BinaryHeap;
        const HEAP_SIZE: usize = 1024;
        let mut heap = BinaryHeap::with_capacity(HEAP_SIZE);
    
        for summoner in summoners {
            let summoner = SummonerHighestRanked(summoner);
            if heap.peek().map(|best_summoner| &summoner >= best_summoner).unwrap_or(false) {
                writer.serialize(summoner.0)?;
                continue;
            };
            heap.push(summoner);
            if HEAP_SIZE <= heap.len() { // If heap full empty one.
                writer.serialize(heap.pop().unwrap().0)?;
            };
        };
        while let Some(summoner) = heap.pop() {
            writer.serialize(summoner.0)?;
        }
    }
    writer.flush()?;

    Ok(())
}
