use chrono::DateTime;
use chrono::offset::Utc;
use riven::consts::Region;

use crate::util::hybitset::HyBitSet;
use crate::util::csvgz;
use crate::util::csv_find;
use crate::model::r#match::Match;
use crate::model::summoner::{ Summoner, SummonerOldest };
use super::filter;

#[allow(dead_code)]
pub fn get_match_hybitset(region: Region, starttime: DateTime<Utc>) -> HyBitSet {
    let mut hbs = HyBitSet::new();

    for path in csv_find::find_after_datetime(region, "match", starttime) {
        let mut match_reader = csvgz::reader(path).expect("Failed to read.");
        for mat in match_reader.deserialize() {
            let mat: Match = mat.expect("Failed to deserialize match.");
            hbs.insert(mat.match_id as usize);
        }
    }

    hbs
}

#[allow(dead_code)]
pub fn get_oldest_summoners(region: Region, update_size: usize) -> Result<impl Iterator<Item = Summoner>, std::io::Error> {
    let summoner_path = csv_find::find_latest_csvgz(region, "summoner").expect("Failed to find latest csvgz");
    let mut summoner_reader = csvgz::reader(summoner_path)?;
    let summoner_reader = summoner_reader
        .deserialize()
        .map(|summoner_res| SummonerOldest(summoner_res.expect("Failed to parse summoner")));

    let oldest_summoners = filter::filter_min_n(update_size, summoner_reader);
    Ok(oldest_summoners.into_iter().map(|s| s.0))
}
