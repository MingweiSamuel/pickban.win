use std::collections::HashMap;
use std::path::{ Path, PathBuf };

use riven::consts::Tier;

use crate::util::csvgz;
use crate::util::file_find;
use crate::util::time;
use crate::model::r#match::Match;
use crate::model::summoner::{ Summoner, SummonerOldest, SummonerHighestRanked };
use crate::model::league::League;
use super::filter;


// pub fn get_match_hybitset(path: impl AsRef<Path>, starttime: DateTime<Utc>) -> HyBitSet {
//     let mut hbs = HyBitSet::new();

//     for path in file_find::find_after_datetime(path, "match", "tar.gz", starttime) {
//         let mut match_reader = csvgz::reader(path).expect("Failed to read.");
//         for mat in match_reader.deserialize() {
//             let mat: Match = mat.expect("Failed to deserialize match.");
//             hbs.insert(mat.match_id as usize);
//         }
//     }

//     hbs
// }

#[allow(dead_code)]
pub fn get_all_summoners(path: impl AsRef<Path>)
    -> std::io::Result<Option<impl Iterator<Item = Summoner>>>
{
    let summoner_path = file_find::find_latest(path, "summoner", "csv.gz")
        .expect("Failed to find latest csvgz");
    match summoner_path {
        None => Ok(None),
        Some(summoner_path) => {
            let summoner_reader = csvgz::reader(summoner_path)?
                .into_deserialize()
                .map(|summoner_res| summoner_res.expect("Failed to parse summoner."));
            Ok(Some(summoner_reader))
        },
    }
}

#[allow(dead_code)]
pub fn get_oldest_summoners(path: impl AsRef<Path>, update_size: usize)
    -> std::io::Result<Option<impl Iterator<Item = Summoner>>>
{
    let summoner_reader = get_all_summoners(path)?;

    Ok(summoner_reader.map(|summoner_reader| {
        let summoner_reader = summoner_reader.map(SummonerOldest);

        let oldest_summoners = filter::filter_min_n(update_size, summoner_reader);
        oldest_summoners.into_iter().map(|s| s.0)
    }))
}

pub fn get_ranked_summoners(path: impl AsRef<Path>)
    -> std::io::Result<HashMap<String, (Tier, String)>>
{
    let mut out = HashMap::with_capacity(65_536);

    if let Some(summoners) = get_all_summoners(path)? {
        for summoner in summoners {
            if let Some(tier) = summoner.rank_tier {
                let league_id = summoner.league_id.expect("Summoner with tier but no league id.");
                out.insert(summoner.encrypted_summoner_id,
                    (tier, league_id));
            }
        }
    }

    Ok(out)
}

pub fn write_leagues(path: impl AsRef<Path>, leagues: impl Iterator<Item = League>) -> std::io::Result<()> {
    let path = path.as_ref().join(format!("leagueIds.csv.gz"));

    let mut writer = csvgz::writer(path).expect("Failed to write leagueIds file.");
    for league in leagues {
        writer.serialize(league)?;
    }
    writer.flush()?;

    Ok(())
}

pub fn write_summoners(path: impl AsRef<Path>, summoners: impl Iterator<Item = Summoner>) -> std::io::Result<()> {
    
    let path_match_out = path.as_ref().join(format!("summoner.{}.csv.gz", time::datetimestamp()));
    // PathBuf = [
    //     "data",
    //     &format!("{:?}", region).to_lowercase(),
    //     &format!("summoner.{}.csv.gz", time::datetimestamp()),
    // ].iter().collect();

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

pub fn write_matches<'a, I: Iterator<Item = &'a Match>>(
    dir: &PathBuf, iso_week_str: &str, matches: I) -> std::io::Result<()>
{
    let mut path = dir.clone();
    path.push(format!("matches.{}.csv.gz", iso_week_str));
    let mut writer = csvgz::writer_or_appender(&path)
        .unwrap_or_else(|e| panic!("Failed to make match writer: {:?}, {}", &path, e));
    for matche in matches {
        writer.serialize(matche)?;
    }
    writer.flush()?;

    Ok(())
}
