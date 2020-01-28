// Dataflow description:
//
//  +----------------------------+              +-------------------------------+
//  | match.na.2020-02-26.csv.gz |              | summoner.na.2020-02-26.csv.gz |
//  +-------------+--------------+              +---------------+---------------+
//                |                                             |
//                V                                             |
//        +-------+-------+                                     V
//        |    matchId    |                           +---------+----------+     +--------------------------+
//        | bitset filter |                           | summoner selection |     | apex tier leagueId fetch |
//        +-------+-------+                           +---------+----------+     +-------------+------------+
//                |                                             |                              |
//                |                +--------------<-------------+----->------+---------<-------+
//                |                |                                         |
//                |                V                                         V
//                |     +----------+-----------+                      +------+------+
//                |     | recent matchId fetch +---+                  | rank  fetch |
//                |     +----------+-----------+   |                  +------+------+
//                |                |               |                         |
//                |     +-----<----+               +-------------->----------+
//                |     |                                                    |
//                V     V                                                    V
//       +--------+-----+--------+                           +---------------+---------------+
//       | filter seen  matchIds |                           |        append / add to        |
//       +-----------+-----------+                           | summoner.na.2020-02-26.csv.gz |
//                   |                                       +-------------------------------+
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

#[macro_use]
extern crate lazy_static;

mod util;
mod model;
mod pipeline;

use std::vec::Vec;
use std::collections::BinaryHeap;
use std::error::Error;
use std::path::PathBuf;

use futures::future::join_all;
use riven::RiotApi;
use riven::consts::Region;
use riven::consts::QueueType;
use riven::consts::Tier;
use riven::consts::Division;

use model::summoner::{ Summoner, SummonerOldest };
use util::csv_find;
use util::csvgz;

pub fn run() -> Result<(), Box<dyn Error>> {
    println!("Hello OwOrld.");

    let region = Region::NA;
    let update: usize = 100;

    let summoner_path = csv_find::find_latest_csvgz(region, "summoner")
        .ok_or("Failed to find summoner csv.gz.")?;
    println!("{:?}", summoner_path);
    let mut summoner_reader = csvgz::reader(summoner_path)?;
    let summoner_reader = summoner_reader
        .deserialize()
        .map(|summoner_res| SummonerOldest(summoner_res.expect("ERR")));


    let heap = filter_min_n(update, summoner_reader);

    for (i, summoner) in heap.into_iter().enumerate() {
        let summoner = summoner.0;
        println!("{}: {}", i, summoner.encrypted_summoner_id);
    }

    println!("Done.");
    Ok(())
}

// use std::cmp::Reverse;
use std::iter::IntoIterator;
pub fn filter_min_n<I, T>(limit: usize, iter: I) -> BinaryHeap<T> where
    I: IntoIterator<Item = T>,
    T: Ord,
{
    let mut heap = BinaryHeap::with_capacity(limit);

    for item in iter {
        if heap.len() < limit {
            heap.push(item);
        }
        else if *heap.peek().unwrap() > item {
            heap.pop();
            heap.push(item);
        }
    }

    heap
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_filter_min_n() {
        let values: [i32; 10] = [ 5, 2, -10, 12, 4, 15, -15, 0, -10, -1 ];
        let min_values = filter_min_n(5, &values);
        println!("{:?}", min_values.into_iter().collect::<Vec<_>>());
    }
}

pub fn main() {
    run().unwrap();
}




pub fn main2() {
    println!("Hello, world!~");

    let region = Region::NA;
    let queue_type = QueueType::RANKED_SOLO_5x5;

    // Create RiotApi instance from key string.
    let api_key = include_str!("apikey.txt");
    let riot_api = RiotApi::with_key(api_key);

    let path_match_out: PathBuf = [
        "data",
        &format!("{:?}", Region::NA).to_lowercase(),
        &format!("summoner.{}.csv.gz", util::time::datetimestamp()),
    ].iter().collect();
    
    {
        println!("{:?}", path_match_out);
        let mut match_entries_out = util::csvgz::writer(path_match_out).expect("Failed to write xd.");

        let mut rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {

            for tier in [
                Tier::CHALLENGER, Tier::GRANDMASTER, Tier::GRANDMASTER,
                Tier::DIAMOND, Tier::PLATINUM, Tier::GOLD,
                Tier::SILVER, Tier::BRONZE, Tier::IRON,
            ].iter() {
                for division in [ Division::I, Division::II, Division::III, Division::IV ].iter() {
                    println!("Starting {} {}.", tier, division);

                    let mut page: i32 = 1;

                    'batchloop: loop {
                        // Batches of 10 pages.
                        let mut league_batch = Vec::new();

                        for _ in 0..10 {
                            league_batch.push(
                                riot_api.league_exp_v4().get_league_entries(region, queue_type, *tier, *division, Some(page)));
                            page += 1;
                        }

                        let league_batch = join_all(league_batch).await;
                        let ts = util::time::epoch_millis();
                        for league in league_batch.into_iter() {
                            let league_entries = league.unwrap_or(Vec::new()); // Error goes away here (may be bad).
                            if 0 == league_entries.len() {
                                break 'batchloop;
                            }
                            for league_entry in league_entries {
                                match_entries_out.serialize(Summoner {
                                    encrypted_summoner_id: league_entry.summoner_id,
                                    encrypted_account_id: None,
                                    league_id: league_entry.league_id,
                                    rank_tier: league_entry.tier,
                                    games_per_day: None,
                                    ts: Some(ts),
                                }).expect("failed to serialize");
                            }
                        }
                    }
                }
            }
        });

        // match_entries_out.serialize(Summoner {
        //     encrypted_summoner_id: "asdf,,".to_owned(),
        //     encrypted_account_id:  "asdf".to_owned(),
        //     league_id: "123-123-123-asdf".to_owned(),
        //     rank_tier: Tier::GOLD,
        //     games_per_day: None,
        //     ts: Some(util::time::epoch_millis()),
        // }).unwrap();

        match_entries_out.flush().unwrap();
    }
}