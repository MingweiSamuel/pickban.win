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

#[macro_use] extern crate lazy_static;
#[macro_use] extern crate tokio;
// #[macro_use] extern crate scan_fmt;

mod util;
mod model;
mod pipeline;

use std::vec::Vec;
use std::error::Error;
use std::path::PathBuf;

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
use util::csv_find;
use util::csvgz;
use pipeline::filter;
use pipeline::source;

async fn main_async() -> Result<(), Box<dyn Error>> {
    println!("Hello world");

    let region = Region::NA;
    let update_size: usize = 100;
    let lookbehind = Duration::days(3);
    let starttime = Utc::now() - lookbehind;

    // Unlike normal futures, this starts immediately (it seems).
    // Match bitset.
    let match_hbs = task::spawn_blocking(
        move || source::get_match_hybitset(region, starttime));
    // Oldest (or selected) summoners, for updating.
    let oldest_summoners = task::spawn_blocking(
        move || source::get_oldest_summoners(region, update_size));

    let (match_hbs, oldest_summoners) = tokio::try_join!(match_hbs, oldest_summoners)?;
    let oldest_summoners: Vec<Summoner> = oldest_summoners?.collect();

    for (i, summoner) in oldest_summoners.iter().enumerate() {
        let summoner = summoner;
        println!("{}: {}", i, summoner.encrypted_summoner_id);
    }

    println!("Done.");
    Ok(())
}

// pub fn run() -> Result<(), Box<dyn Error>> {

//     let region = Region::NA;
//     let encrypted_summoner_id = "kGi6nGk-fB1OYuXKHh9sZTgGXwicDSc_4PdniwBq8OoTSEeM";

//     // Create RiotApi instance from key string.
//     let api_key = include_str!("apikey.txt");
//     let api = RiotApi::with_key(api_key);
//     let api = &api;

//     let lookbehind = Duration::days(3);
//     let earliest = Utc::now() - lookbehind;

//     println!("Earliest: {}.", earliest.timestamp_millis());

//     let path_match_out: PathBuf = [
//         "data",
//         &format!("{:?}", Region::NA).to_lowercase(),
//         &format!("match.{}.csv.gz", time::datetimestamp()),
//     ].iter().collect();

//     println!("{:?}", path_match_out);
//     let mut match_entries_out = util::csvgz::writer(path_match_out).expect("Failed to write.");
//     {
//         let mut rt = tokio::runtime::Runtime::new().unwrap();
//         rt.block_on(async {
//             let summoner = api.summoner_v4().get_by_summoner_id(region, &encrypted_summoner_id);
//             let summoner = summoner.await.expect("Failed to get summoner.");

//             let matches = api.match_v4().get_matchlist(region, &summoner.account_id,
//                 Some(earliest.timestamp_millis()), // begin_time
//                 None, // begin_index
//                 None, // champion
//                 None, // end_time
//                 None, // end_index
//                 Some(vec![ Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO_GAMES ]), // queue
//                 None, // season
//             );
//             let match_dto = matches.await.expect("Failed to get matchlist.").expect("Matchlist 404.");

//             for match_api in match_dto.matches {
//                 let match_model = Match {
//                     match_id: match_api.game_id as u64,
//                     rank_tier: Tier::CHALLENGER,
//                     ts: match_api.timestamp as u64,
//                 };
//                 match_entries_out.serialize(match_model).expect("Failed to serialize match.");
//             }
//         });
//     }
//     match_entries_out.flush().unwrap();

//     Ok(())
// }

pub fn main() {
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(main_async())
        .unwrap_or_else(|e| panic!("Failed to complete: {}", e));
}
