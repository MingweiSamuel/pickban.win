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
use std::sync::Mutex;

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
    let update_size: usize = 500;
    let lookbehind = Duration::hours(4);
    let starttime = Utc::now() - lookbehind;

    // Create RiotApi instance from key string.
    let api_key = include_str!("apikey.txt");
    let api = RiotApi::with_key(api_key);
    let api = &api;

    // Unlike normal futures, this starts immediately (it seems).
    // Match bitset.
    let match_hbs = task::spawn_blocking(
        move || source::get_match_hybitset(region, starttime));
    // Oldest (or selected) summoners, for updating.
    let oldest_summoners = task::spawn_blocking(
        move || source::get_oldest_summoners(region, update_size));

    let (mut match_hbs, oldest_summoners) = tokio::try_join!(match_hbs, oldest_summoners)?;
    // let oldest_summoners: Vec<Summoner> = oldest_summoners?.collect();

    let new_matches = {
        // Chunk size? Shitty parallelism?
        let mut new_matches = vec![];
        let summoner_chunk_size: usize = 10;
        for summoners_chunk in oldest_summoners?.collect::<Vec<_>>().chunks(summoner_chunk_size) {
            let chunk_futures = summoners_chunk.into_iter().map(|summoner| {
                let matches_dto = api.match_v4().get_matchlist(region, &summoner.encrypted_account_id,
                    Some(starttime.timestamp_millis()), // begin_time
                    None, // begin_index
                    None, // champion
                    None, // end_time
                    None, // end_index
                    Some(vec![ Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO_GAMES ]), // queue
                    None, // season
                );
                matches_dto
            }).collect::<Vec<_>>();

            let lists_of_matches = join_all(chunk_futures).await;
            let lists_of_matches = lists_of_matches.into_iter()
                .flat_map(|m: riven::Result<Option<riven::models::match_v4::Matchlist>>| m.expect("Failed to get matchlist").map_or(vec![], |m| m.matches));
            for matche in lists_of_matches {
                // Insert into bitmap. If match was not in bitmap, then add it to new_matches.
                if !match_hbs.insert(matche.game_id as usize) {
                    new_matches.push(matche.game_id);
                }
            }
        }
        new_matches
    };

    println!("New matches len: {}.", new_matches.len());
    // println!("New matches: {:?}.", new_matches);

    {
        let matches_chunk_size: usize = 50;
        for matches_chunk in new_matches.chunks(matches_chunk_size) {

            let chunk_futures = matches_chunk.into_iter()
                .map(|match_id| api.match_v4().get_match(region, *match_id))
                .collect::<Vec<_>>();

            let matches = join_all(chunk_futures).await;
            let matches = matches.into_iter()
                .map(|m| m.expect("Failed to get matchlist.").expect("Match not found."));

            // TODO: handle matches.
            let _ = matches;
        }
    }

    println!("HBS len: {}.", match_hbs.len());
    println!("HBS density: {}.", match_hbs.density());
    // let j = serde_json::to_string(&match_hbs)?;
    // println!("HBS:\n{}.", j);

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
