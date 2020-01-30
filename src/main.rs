// Dataflow description:
//
//  +----------------------------+      +-------------------------------+      +--------------------------+
//  | match.na.2020-02-26.csv.gz |      | summoner.na.2020-02-26.csv.gz |      | ranked league pagination |
//  +-------------+--------------+      +---------------+---------------+      +-------------+------------+
//                |                                     |                                    |
//                V                          +----<-----+---->----+                          V
//        +-------+-------+                  |                    |             +------------+-----------+
//        |    matchId    |                  V                    |             | summonerId -> data map |
//        | bitset filter |        +---------+----------+         |             +----------+---+---------+
//        +-------+-------+        | summoner selection |         |                        |   |
//                |                +---------+----------+         |                        |   |
//                |                          |                    V                        |   |
//                |                +----<----+                    |                        |   |
//                |                |                              |                        |   |
//                |                V                              |                        |   |
//                V     +----------+-----------+                  |                        |   |
//                |     | recent matchId fetch +--->---+-----<----+-----------<------------+   V
//                |     +----------+-----------+       |                                       |
//                |                |                   |                                       |
//                |     +-----<----+                   |                                       |
//                |     |                              |                                       |
//                V     V                              V                                       |
//       +--------+-----+--------+     +---------------+---------------+                       |
//       | filter seen  matchIds |     |           write new           |                       |
//       +-----------+-----------+     | summoner.na.2020-02-27.csv.gz |                       |
//                   |                 +-------------------------------+                       |
//                   V                                                                         |
//       +-----------+-----------+                                                             V
//       | streaming match fetch |     +----------------------------<--------------------------+
//       +-----------+-----------+     |
//                   |                 |
//                   V                 |
//            +------+------+          V     +-------------------+
//            | assign rank +----<-----+     | stats files .CSVs |
//            +------+------+                +---------+---------+
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
// #[macro_use] extern crate tokio;
// #[macro_use] extern crate scan_fmt;

mod util;
mod model;
mod pipeline;

use std::vec::Vec;
use std::error::Error;

use chrono::{ Duration };
use chrono::offset::Utc;
use futures::future::join_all;
// use itertools::Itertools;
use riven::RiotApi;
use riven::consts::{ Region, Queue, QueueType };
use tokio::task;

use model::summoner::Summoner;
use pipeline::source;
use pipeline::source_api;
use pipeline::mapping_api;


lazy_static! {
    static ref RIOT_API: RiotApi =
        RiotApi::with_key(include_str!("apikey.txt").trim());
}


async fn main_async() -> Result<(), Box<dyn Error>> {
    println!("Hello world");

    let region = Region::NA;
    let queue_type = QueueType::RANKED_SOLO_5x5;
    let queue = Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO_GAMES;

    let update_size: usize = 500;
    let lookbehind = Duration::days(3);
    let starttime = Utc::now() - lookbehind;
    let pagination_batch_size: usize = 10;

    // Unlike normal futures, this starts immediately (it seems).
    // Match bitset.
    let match_hbs = task::spawn_blocking(
        move || source::get_match_hybitset(region, starttime));
    // Oldest (or selected) summoners, for updating.
    let oldest_summoners = task::spawn_blocking(
        move || source::get_oldest_summoners(region, update_size));
    // All ranked summoners.
    let ranked_summoners = tokio::spawn(
        source_api::get_ranked_summoners(&RIOT_API, queue_type, region, pagination_batch_size));

    // Join match bitset and oldest selected summoners.
    let (mut match_hbs, oldest_summoners) = tokio::try_join!(match_hbs, oldest_summoners)?;
    let oldest_summoners = oldest_summoners?.collect::<Vec<Summoner>>();

    // Get new match IDs via matchlist.
    let oldest_summoners = mapping_api::update_missing_summoner_account_ids(
        &RIOT_API, region, 20, oldest_summoners).await;

    let new_match_ids = mapping_api::get_new_matchids(
        &RIOT_API, region, queue, 20, starttime, &oldest_summoners, &mut match_hbs).await;

    println!("new_match_ids len: {}.", new_match_ids.len());

    // Get new match values.
    // TODO: this should stream (?).
    let new_matches = mapping_api::get_matches(
        &RIOT_API, region, 20, new_match_ids);
    let new_matches = new_matches.await;

    // Completion of ranked_summoners map.
    let ranked_summoners = ranked_summoners.await?;

    println!("HBS len: {}.", match_hbs.len());
    println!("HBS density: {}.", match_hbs.density());
    // let j = serde_json::to_string(&match_hbs)?;
    // println!("HBS:\n{}.", j);

    let mut i: u32 = 0;
    for matche in new_matches {
        println!("Match: {}.", matche.game_id);
        for participant in matche.participant_identities {
            println!("Participant name: {}.", participant.player.summoner_name);
            println!("Participant tier: {:?}.", ranked_summoners.get(&participant.player.summoner_id).map(|s| s.rank_tier));
        }
        println!();
        i += 1;
        if i > 10 {
            break;
        };
    };

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
