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
// Things stored in summoner.csv.gz:
// - Everything (in case rate limits get tightened)
//
// Ranks (which are need) are pulled in their entirety each run.
//
//

// #![deny(unused_variables)]
#![deny(unused_must_use)]

#[macro_use] extern crate lazy_static;
// #[macro_use] extern crate tokio;
// #[macro_use] extern crate scan_fmt;

mod util;
mod model;
mod pipeline;

use std::vec::Vec;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::error::Error;

use chrono::{ Duration };
use chrono::offset::Utc;
// use futures::future::join_all;
// use itertools::Itertools;
use riven::{ RiotApi, RiotApiConfig };
use riven::consts::{ Region, Queue, QueueType };
use tokio::task;

use model::summoner::Summoner;
use pipeline::source_fs;
use pipeline::source_api;
use pipeline::mapping_api;
use util::time;


lazy_static! {
    static ref RIOT_API: RiotApi =
        RiotApi::with_config(
            RiotApiConfig::with_key(include_str!("apikey.txt").trim())
                .preconfig_throughput());
}


pub fn dyn_err<E: Error + Send + 'static>(e: E) -> Box<dyn Error + Send> {
    Box::new(e)
}

pub fn distance<T: std::ops::Sub<Output = T> + Ord>(x: T, y: T) -> T {
    if x < y {
        y - x
    } else {
        x - y
    }
}


async fn main_async() -> Result<(), Box<dyn Error>> {
    println!("Hello world");

    let region = Region::NA;
    let queue_type = QueueType::RANKED_SOLO_5x5;
    let queue = Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO_GAMES;

    let update_size: usize = 10;
    let lookbehind = Duration::weeks(2);
    let starttime = Utc::now() - lookbehind;
    let pagination_batch_size: usize = 10;

    // Match bitset.
    let match_hbs = tokio::spawn(
        pipeline::hybitset::read_match_hybitset(region));
    // Oldest (or selected) summoners, for updating.
    // Unlike normal futures, this starts automatically (it seems).
    let oldest_summoners = task::spawn_blocking(
        move || source_fs::get_oldest_summoners(region, update_size));
    // All ranked summoners.
    // let ranked_summoners = tokio::spawn(
    //     source_api::get_ranked_summoners(&RIOT_API, queue_type, region, pagination_batch_size));
    let ranked_summoners = task::spawn_blocking(
        move || source_fs::get_ranked_summoners(region));

    // Join match bitset and oldest selected summoners.
    let (match_hbs, oldest_summoners) = tokio::try_join!(match_hbs, oldest_summoners)?;
    let mut match_hbs = match_hbs.map_err(|e| e as Box<dyn Error>)?;
    let oldest_summoners = oldest_summoners?.collect::<Vec<Summoner>>();

    // Get new match IDs via matchlist.
    let oldest_summoners = mapping_api::update_missing_summoner_account_ids(
        &RIOT_API, region, 20, oldest_summoners).await;
    let update_summoner_ts: u64 = time::epoch_millis();

    let new_match_ids = mapping_api::get_new_matchids(
        &RIOT_API, region, queue, 20, starttime, &oldest_summoners, &mut match_hbs).await;

    let mut updated_summoners_by_id = oldest_summoners.into_iter()
        // TODO extra clone.
        .map(|summoner| { (summoner.encrypted_summoner_id.clone(), summoner) })
        .collect::<HashMap<_, _>>();

    println!("!! new_match_ids len: {}.", new_match_ids.len());

    let write_match_hbs = pipeline::hybitset::write_match_hybitset(region, &match_hbs);

    // Get new match values.
    // TODO: this should stream (?).
    let new_matches = mapping_api::get_matches(
        &RIOT_API, region, 20, new_match_ids);
    let new_matches = new_matches.await;

    // Completion of ranked_summoners map.
    let ranked_summoners = ranked_summoners.await??;

    println!("HBS len: {}.", match_hbs.len());
    println!("HBS density: {}.", match_hbs.density());

    // Handle matches
    for matche in new_matches {
        let avg_tier = util::lol::match_avg_tier(matche.participant_identities.iter()
            .map(|participant| ranked_summoners.get(&participant.player.summoner_id)));
        println!("Match: {}, tier: {:?}, ver: {}.", matche.game_id, avg_tier, matche.game_version);
    };
    // TODO update summoners here.


    // Read back and update summoners.
    let write_summoners = {
        let all_summoners = task::spawn_blocking(
            move || source_fs::get_all_summoners(region));
        let all_summoners = all_summoners.await??;
        // Set timestamps on updated summoner.
        let all_summoners = all_summoners.map(move |mut summoner| {
            if let Some(updated_summoner) = updated_summoners_by_id.remove(&summoner.encrypted_summoner_id) {
                summoner.ts = Some(update_summoner_ts);
                summoner.encrypted_account_id = updated_summoner.encrypted_account_id;
                // TODO update any other things.
            }
            summoner
        });

        // Write summoners job.
        let write_summoners = task::spawn_blocking(
            move || source_fs::write_summoners(region, all_summoners));
        write_summoners
    };

    // Join not needed since both are already started.
    write_summoners.await??;
    write_match_hbs.await?;

    println!("Done.");
    Ok(())
}

pub fn main() {
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(main_async())
        .unwrap_or_else(|e| panic!("Failed to complete: {}", e));
}
