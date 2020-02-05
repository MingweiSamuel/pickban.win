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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;
use std::vec::Vec;

use chrono::{ Duration };
use chrono::offset::Utc;
use futures::future::join_all;
use itertools::Itertools;
use riven::{ RiotApi, RiotApiConfig };
use riven::consts::{ Region, Tier, Queue, QueueType };
use tokio::fs;
use tokio::task;

use model::summoner::Summoner;
use model::r#match::{ MatchFileKey, Match };
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


const QUEUE_TYPE: QueueType = QueueType::RANKED_SOLO_5x5;
const QUEUE: Queue = Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO_GAMES;


async fn get_ranked_summoners(region: Region, pull_ranks: bool)
    -> Result<HashMap<String, Tier>, Box<dyn Error + Send>>
{
    let pagination_batch_size: usize = 10;
    if pull_ranks {
        let future = tokio::spawn(source_api::get_ranked_summoners(
            &RIOT_API, QUEUE_TYPE, region, pagination_batch_size));
        let hashmap = future.await.map_err(dyn_err)?;
        Ok(hashmap)
    } else {
        let future = task::spawn_blocking(move || source_fs::get_ranked_summoners(region));
        let hashmap = future.await
            .map_err(dyn_err)?
            .map_err(dyn_err)?;
        Ok(hashmap)
    }
}


async fn run_async(region: Region, update_size: usize, pull_ranks: bool) -> Result<(), Box<dyn Error>> {
    println!("Updating {} in region {:?}", update_size, region);
    if pull_ranks {
        println!("Updating ranks from API.");
    } else {
        println!("Using stored ranks.");
    }

    let update_size: usize = 5000;
    let lookbehind = Duration::weeks(1);
    let starttime = Utc::now() - lookbehind;

    let path_data = {
        let mut path_data = PathBuf::new();
        path_data.push("data");
        path_data.push(format!("{:?}", region).to_lowercase());
        path_data
    };
    fs::create_dir_all(&path_data).await?;

    // Match bitset.
    let match_hbs = tokio::spawn(
        pipeline::hybitset::read_match_hybitset(region));
    // Oldest (or selected) summoners, for updating.
    // Unlike normal futures, this starts automatically (it seems).
    let oldest_summoners = task::spawn_blocking(
        move || source_fs::get_oldest_summoners(region, update_size));
    // All ranked summoners.
    let ranked_summoners = get_ranked_summoners(region, pull_ranks);
        // if pull_ranks {
        //     let task = tokio::spawn(source_api::get_ranked_summoners(
        //         &RIOT_API, queue_type, region, pagination_batch_size))
        // } else {
        //     task::spawn_blocking(
        //         move || source_fs::get_ranked_summoners(region))
        // };

    // Join match bitset and oldest selected summoners.
    let (match_hbs, oldest_summoners) = tokio::try_join!(match_hbs, oldest_summoners)?;
    let mut match_hbs = match_hbs.map_err(|e| e as Box<dyn Error>)?;
    let oldest_summoners = oldest_summoners?.expect("Summoner csvgz doesn't exist.").collect::<Vec<Summoner>>();

    // Get new match IDs via matchlist.
    let oldest_summoners = mapping_api::update_missing_summoner_account_ids(
        &RIOT_API, region, 20, oldest_summoners).await;
    let update_summoner_ts: u64 = time::epoch_millis();

    let new_match_ids = mapping_api::get_new_matchids(
        &RIOT_API, region, QUEUE, 20, starttime, &oldest_summoners, &mut match_hbs).await;
    // Updated summoners to update in CSV.
    let mut updated_summoners_by_id = oldest_summoners.into_iter()
        // TODO extra clone.
        .map(|summoner| { (summoner.encrypted_summoner_id.clone(), summoner) })
        .collect::<HashMap<_, _>>();
    
    // Read back and update summoners.
    let write_summoners = {
        let all_summoners = task::spawn_blocking(
            move || source_fs::get_all_summoners(region));
        let all_summoners = all_summoners.await??.expect("No summoner csvgz found (TODO).");
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

    println!("!! new_match_ids len: {}.", new_match_ids.len());

    let write_match_hbs = pipeline::hybitset::write_match_hybitset(region, &match_hbs);

    // Get new match values.
    // TODO: this should stream (?).
    let new_matches = mapping_api::get_matches(
        &RIOT_API, region, 20, new_match_ids);
    let new_matches = new_matches.await;

    // Completion of ranked_summoners map.
    let ranked_summoners = ranked_summoners.await
        .map_err(|e| e as Box<dyn Error>)?;

    println!("HBS len: {}.", match_hbs.len());
    println!("HBS density: {}.", match_hbs.density());

    // Handle matches.
    // Matches grouped by their file key for convenient access.
    let grouped_new_matches = new_matches.into_iter()
        .map(|matche| (MatchFileKey::from(&matche), matche))
        .into_group_map();

    let mut write_matches_tasks = Vec::with_capacity(grouped_new_matches.len());

    for (match_key, matches) in grouped_new_matches {
        let version = match_key.version;
        let iso_week = match_key.iso_week;

        // Create directory (if not exists) for this patch.
        let mut path_data_key = path_data.clone();
        path_data_key.push(format!("{}.{}", version.0, version.1));
        fs::create_dir_all(&path_data_key).await?;

        let model_matches = matches.iter()
            .map(|matche| {
                let avg_tier = util::lol::match_avg_tier(matche.participant_identities.iter()
                    .map(|participant| ranked_summoners.get(&participant.player.summoner_id)));
                Match {
                    match_id: matche.game_id as u64,
                    rank_tier: avg_tier,
                    ts: matche.game_creation as u64,
                }
            })
            .collect::<Vec<_>>();

        let iso_week_str = format!("{:04}-W{:02}", iso_week.0, iso_week.1);
        // println!("Version: {:?}, Iso Week: {}.", version, iso_week_str);
        // for matche in &matches {
            
        //     println!("    {} ({:?})", matche.game_id, avg_tier);
        //     model_matches.push()
        // }

        let write_matches = task::spawn_blocking(
            move || source_fs::write_matches(&path_data_key, &iso_week_str, model_matches.iter()));
        write_matches_tasks.push(write_matches);
    };

    // Join not needed since both are already started.
    for res in join_all(write_matches_tasks).await {
        res??;
    }
    write_summoners.await??;
    write_match_hbs.await?;

    println!("Done.");
    Ok(())
}

pub fn main() {
    use clap::{ Arg, App };

    let argparse = App::new("pickban.win script")
        .version("0.1.0")
        .about("Gets data from Riot API.")
        .arg(Arg::with_name("region")
            .short("r")
            .long("region")
            .takes_value(true)
            .default_value("NA")
            .help("Region to run on."))
        .arg(Arg::with_name("update size")
            .short("n")
            .long("update-size")
            .takes_value(true)
            .default_value("100")
            .help("Number of summoners to update."))
        .arg(Arg::with_name("pull ranks")
            .short("pr")
            .long("pull-ranks")
            .takes_value(false))
        .get_matches();

    let region_str = argparse.value_of("region").unwrap();
    let region: Region = region_str.parse()
        .unwrap_or_else(|_e| {
            println!("Unknown region: {}.", region_str);
            std::process::exit(1);
        });

    let update_size_str = argparse.value_of("update size").unwrap();
    let update_size: usize = update_size_str.parse()
        .unwrap_or_else(|_e| {
            println!("Invalid update size: {}.", update_size_str);
            std::process::exit(1);
        });

    let pull_ranks = argparse.value_of("pull ranks").is_some();

    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(run_async(region, update_size, pull_ranks))
        .unwrap_or_else(|e| panic!("Failed to complete: {}", e));
}
