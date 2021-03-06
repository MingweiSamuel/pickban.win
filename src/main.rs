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

mod util;
mod model;
mod pipeline;

use std::collections::HashMap;
use std::error::Error;
use std::path::{ PathBuf };
use std::vec::Vec;
use std::sync::Arc;

use chrono::{ Duration };
use chrono::offset::Utc;
use futures::future::join_all;
// use itertools::Itertools;
use riven::{ RiotApi, RiotApiConfig };
use riven::consts::{ Region, Queue, QueueType, Tier };
use riven::models::match_v4;
use tokio::fs;
use tokio::task;
use tokio::sync::mpsc;

use model::summoner::Summoner;
use model::r#match::{ MatchFileKey, Match };
use pipeline::basic;
use pipeline::source_fs;
use pipeline::mapping_api;
use util::time;
use util::hybitset::HyBitSet;


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



async fn run_async(region: Region, update_size: usize, pull_ranks: bool) -> Result<(), Box<dyn Error>> {
    println!("Updating {} in region {:?}", update_size, region);
    if pull_ranks {
        println!("Updating ranks from API.");
    } else {
        println!("Using stored ranks.");
    }

    let lookbehind = Duration::weeks(1);
    let starttime = Utc::now() - lookbehind;

    let path_data: PathBuf = [
        "data",
        &format!("{:?}", region).to_lowercase(),
    ].iter().collect();

    let path_data_local = {
        let mut x = path_data.clone();
        x.push("local");
        x
    };

    fs::create_dir_all(&path_data_local).await?;

    // Match bitset.
    let match_hbs = tokio::spawn(pipeline::hybitset::read_match_hybitset(path_data_local.clone()));
    // Oldest (or selected) summoners, for updating.
    // Unlike normal futures, this starts automatically (it seems).
    let oldest_summoners = {
        let path_data_local = path_data_local.clone();
        task::spawn_blocking(
            move || source_fs::get_oldest_summoners(path_data_local, update_size))
    };
    // All ranked summoners.
    let ranked_summoners = basic::get_ranked_summoners(&RIOT_API, QUEUE_TYPE, region, &path_data_local, pull_ranks);

    // Join match bitset and oldest selected summoners.
    let (match_hbs, oldest_summoners) = tokio::try_join!(match_hbs, oldest_summoners)?;
    let match_hbs = match_hbs.map_err(|e| e as Box<dyn Error>)?;
    let mut match_hbs = match_hbs.unwrap_or_else(|| HyBitSet::new()); // Create new if none saved.
    let oldest_summoners: Vec<Summoner> = match oldest_summoners? {
        Some(x) => x.collect(),
        None => {
            if !pull_ranks {
                println!("!! No Summoner .csv.gz found. Use --pull-ranks to start new.");
                std::process::exit(2);
            }
            vec![]
        },
    };

    println!("Obtained oldest summoners, count: {}.", oldest_summoners.len());

    // Get new match IDs via matchlist.
    let mut oldest_summoners: Vec<Summoner> = mapping_api::update_missing_summoner_account_ids(
        &RIOT_API, region, 20, oldest_summoners).await;
    println!("Added missing account IDs, cound: {}.", oldest_summoners.len());
    let update_summoner_ts: u64 = time::epoch_millis();

    let new_match_ids = mapping_api::get_new_matchids_update_summoner_gpd(
        &RIOT_API, region, QUEUE, 20, starttime, &mut oldest_summoners, &mut match_hbs).await;
    println!("Getting new matches, count: {}.", new_match_ids.len());
    // Updated summoners to update in CSV.
    let mut updated_summoners_by_id = oldest_summoners.into_iter()
        // TODO extra clone.
        .map(|summoner| { (summoner.encrypted_summoner_id.clone(), summoner) })
        .collect::<HashMap<_, _>>();

    let write_match_hbs = pipeline::hybitset::write_match_hybitset(&path_data_local, &match_hbs);

    // Completion of ranked_summoners map.
    let ranked_summoners = ranked_summoners.await
        .map_err(|e| e as Box<dyn Error>)?;
    let ranked_summoners = Arc::new(ranked_summoners);

    println!("HBS len: {}.", match_hbs.len());
    println!("HBS density: {}.", match_hbs.density());

    // Read back and update summoners.
    let write_summoners = {
        println!("Writing updated summoners.");
        let ranked_summoners = ranked_summoners.clone();
        let path_data_local = path_data_local.clone();
        task::spawn_blocking(move || basic::write_summoners(
            path_data_local, update_summoner_ts, &mut updated_summoners_by_id, ranked_summoners))
    };

    // Write rank -> league csv
    let write_leagues = {
        println!("Writing leagues.");
        // TODO: could optimize by onlying doing this when pull_ranks is true.
        let ranked_summoners = ranked_summoners.clone();
        let path_data = path_data.clone();
        task::spawn_blocking(move || basic::write_league_ids(path_data, ranked_summoners))
    };

    // Get new match values.
    // TODO: this should stream (?).
    let (matches_sender, matches_receiver) = mpsc::unbounded_channel();
    let matches_mpsc = tokio::spawn(mapping_api::get_matches_mpsc(matches_sender,
        &RIOT_API, region, 40, new_match_ids));

    // let new_matches = new_matches.await;
    println!("Started getting matches.");

    // Handle matches.
    // Matches grouped by their file key for convenient access.
    let grouped_new_matches = handle_matches(matches_receiver, ranked_summoners.clone()).await;

    // Collect any errors from matches mpsc.
    {
        let count = matches_mpsc.await??;
        println!("Fetched {} matches.", count);
    }

    // let grouped_new_matches = new_matches.into_iter()
    //     .map(|matche| (MatchFileKey::from(&matche), matche))
    //     .into_group_map();

    let mut write_matches_tasks = Vec::with_capacity(grouped_new_matches.len());

    for (match_key, model_matches) in grouped_new_matches {
        let version = match_key.version;
        let iso_week = match_key.iso_week;

        // Create directory (if not exists) for this patch.
        let mut path_data_key = path_data.clone();
        path_data_key.push(format!("{}.{}", version.0, version.1));
        fs::create_dir_all(&path_data_key).await?;

        // let model_matches = matches.iter()
        //     .map(|matche| {
        //         let tiers = matche.participant_identities.iter()
        //             .map(|participant| {
        //                 ranked_summoners.get(&participant.player.summoner_id)
        //                     .map(|(tier, _league_id)| tier)
        //                     .cloned()
        //             });
        //         let avg_tier = util::lol::match_avg_tier(tiers);
        //         Match {
        //             match_id: matche.game_id as u64,
        //             rank_tier: avg_tier,
        //             ts: matche.game_creation as u64,
        //         }
        //     })
        //     .collect::<Vec<_>>();

        let iso_week_str = format!("{:04}-W{:02}", iso_week.0, iso_week.1);

        let write_matches = task::spawn_blocking(
            move || source_fs::write_matches(&path_data_key, &iso_week_str, model_matches.iter()));
        write_matches_tasks.push(write_matches);
    };

    // Join not needed since both are already started.
    for res in join_all(write_matches_tasks).await {
        res??;
    }
    write_summoners.await?.map_err(|e| e as Box<dyn Error>)?;
    write_leagues.await??;
    write_match_hbs.await?;

    println!("Done.");
    Ok(())
}

async fn handle_matches(mut matches_receiver: mpsc::UnboundedReceiver<match_v4::Match>,
    ranked_summoners: Arc<HashMap<String, (Tier, String)>>)
    -> HashMap<MatchFileKey, Vec<Match>>
{
    let mut out = HashMap::new();
    while let Some(matche) = matches_receiver.recv().await {
        let match_key = MatchFileKey::from(&matche);

        let tiers = matche.participant_identities.iter()
            .map(|participant| {
                ranked_summoners.get(&participant.player.summoner_id)
                    .map(|(tier, _league_id)| tier)
                    .cloned()
            });
        let avg_tier = util::lol::match_avg_tier(tiers);

        let vec = out.entry(match_key).or_insert_with(Vec::new);
        vec.push(Match {
            match_id: matche.game_id as u64,
            rank_tier: avg_tier,
            ts: matche.game_creation as u64,
        })
    };
    out
}

pub fn main() {
    use clap::{ Arg, App };

    let argparse = App::new("pickban.win script")
        .version("0.1.0")
        .about("Gets data from Riot API.")
        .arg(Arg::with_name("region")
            .takes_value(true)
            .help("Region to run on.")
            .index(1))
        .arg(Arg::with_name("update size")
            .takes_value(true)
            .help("Number of summoners to update.")
            .index(2))
        .arg(Arg::with_name("pull ranks")
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

    let pull_ranks = argparse.is_present("pull ranks");

    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(run_async(region, update_size, pull_ranks))
        .unwrap_or_else(|e| panic!("Failed to complete: {}", e));
}
