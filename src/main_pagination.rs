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

#![feature(async_closure)]

#[macro_use]
extern crate lazy_static;

mod util;
mod model;
mod pipeline;

use std::vec::Vec;
use std::path::PathBuf;

use futures::future::join_all;
use riven::RiotApi;
use riven::consts::Region;
use riven::consts::QueueType;
use riven::consts::Tier;
use riven::consts::Division;

use model::summoner::Summoner;

pub fn main() {
    println!("Hello, world!~");

    let region = Region::NA;
    let queue_type = QueueType::RANKED_SOLO_5x5;

    // Create RiotApi instance from key string.
    let api_key = include_str!("apikey.txt");
    let riot_api = RiotApi::with_key(api_key);
    let riot_api = &riot_api;

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
                Tier::CHALLENGER, Tier::GRANDMASTER, Tier::MASTER,
                Tier::DIAMOND, Tier::PLATINUM, Tier::GOLD,
                Tier::SILVER, Tier::BRONZE, Tier::IRON,
            ].iter() {

                let divisions: &[Division] = if tier.is_apex_tier() {
                    &[ Division::I ]
                } else {
                    &[ Division::I, Division::II, Division::III, Division::IV ]
                };

                for division in divisions.iter() {
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

                        let league_batch = league_batch.into_iter()
                            .enumerate()
                            .flat_map(|(i, league_entries)|
                                league_entries.unwrap_or_else(|e| {
                                    println!("Failed to get league page {}, error: {}.", i, e);
                                    vec![]
                                }))
                            .collect::<Vec<_>>();

                        if 0 == league_batch.len() {
                            break 'batchloop;
                        }
                        for league_entry in league_batch {
                            match_entries_out.serialize(Summoner {
                                encrypted_summoner_id: league_entry.summoner_id,
                                encrypted_account_id: None,
                                league_id: league_entry.league_id.to_owned(), // Extra copy, sucks :)
                                rank_tier: Some(league_entry.tier),
                                games_per_day: None,
                                ts: Some(ts),
                            }).expect("failed to serialize");
                        }

                        // let summoner_futures = league_entries.iter()
                        //     .map(async move |league_entry| {
                        //         let summoner_data = riot_api.summoner_v4()
                        //             .get_by_summoner_id(region, &league_entry.summoner_id).await
                        //             .expect("Failed to get summoner.");
                        //         summoner_data
                        //     })
                        //     .collect::<Vec<_>>();
                        // let summoner_datas = join_all(summoner_futures).await;

                        // for (league_entry, summoner_data) in league_entries.iter().zip(summoner_datas) {
                        //     match_entries_out.serialize(Summoner {
                        //         encrypted_summoner_id: summoner_data.id,
                        //         encrypted_account_id: Some(summoner_data.account_id),
                        //         league_id: league_entry.league_id.to_owned(), // Sucks, copying for no reason.
                        //         rank_tier: league_entry.tier,
                        //         games_per_day: None,
                        //         ts: Some(ts),
                        //     }).expect("Failed to serialize");
                        // }
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