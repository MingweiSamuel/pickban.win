use std::collections::HashMap;

use futures::future::join_all;
use riven::consts::{ Region, Tier, Division, QueueType };
use riven::RiotApi;

use crate::model::summoner::Summoner;
use crate::util::time;


#[allow(dead_code)]
pub async fn get_ranked_summoners(api: &RiotApi, queue_type: QueueType, region: Region, batch_size: usize)
    -> HashMap<String, Tier>
{    
    let mut out = HashMap::with_capacity(65_536);

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
            let mut page: usize = 1;

            'batchloop: loop {
                // Batches of multiple pages.
                let mut league_batch = Vec::with_capacity(batch_size);

                for _ in 0..batch_size {
                    league_batch.push(
                        api.league_exp_v4().get_league_entries(region, queue_type, *tier, *division, Some(page as i32)));
                    page += 1;
                };

                let league_batch = join_all(league_batch).await;
                // let ts = time::epoch_millis();

                for (i, league_entries) in league_batch.into_iter().enumerate() {
                    match league_entries {
                        Err(e) => {
                            println!("Failed to get league page {}, error: {}, retries: {}, response {:?}.",
                                page - 10 + i, e.source_reqwest_error(), e.retries(), e.response());
                        },
                        Ok(league_entries) => {
                            if 0 == league_entries.len() {
                                println!("{} {} DONE. <{} pages.", tier, division, page - 1);
                                break 'batchloop;
                            };
                            let summoners_by_id = league_entries
                                .into_iter()
                                .map(|league_entry| (
                                    league_entry.summoner_id,
                                    league_entry.tier, //league_entry.rank),
                                ));
                                // .map(|league_entry| Summoner {
                                //     encrypted_summoner_id: league_entry.summoner_id,
                                //     encrypted_account_id: None,
                                //     league_id: league_entry.league_id.to_owned(), // Extra copy, sucks :)
                                //     rank_tier: Some(league_entry.tier),
                                //     games_per_day: None,
                                //     ts: Some(ts),
                                // })
                                // .map(|summoner| (summoner.encrypted_summoner_id.clone(), summoner));
                            out.extend(summoners_by_id);
                        },
                    };
                };
            };
        };
    };

    out
}
