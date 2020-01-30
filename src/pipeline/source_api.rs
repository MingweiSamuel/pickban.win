use std::collections::HashMap;

use futures::future::join_all;
use riven::consts::{ Region, Tier, Division, QueueType };
use riven::RiotApi;

use crate::model::summoner::Summoner;
use crate::util::time;


#[allow(dead_code)]
pub async fn get_ranked_summoners(api: &RiotApi, queue_type: QueueType, region: Region, batch_size: usize)
    -> HashMap<String, Summoner>
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
            println!("Starting {} {}.", tier, division);
            let mut page: i32 = 1;

            'batchloop: loop {
                // Batches of multiple pages.
                let mut league_batch = Vec::with_capacity(batch_size);

                for _ in 0..batch_size {
                    league_batch.push(
                        api.league_exp_v4().get_league_entries(region, queue_type, *tier, *division, Some(page)));
                    page += 1;
                };

                let league_batch = join_all(league_batch).await;
                let ts = time::epoch_millis();

                let league_batch = league_batch.into_iter()
                    .enumerate()
                    .flat_map(|(i, league_entries)|
                        league_entries.unwrap_or_else(|e| {
                            println!("Failed to get league page {}, error: {}.", i, e);
                            vec![]
                        })
                    )
                    .map(|league_entry| Summoner {
                        encrypted_summoner_id: league_entry.summoner_id,
                        encrypted_account_id: None,
                        league_id: league_entry.league_id.to_owned(), // Extra copy, sucks :)
                        rank_tier: Some(league_entry.tier),
                        games_per_day: None,
                        ts: Some(ts),
                    })
                    // TODO: copy.
                    .map(|summoner| (summoner.encrypted_summoner_id.clone(), summoner));
                
                let len_before = out.len();
                out.extend(league_batch);

                // No more data came.
                if len_before == out.len() {
                    break 'batchloop;
                };
            };
        };
    };

    out
}
