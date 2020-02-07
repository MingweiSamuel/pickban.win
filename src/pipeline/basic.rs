use std::collections::{ BTreeSet, HashMap };
use std::error::Error;
use std::path::{ Path, PathBuf };

use riven::{ RiotApi };
use riven::consts::{ Region, Tier, QueueType };
use tokio::task;

use crate::dyn_err;
use crate::model::summoner::Summoner;
use crate::model::league::League;
use crate::pipeline::{ source_fs, source_api };

pub async fn get_ranked_summoners(riot_api: &'static RiotApi, queue_type: QueueType,
    region: Region, path_data_local: &PathBuf, pull_ranks: bool)
    -> Result<HashMap<String, (Tier, String)>, Box<dyn Error + Send>>
{
    let pagination_batch_size: usize = 10;
    if pull_ranks {
        let future = tokio::spawn(source_api::get_ranked_summoners(
            riot_api, queue_type, region, pagination_batch_size));
        let hashmap = future.await.map_err(dyn_err)?;
        Ok(hashmap)
    } else {
        let path_data_local = path_data_local.clone();
        let future = task::spawn_blocking(move || source_fs::get_ranked_summoners(path_data_local));
        let hashmap = future.await
            .map_err(dyn_err)?
            .map_err(dyn_err)?;
        Ok(hashmap)
    }
}

pub fn write_league_ids<RS>(path_data: impl AsRef<Path>, ranked_summoners: RS)
    -> std::io::Result<()>
where
    RS: AsRef<HashMap<String, (Tier, String)>>
{
    let mut leagues = BTreeSet::new();
    for (tier, league_id) in ranked_summoners.as_ref().values() {
        leagues.insert(League {
            league_id: league_id.clone(), //TODO extra clone.
            tier: *tier,
        });
    };
    source_fs::write_leagues(path_data, leagues.into_iter().rev())
}

pub fn write_summoners<RS>(path: impl AsRef<Path>, update_summoner_ts: u64,
    updated_summoners_by_id: &mut HashMap<String, Summoner>,
    ranked_summoners: RS)
    -> Result<(), Box<dyn Error + Send>>
where
    RS: AsRef<HashMap<String, (Tier, String)>>
{
    let all_summoners = source_fs::get_all_summoners(&path).map_err(dyn_err)?;

    match all_summoners {
        None => { // THERES NO SUMMONER .CSV.GZ TO READ FROM!
            assert!(updated_summoners_by_id.is_empty(), "all_summoners empty but updated_summoners_by_id not empty.");

            let summoner_models = ranked_summoners.as_ref().iter()
                .map(|(summoner_id, (tier, league_id))| Summoner {
                    encrypted_summoner_id: summoner_id.clone(), // TODO extra clone.
                    encrypted_account_id: None,
                    league_id: Some(league_id.clone()), // TODO extra clone.
                    rank_tier: Some(*tier),
                    games_per_day: None,
                    ts: None,
                });

            source_fs::write_summoners(&path, summoner_models).map_err(dyn_err)?;
        },
        Some(all_summoners) => {
            // Set timestamps on updated summoner.
            let all_summoners = all_summoners.map(move |mut summoner| {
                // Update timestamp and games per day (TODO).
                if let Some(updated_summoner) = updated_summoners_by_id.remove(&summoner.encrypted_summoner_id) {
                    summoner.ts = Some(update_summoner_ts);
                    summoner.encrypted_account_id = updated_summoner.encrypted_account_id;
                    // TODO update any other things.
                }
                // Update tiers.
                if let Some((tier, league_id)) = ranked_summoners.as_ref().get(&summoner.encrypted_summoner_id) {
                    summoner.rank_tier = Some(*tier);
                    summoner.league_id = Some(league_id.clone()); // TODO bad copy.
                }
                summoner
            });

            // Write summoners job.
            source_fs::write_summoners(&path, all_summoners).map_err(dyn_err)?;
        },
    };
    Ok(())
}
