use std::cmp;

use chrono::DateTime;
use chrono::offset::Utc;
use futures::future::join_all;
use itertools::Itertools;
use riven::consts::{ Region, Queue };
use riven::models::match_v4::Match;
use riven::RiotApi;
use tokio::sync::mpsc;

use crate::model::summoner::Summoner;
use crate::util::hybitset::HyBitSet;


const MILLIS_PER_DAY: usize = 24 * 3600 * 1000;


pub async fn update_missing_summoner_account_ids(
    api: &RiotApi, region: Region, chunk_size: usize, mut summoners: Vec<Summoner>) -> Vec<Summoner>
{
    // Summoners without AccountIDs (AID).
    for summoner_chunk in summoners.iter_mut()
        .filter(|summoner| summoner.encrypted_account_id.is_none())
        .chunks(chunk_size)
        .into_iter()
    {
        let summoner_chunk = summoner_chunk.collect::<Vec<_>>();

        let summoner_datas = summoner_chunk.iter()
            .map(|summoner| &summoner.encrypted_summoner_id)
            .map(|sid| api.summoner_v4().get_by_summoner_id(region, sid))
            .collect::<Vec<_>>();
        let summoner_datas = join_all(summoner_datas).await;

        for (summoner, summoner_data) in summoner_chunk.into_iter().zip(summoner_datas.into_iter()) {
            if let Ok(summoner_data) = summoner_data {
                summoner.encrypted_account_id = Some(summoner_data.account_id);
            };
        };
    };
    summoners
}

pub async fn get_new_matchids_update_summoner_gpd(
    api: &RiotApi, region: Region, queue: Queue,
    batch_size: usize, starttime: DateTime<Utc>,
    oldest_summoners: &mut Vec<Summoner>, match_hbs: &mut HyBitSet)
    -> Vec<i64>
{
    let now_millis = Utc::now().timestamp_millis();
    // Chunk size? Shitty parallelism?
    let mut new_matches = vec![];
    for summoners_chunk in oldest_summoners.chunks_mut(batch_size) {
        let chunk_futures = summoners_chunk.iter()
            .filter(|summoner| summoner.encrypted_account_id.is_some())
            .map(|summoner| {
                let begin_millis = cmp::max(starttime.timestamp_millis(), summoner.ts.unwrap_or(0) as i64);
                let matches_dto = api.match_v4().get_matchlist(
                    region,
                    summoner.encrypted_account_id.as_ref().unwrap(),
                    Some(begin_millis), // begin_time
                    None, // begin_index
                    None, // champion
                    None, // end_time
                    None, // end_index
                    Some(vec![ queue ]), // queue
                    None, // season
                );
                matches_dto
            }).collect::<Vec<_>>();

        let list_of_lists_of_matches = join_all(chunk_futures).await;

        let lists_of_match_ids = list_of_lists_of_matches.into_iter()
            .zip(summoners_chunk.iter_mut())
            .flat_map(|(m, summoner): (riven::Result<Option<riven::models::match_v4::Matchlist>>, &mut Summoner)| {
                let matchlist_opt = m.expect("Failed to get matchlist");
                match matchlist_opt {
                    Some(matchlist) => {
                        // TODO: duplicate begin_time for each summoner.
                        // Code here updates games_per_day.
                        let begin_millis = cmp::max(starttime.timestamp_millis(), summoner.ts.unwrap_or(0) as i64);
                        let delta_millis = now_millis - begin_millis;
                        let new_games_per_day = ((matchlist.matches.len() * MILLIS_PER_DAY) as f32) / (delta_millis as f32);
                        let old_games_per_day = summoner.games_per_day.unwrap_or(new_games_per_day);
                        summoner.games_per_day = Some((old_games_per_day + new_games_per_day) / 2.0);
                        // Return matchlist.
                        matchlist.matches
                    },
                    None => vec![],
                }
            })
            .map(|matche| matche.game_id);
        for match_id in lists_of_match_ids {
            // Insert into bitmap. If match was not in bitmap, then add it to new_matches.
            if !match_hbs.insert(match_id as usize) {
                new_matches.push(match_id);
            }
        }
    }
    new_matches
}

pub async fn get_matches_mpsc(sender: mpsc::UnboundedSender<Match>,
    api: &RiotApi, region: Region, chunk_size: usize, match_ids: Vec<i64>)
    -> Result<usize, mpsc::error::SendError<Match>>
{
    let mut count = 0;
    for match_ids_chunk in match_ids.chunks(chunk_size) {

        let chunk_futures = match_ids_chunk.into_iter()
            .map(|match_id| api.match_v4().get_match(region, *match_id))
            .collect::<Vec<_>>();

        let matches = join_all(chunk_futures).await;
        let matches = matches.into_iter()
            .filter_map(|m| m.ok()) // Remove errors (TODO: silent).
            .filter_map(|m| m); // Remove 404 (TODO: silent).

        for matche in matches {
            sender.send(matche)?;
            count += 1;
            if 0 == count % 10_000 {
                println!("  Fetched {} matches so far.", count);
            }
        }
    }
    Ok(count)
}

// pub async fn get_matches(
//     api: &RiotApi, region: Region, chunk_size: usize, match_ids: Vec<i64>)
//     -> Vec<Match>
// {
//     let mut matches_out = vec![];
//     for match_ids_chunk in match_ids.chunks(chunk_size) {

//         let chunk_futures = match_ids_chunk.into_iter()
//             .map(|match_id| api.match_v4().get_match(region, *match_id))
//             .collect::<Vec<_>>();

//         let matches = join_all(chunk_futures).await;
//         let matches = matches.into_iter()
//             .filter_map(|m| m.ok()) // Remove errors (TODO: silent).
//             .filter_map(|m| m); // Remove 404 (TODO: silent).

//         matches_out.extend(matches);
//     }
//     matches_out
// }
