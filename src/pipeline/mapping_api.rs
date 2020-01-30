use std::collections::HashMap;

use chrono::DateTime;
use chrono::offset::Utc;
use futures::future::join_all;
use itertools::Itertools;
use riven::consts::{ Region, Queue };
use riven::models::match_v4::Match;
use riven::RiotApi;

use crate::model::summoner::Summoner;
use crate::util::hybitset::HyBitSet;

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

pub async fn get_new_matchids(
    api: &RiotApi, region: Region, queue: Queue,
    batch_size: usize, starttime: DateTime<Utc>,
    oldest_summoners: &Vec<Summoner>, match_hbs: &mut HyBitSet)
    -> Vec<i64>
{
    // Chunk size? Shitty parallelism?
    let mut new_matches = vec![];
    for summoners_chunk in oldest_summoners.iter().collect::<Vec<_>>().chunks(batch_size) {
        let chunk_futures = summoners_chunk.into_iter()
            .filter(|summoner| summoner.encrypted_account_id.is_some())
            .map(|summoner| {
                let matches_dto = api.match_v4().get_matchlist(
                    region,
                    summoner.encrypted_account_id.as_ref().unwrap(),
                    Some(starttime.timestamp_millis()), // begin_time
                    None, // begin_index
                    None, // champion
                    None, // end_time
                    None, // end_index
                    Some(vec![ queue ]), // queue
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
}

pub async fn get_matches(
    api: &RiotApi, region: Region, chunk_size: usize, match_ids: Vec<i64>)
    -> Vec<Match>
{
    let mut matches_out = vec![];
    for match_ids_chunk in match_ids.chunks(chunk_size) {

        let chunk_futures = match_ids_chunk.into_iter()
            .map(|match_id| api.match_v4().get_match(region, *match_id))
            .collect::<Vec<_>>();

        let matches = join_all(chunk_futures).await;
        let matches = matches.into_iter()
            .map(|m| m.expect("Failed to get matchlist.").expect("Match not found."));

        matches_out.extend(matches);
    }
    matches_out
}