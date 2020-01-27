use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct SummonerModel {
    encrypted_summoner_id: String,
    encrypted_account_id:  String,
    rank: u64,
    league_uuid: String,
    games_per_day: Option<f32>,
    ts: u64,
}

