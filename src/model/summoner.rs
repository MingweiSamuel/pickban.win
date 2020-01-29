use serde::{Serialize, Deserialize};
use riven::consts::Tier;

#[derive(Serialize, Deserialize, Debug)]
pub struct Summoner {
    pub encrypted_summoner_id: String,
    pub encrypted_account_id:  String, // Option<String>
    pub league_id: String,
    pub rank_tier: Tier,
    pub games_per_day: Option<f32>,
    pub ts: Option<u64>,
}

pub struct SummonerOldest(pub Summoner);

impl Ord for SummonerOldest {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.ts.cmp(&other.0.ts)
    }
}
impl PartialOrd for SummonerOldest {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for SummonerOldest {
    fn eq(&self, other: &Self) -> bool {
        self.0.ts == other.0.ts
    }
}
impl Eq for SummonerOldest {}

