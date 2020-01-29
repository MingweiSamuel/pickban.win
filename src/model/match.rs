use serde::{Serialize, Deserialize};
use riven::consts::Tier;

#[derive(Serialize, Deserialize, Debug)]
pub struct Match {
    pub match_id: u64,
    pub rank_tier: Tier,
    pub ts: u64,
}
