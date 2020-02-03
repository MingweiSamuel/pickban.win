use serde::{Serialize, Deserialize};
use riven::consts::Tier;

#[derive(Serialize, Deserialize, Debug)]
pub struct Match {
    pub match_id: u64,
    pub rank_tier: Tier,
    pub ts: u64,
}

#[derive(Eq, PartialEq, Ord, PartialOrd)]
pub struct MatchAggKey((u8, u8), chrono::DateTime<chrono::offset::Utc>, Tier);

use chrono::DateTime;
use chrono::naive::NaiveDateTime;
use chrono::offset::Utc;
use riven::models::match_v4;

impl MatchAggKey {
    pub fn from_match_and_tier(matche: &match_v4::Match, tier: Tier) -> Self {
        let version = crate::util::lol::parse_version(&matche.game_version)
            .unwrap_or_else(|| panic!("Failed to parse game version: {}.", matche.game_version));
        let ndt = NaiveDateTime::from_timestamp(
            matche.game_creation / 1000, ((matche.game_creation % 1000) as u32) * 1_000_000);
        let dt = DateTime::<Utc>::from_utc(ndt, Utc);
        Self(version, dt, tier)
    }
}
