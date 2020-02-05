use serde::{Serialize, Deserialize};
use riven::consts::Tier;

use crate::util::time;

#[derive(Serialize, Deserialize, Debug)]
pub struct Match {
    pub match_id: u64,
    pub rank_tier: Option<Tier>,
    pub ts: u64,
}


use chrono::{ Datelike, DateTime };
use chrono::naive::{ NaiveDateTime, IsoWeek };
use chrono::offset::Utc;
use riven::models::match_v4;

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MatchFileKey {
    pub version: (u8, u8),
    pub iso_week: (i32, u32),
    // pub tier: Tier,
}

impl From<&match_v4::Match> for MatchFileKey {
    fn from(matche: &match_v4::Match) -> Self {
        let version = crate::util::lol::parse_version(&matche.game_version)
            .unwrap_or_else(|| panic!("Failed to parse game version: {}.", matche.game_version));
        let ndt = time::naive_from_millis(matche.game_creation);
        let dt = DateTime::<Utc>::from_utc(ndt, Utc);
        let iw = dt.iso_week();
        Self {
            version: version,
            iso_week: (iw.year(), iw.week()),
            // tier: tier,
        }
    }
}
