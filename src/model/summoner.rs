use serde::{Serialize, Deserialize};
use riven::consts::Tier;

#[derive(Serialize, Deserialize, Debug)]
pub struct Summoner {
    pub encrypted_summoner_id: String,
    pub encrypted_account_id:  Option<String>,
    pub league_id: Option<String>,
    pub rank_tier: Option<Tier>,
    pub games_per_day: Option<f32>,
    pub ts: Option<u64>,
}

pub struct SummonerOldest(pub Summoner);
impl Ord for SummonerOldest {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.ts.cmp(&other.0.ts)
        // // Sort by timestamp, then by summoner id (to give some randomness).
        // (self.0.ts, &self.0.encrypted_summoner_id).cmp(&(other.0.ts, &other.0.encrypted_summoner_id))
    }
}
impl PartialOrd for SummonerOldest {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for SummonerOldest {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}
impl Eq for SummonerOldest {}


pub struct SummonerHighestRanked(pub Summoner);
impl Ord for SummonerHighestRanked {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Sort by timestamp, then by summoner id (to give some randomness).
        (self.0.rank_tier, &self.0.league_id).cmp(&(other.0.rank_tier, &other.0.league_id))
    }
}
impl PartialOrd for SummonerHighestRanked {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for SummonerHighestRanked {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}
impl Eq for SummonerHighestRanked {}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_ord() {
        let a = Summoner {
            encrypted_summoner_id: "abc".to_owned(),
            encrypted_account_id:  None,
            league_id: None,
            rank_tier: None,
            games_per_day: None,
            ts: None,
        };
        let b = Summoner {
            encrypted_summoner_id: "abc".to_owned(),
            encrypted_account_id:  None,
            league_id: None,
            rank_tier: None,
            games_per_day: None,
            ts: Some(100),
        };
        assert!(SummonerOldest(a) < SummonerOldest(b));
    }
}