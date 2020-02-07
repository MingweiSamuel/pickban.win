use serde::{Serialize, Deserialize};
use riven::consts::Tier;

#[derive(Serialize, Deserialize, Debug)]
#[derive(PartialOrd, Ord, PartialEq, Eq)]
pub struct League {
    pub tier: Tier,
    pub league_id: String,
}
