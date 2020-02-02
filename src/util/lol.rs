use std::convert::Into;

use riven::consts::Tier;
use riven::consts::IntoEnumIterator;

pub fn match_avg_tier<'a, I: Iterator<Item = Option<&'a Tier>>>(tiers: I) -> Option<Tier> {
    let (sum, cnt) = tiers.filter_map(std::convert::identity)
        .map(|tier| Into::<u8>::into(*tier) as u16)
        .fold((0_u16, 0_u16), |(sum, cnt), x| (sum + x, cnt + 1));
    if 0 == cnt {
        return None;
    }
    let tier_avg = ((sum as f32) / (cnt as f32)) as u8; 
    let (_dist, tier_nearest) = Tier::iter().fold((u8::max_value(), Tier::CHALLENGER), |acc, tier| {
        let dist = crate::distance(tier_avg, Into::<u8>::into(tier));
        if dist < acc.0 {
            (dist, tier)
        }
        else {
            acc
        }
    });
    Some(tier_nearest)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_round_up() {
        let x = [ Some(&Tier::CHALLENGER), Some(&Tier::GRANDMASTER) ];
        assert_eq!(Some(Tier::CHALLENGER), match_avg_tier(x.iter().cloned()));
    }
}
