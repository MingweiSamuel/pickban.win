use std::convert::Into;

use riven::consts::Tier;
use riven::consts::IntoEnumIterator;

pub fn match_avg_tier<I: Iterator<Item = Option<Tier>>>(tiers: I) -> Option<Tier> {
    let (sum, cnt) = tiers.filter_map(std::convert::identity)
        .map(|tier| Into::<u8>::into(tier) as u16)
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

pub fn parse_version(version: &str) -> Option<(u8, u8)> {
    let mut split = version.split('.');
    split.next()
        .and_then(|a| a.parse::<u8>().ok())
        .and_then(|a| split.next()
            .and_then(|b| b.parse::<u8>().ok())
            .map(|b| (a, b)))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_round_up() {
        let x = [ Some(&Tier::CHALLENGER), Some(&Tier::GRANDMASTER) ];
        assert_eq!(Some(Tier::CHALLENGER), match_avg_tier(x.iter().cloned()));
    }

    #[test]
    fn test_parse_version() {
        assert_eq!(Some((10, 1)), parse_version("10.1.303.9385"));
        assert_eq!(Some((10, 2)), parse_version("10.2.305.4739"));
        assert_eq!(Some((10, 2)), parse_version("10.2"));
        assert_eq!(None,          parse_version("10."));
        assert_eq!(None,          parse_version(""));
    }
}
