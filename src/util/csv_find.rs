use std::path::PathBuf;

use glob::glob_with;
use glob::MatchOptions;

use riven::consts::Region;

lazy_static! {
    static ref MATCH_OPTIONS: MatchOptions = MatchOptions {
            case_sensitive: false,
            require_literal_separator: false,
            require_literal_leading_dot: false,
    };
}

pub fn find_latest_csvgz(region: Region, name: &str) -> Option<PathBuf> {
    // println!("data/{:?}/{}.*.csv.gz", region, name);

    let mut latest: Option<PathBuf> = None; 
    for entry in glob_with(&format!("data/{:?}/{}.*.csv.gz", region, name), *MATCH_OPTIONS).expect("Bad glob.") {
        let entry = Some(entry.ok()?);
        if entry > latest {
            latest = entry;
        };
    };
    latest
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn test_basic() {
        let out = find_latest_csvgz(Region::NA, "match");
        println!("Result: {:?}", out);
    }
}
