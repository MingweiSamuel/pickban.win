use std::path::{ Path, PathBuf };

use glob::glob_with;
use glob::MatchOptions;

lazy_static! {
    static ref MATCH_OPTIONS: MatchOptions = MatchOptions {
            case_sensitive: false,
            require_literal_separator: false,
            require_literal_leading_dot: false,
    };
}

// TODO: really need to distinguish between "no files found" and "it fucked up".
pub fn find_latest(path: impl AsRef<Path>, name: &str, ext: &str) -> Result<Option<PathBuf>, glob::GlobError> {

    let mut latest: Option<PathBuf> = None;
    let pattern = format!("{}/{}.*.{}",
        path.as_ref().to_str().expect("path has unicode"),
        name, ext);

    for entry in glob_with(&pattern, *MATCH_OPTIONS).expect("bad glob") {
        let entry = Some(entry?);
        if entry > latest {
            latest = entry;
        };
    };
    Ok(latest)
}

// pub fn find_after_datetime(path: impl AsRef<Path>, name: &str, ext: &str, starttime: DateTime<Utc>) -> Vec<PathBuf> {
    
//     let mut results: Vec<PathBuf> = vec![];
//     let pattern = format!("{}/{}.*.{}",
//         path.as_ref().to_str().expect("path has unicode"),
//         name, ext);

//     for entry in glob_with(&pattern, *MATCH_OPTIONS).expect("Bad glob.") {
//         if let Ok(entry) = entry {
//             let filename = entry.file_name().expect("No filename.");
//             let filename = filename.to_str().to_owned().expect("Failed to convert filename to string.");
//             let datestr = filename.rsplit(".").nth(2).expect("Missing datetime in filename.");
//             let datetimestamp = util::time::parse_datetimestamp(datestr)
//                 .unwrap_or_else(|e| panic!("Failed to parse datetime in filename: {}", e));
//             if datetimestamp >= starttime {
//                 results.push(entry);
//             }
//         }
//     }
//     results
// }

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn test_basic() {
        let out = find_latest(Region::NA, "match", "tar.gz");
        println!("Result: {:?}", out);
    }

    #[test]
    pub fn test_after_datetime() {
        let out = find_after_datetime(Region::NA, "match", "tar.gz", Utc::now() - Duration::days(3));
        println!("Results: {:?}", out);
    }
}
