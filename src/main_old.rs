// Dataflow description:
//
//  +----------------------------+              +-------------------------------+
//  | match.na.2020-02-26.csv.gz |              | summoner.na.2020-02-26.csv.gz |
//  +-------------+--------------+              +---------------+---------------+
//                |                                             |
//                V                                             |
//        +-------+-------+                                     V
//        |    matchId    |                           +---------+----------+     +--------------------------+
//        | bitset filter |                           | summoner selection |     | apex tier leagueId fetch |
//        +-------+-------+                           +---------+----------+     +-------------+------------+
//                |                                             |                              |
//                |                +--------------<-------------+----->------+---------<-------+
//                |                |                                         |
//                |                V                                         V
//                |     +----------+-----------+                      +------+------+
//                |     | recent matchId fetch +---+                  | rank  fetch |
//                |     +----------+-----------+   |                  +------+------+
//                |                |               |                         |
//                |     +-----<----+               +-------------->----------+
//                |     |                                                    |
//                V     V                                                    V
//       +--------+-----+--------+                           +---------------+---------------+
//       | filter seen  matchIds |                           |        append / add to        |
//       +-----------+-----------+                           | summoner.na.2020-02-26.csv.gz |
//                   |                                       +-------------------------------+
//                   V
//       +-----------+-----------+           +-------------------+
//       | streaming match fetch |           | stats files .CSVs |
//       +-----------+-----------+           +---------+---------+
//                   |                                 |
//                   +----------------->---------------+
//                   |                                 |
//                   V                                 V
//     +-------------+--------------+      +-----------+-------------+
//     | match.na.2020-02-26.csv.gz |      | streaming stats updates |
//     +----------------------------+      +-------------------------+
//
//
//

#[macro_use]
extern crate lazy_static;

mod util;
mod model;
mod pipeline;

use std::path::PathBuf;

use riven::consts::Region;
use riven::consts::Tier;

use model::summoner::Summoner;

pub fn main() {
    println!("Hello, world!~");

    let path_match = util::csv_find::find_latest_csvgz(Region::NA, "match").expect("Failed to find match .csv.gz");
    let mut match_entries = util::csvgz::reader(path_match).expect("Failed to open match .csv.gz");

    for match_entry in match_entries.deserialize() {
        let match_entry: Summoner = match_entry.expect("Failed to xd");
        println!("{:?}", match_entry);
    }

    // TODO

    let path_match_out: PathBuf = [
        "data",
        &format!("{:?}", Region::NA).to_lowercase(),
        &format!("summoner.{}.csv.gz", util::time::datetimestamp()),
    ].iter().collect();
    
    {
        let mut match_entries_out = util::csvgz::writer(path_match_out).expect("Failed to write xd.");

        match_entries_out.serialize(Summoner {
            encrypted_summoner_id: "asdf,,".to_owned(),
            encrypted_account_id:  "asdf".to_owned(),
            league_uuid: "123-123-123-asdf".to_owned(),
            rank_tier: Tier::GOLD,
            games_per_day: None,
            ts: Some(util::time::epoch_millis()),
        }).unwrap();
        match_entries_out.flush().unwrap();
    }
}