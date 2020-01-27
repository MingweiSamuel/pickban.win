use std::time::SystemTime;

use chrono::offset::Utc;
use chrono::format::DelayedFormat;
use chrono::format::strftime::StrftimeItems;

pub fn datetimestamp<'a>() -> DelayedFormat<StrftimeItems<'a>> {
    Utc::now().format("%Y-%m-%dT%H-%M-%S")
}

pub fn epoch_millis() -> u64 {
    let dur = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).expect("It's before 1970 lol.");
    dur.as_millis() as u64
}
