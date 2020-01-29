use std::time::SystemTime;

use chrono::{ DateTime, NaiveDateTime };
use chrono::offset::Utc;
use chrono::format::{ DelayedFormat, ParseResult };
use chrono::format::strftime::StrftimeItems;

const FORMAT: &'static str = "%Y-%m-%dT%H-%M-%S";

pub fn datetimestamp<'a>() -> DelayedFormat<StrftimeItems<'a>> {
    Utc::now().format(FORMAT)
}

pub fn parse_datetimestamp(datetimestamp: &str) -> chrono::format::ParseResult<DateTime<Utc>> {
    NaiveDateTime::parse_from_str(datetimestamp, FORMAT).map(|ndt| DateTime::<Utc>::from_utc(ndt, Utc))
}

pub fn epoch_millis() -> u64 {
    let dur = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).expect("It's before 1970 lol.");
    dur.as_millis() as u64
}
