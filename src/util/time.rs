use std::time::SystemTime;

use chrono::{ DateTime, NaiveDateTime };
use chrono::offset::Utc;
use chrono::format::{ DelayedFormat, ParseResult };
use chrono::format::strftime::StrftimeItems;

const FORMAT: &'static str = "%Y-%m-%dT%H-%M-%S";

#[allow(dead_code)]
pub fn datetimestamp<'a>() -> DelayedFormat<StrftimeItems<'a>> {
    Utc::now().format(FORMAT)
}

#[allow(dead_code)]
pub fn parse_datetimestamp(datetimestamp: &str) -> ParseResult<DateTime<Utc>> {
    NaiveDateTime::parse_from_str(datetimestamp, FORMAT).map(|ndt| DateTime::<Utc>::from_utc(ndt, Utc))
}

#[allow(dead_code)]
pub fn epoch_millis() -> u64 {
    let dur = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).expect("It's before 1970 lol.");
    dur.as_millis() as u64
}

pub fn naive_from_millis(millis: i64) -> NaiveDateTime {
    NaiveDateTime::from_timestamp(millis / 1000, ((millis % 1000) as u32) * 1_000_000)
}