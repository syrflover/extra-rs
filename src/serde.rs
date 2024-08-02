use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serializer;

pub fn to_rfc3339_ms<S>(datetime: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let r = datetime.to_rfc3339_opts(SecondsFormat::Millis, true);

    serializer.serialize_str(&r)
}
