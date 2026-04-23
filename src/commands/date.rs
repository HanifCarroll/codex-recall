use anyhow::{bail, Context, Result};
use rusqlite::{params_from_iter, Connection};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedSince {
    pub value: String,
    pub timestamp_key: String,
    pub date: CalendarDate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CalendarDate {
    pub year: i32,
    pub month: u32,
    pub day: u32,
}

impl CalendarDate {
    pub fn parse(value: &str) -> Result<Self> {
        let (year, month, day) = parse_day(value)?;
        Ok(Self {
            year: year as i32,
            month,
            day,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DateFilter {
    Absolute(String),
    LastDays(u32),
    Today,
    Yesterday,
}

pub fn resolve_date_window(
    since: Option<String>,
    from: Option<String>,
    until: Option<String>,
    day: Option<String>,
) -> Result<(Option<String>, Option<String>, Option<String>)> {
    let Some(day) = day else {
        return Ok((since, from, until));
    };

    if since.is_some() || from.is_some() || until.is_some() {
        bail!("use --day by itself; do not combine it with --since, --from, or --until");
    }

    let next_day = next_calendar_day(&day)?;
    Ok((None, Some(day), Some(next_day)))
}

fn next_calendar_day(day: &str) -> Result<String> {
    let (year, month, day_of_month) = parse_day(day)?;
    let days_this_month = days_in_month(year, month)?;
    let (year, month, day_of_month) = if day_of_month < days_this_month {
        (year, month, day_of_month + 1)
    } else if month < 12 {
        (year, month + 1, 1)
    } else {
        (year + 1, 1, 1)
    };

    Ok(format!("{year:04}-{month:02}-{day_of_month:02}"))
}

pub(crate) fn parse_date_filter(value: &str, flag_name: &str) -> Result<DateFilter> {
    let trimmed = value.trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower == "today" {
        return Ok(DateFilter::Today);
    }
    if lower == "yesterday" {
        return Ok(DateFilter::Yesterday);
    }
    if let Some(days) = lower.strip_suffix('d') {
        let days = days
            .parse::<u32>()
            .with_context(|| format!("parse {flag_name} relative day value `{value}`"))?;
        if days == 0 {
            return Ok(DateFilter::Today);
        }
        return Ok(DateFilter::LastDays(days));
    }
    if looks_like_absolute_date(trimmed) {
        return Ok(DateFilter::Absolute(trimmed.to_owned()));
    }

    bail!(
        "unsupported {flag_name} value `{value}`; use YYYY-MM-DD, today, yesterday, or Nd like 7d"
    )
}

pub(crate) fn resolve_since(value: &str) -> Result<ResolvedSince> {
    let filter = parse_date_filter(value, "--since")?;
    let timestamp = match filter {
        DateFilter::Absolute(value) => sqlite_datetime("datetime(?)", &[value.as_str()])?,
        DateFilter::LastDays(days) => {
            let modifier = format!("-{days} days");
            sqlite_datetime("datetime('now', ?)", &[modifier.as_str()])?
        }
        DateFilter::Today => {
            sqlite_datetime("datetime('now', 'localtime', 'start of day', 'utc')", &[])?
        }
        DateFilter::Yesterday => sqlite_datetime(
            "datetime('now', 'localtime', 'start of day', '-1 day', 'utc')",
            &[],
        )?,
    };
    let timestamp_key = timestamp_key(&timestamp)
        .ok_or_else(|| anyhow::anyhow!("could not resolve --since value `{value}`"))?;
    let date = CalendarDate::parse(&timestamp_key[..10])?;

    Ok(ResolvedSince {
        value: value.to_owned(),
        timestamp_key,
        date,
    })
}

fn sqlite_datetime(expr: &str, params: &[&str]) -> Result<String> {
    let conn = Connection::open_in_memory()?;
    let sql = format!("SELECT strftime('%Y-%m-%dT%H:%M:%SZ', {expr})");
    let mut stmt = conn.prepare(&sql)?;
    let timestamp: Option<String> =
        stmt.query_row(params_from_iter(params.iter()), |row| row.get(0))?;
    timestamp.ok_or_else(|| anyhow::anyhow!("could not resolve date expression"))
}

pub(crate) fn timestamp_key(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.len() >= 19 {
        let bytes = trimmed.as_bytes();
        if bytes[0..4].iter().all(|byte| byte.is_ascii_digit())
            && bytes[4] == b'-'
            && bytes[5..7].iter().all(|byte| byte.is_ascii_digit())
            && bytes[7] == b'-'
            && bytes[8..10].iter().all(|byte| byte.is_ascii_digit())
            && (bytes[10] == b'T' || bytes[10] == b' ')
            && bytes[11..13].iter().all(|byte| byte.is_ascii_digit())
            && bytes[13] == b':'
            && bytes[14..16].iter().all(|byte| byte.is_ascii_digit())
            && bytes[16] == b':'
            && bytes[17..19].iter().all(|byte| byte.is_ascii_digit())
        {
            let mut key = trimmed[..19].to_owned();
            key.replace_range(10..11, "T");
            return Some(key);
        }
    }
    if looks_like_absolute_date(trimmed) {
        return Some(format!("{}T00:00:00", &trimmed[..10]));
    }
    None
}

fn parse_day(day: &str) -> Result<(u32, u32, u32)> {
    let parts = day.split('-').collect::<Vec<_>>();
    if parts.len() != 3 {
        bail!("unsupported --day value `{day}`; use YYYY-MM-DD");
    }

    let year = parts[0]
        .parse::<u32>()
        .map_err(|_| anyhow::anyhow!("unsupported --day value `{day}`; use YYYY-MM-DD"))?;
    let month = parts[1]
        .parse::<u32>()
        .map_err(|_| anyhow::anyhow!("unsupported --day value `{day}`; use YYYY-MM-DD"))?;
    let day_of_month = parts[2]
        .parse::<u32>()
        .map_err(|_| anyhow::anyhow!("unsupported --day value `{day}`; use YYYY-MM-DD"))?;

    let max_day = days_in_month(year, month)?;
    if day_of_month == 0 || day_of_month > max_day {
        bail!("unsupported --day value `{day}`; use YYYY-MM-DD");
    }

    Ok((year, month, day_of_month))
}

fn days_in_month(year: u32, month: u32) -> Result<u32> {
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => bail!("unsupported month `{month}` in --day value"),
    };
    Ok(days)
}

pub(crate) fn looks_like_absolute_date(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 10
        && bytes[0..4].iter().all(|byte| byte.is_ascii_digit())
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(|byte| byte.is_ascii_digit())
        && bytes[7] == b'-'
        && bytes[8..10].iter().all(|byte| byte.is_ascii_digit())
}

fn is_leap_year(year: u32) -> bool {
    year.is_multiple_of(4) && !year.is_multiple_of(100) || year.is_multiple_of(400)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_calendar_day_handles_month_year_and_leap_boundaries() {
        assert_eq!(next_calendar_day("2026-04-13").unwrap(), "2026-04-14");
        assert_eq!(next_calendar_day("2026-04-30").unwrap(), "2026-05-01");
        assert_eq!(next_calendar_day("2026-12-31").unwrap(), "2027-01-01");
        assert_eq!(next_calendar_day("2024-02-28").unwrap(), "2024-02-29");
        assert_eq!(next_calendar_day("2024-02-29").unwrap(), "2024-03-01");
    }

    #[test]
    fn resolves_absolute_since_to_comparable_timestamp_key_and_date() {
        let since = resolve_since("2026-04-13").unwrap();
        assert_eq!(since.timestamp_key, "2026-04-13T00:00:00");
        assert_eq!(
            since.date,
            CalendarDate {
                year: 2026,
                month: 4,
                day: 13
            }
        );
    }

    #[test]
    fn timestamp_key_ignores_fractional_seconds_for_string_comparison() {
        assert_eq!(
            timestamp_key("2026-04-13T01:02:03.456Z").as_deref(),
            Some("2026-04-13T01:02:03")
        );
        assert_eq!(
            timestamp_key("2026-04-13 01:02:03").as_deref(),
            Some("2026-04-13T01:02:03")
        );
    }
}
