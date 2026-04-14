use anyhow::{bail, Result};

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
}
