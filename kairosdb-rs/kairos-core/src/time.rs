//! Time units, sampling, and range arithmetic.
//!
//! The range math reproduces `org.kairosdb.core.aggregator.RangeAggregator`
//! exactly, including its quirks (e.g. `align_sampling` with an HOURS unit
//! aligns to the start of the *day*, due to the Java switch fallthrough).
//! Calendar math is UTC-only for now; per-query time zones are a TODO.

use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Timelike, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeUnit {
    Milliseconds,
    Seconds,
    Minutes,
    Hours,
    Days,
    Weeks,
    Months,
    Years,
}

impl TimeUnit {
    /// Fixed width in milliseconds for non-calendar units (UTC has no DST).
    fn fixed_millis(self) -> Option<i64> {
        match self {
            TimeUnit::Milliseconds => Some(1),
            TimeUnit::Seconds => Some(1_000),
            TimeUnit::Minutes => Some(60_000),
            TimeUnit::Hours => Some(3_600_000),
            TimeUnit::Days => Some(86_400_000),
            TimeUnit::Weeks => Some(604_800_000),
            TimeUnit::Months | TimeUnit::Years => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Sampling {
    pub value: i64,
    pub unit: TimeUnit,
}

impl Sampling {
    pub fn new(value: i64, unit: TimeUnit) -> Self {
        Sampling { value, unit }
    }
}

fn utc(ts_ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ts_ms).single().expect("valid ms timestamp")
}

/// Add `n` units to a timestamp, matching Joda `DateTimeField.add` (months
/// clamp the day-of-month: Jan 31 + 1 month = Feb 28).
pub fn add_units(ts_ms: i64, unit: TimeUnit, n: i64) -> i64 {
    if let Some(width) = unit.fixed_millis() {
        return ts_ms + n * width;
    }
    let dt = utc(ts_ms);
    let total_months = match unit {
        TimeUnit::Months => i64::from(dt.year()) * 12 + i64::from(dt.month0()) + n,
        TimeUnit::Years => (i64::from(dt.year()) + n) * 12 + i64::from(dt.month0()),
        _ => unreachable!(),
    };
    let year = total_months.div_euclid(12) as i32;
    let month0 = total_months.rem_euclid(12) as u32;
    let last_day = days_in_month(year, month0 + 1);
    let day = dt.day().min(last_day);
    let date = NaiveDate::from_ymd_opt(year, month0 + 1, day).expect("valid date");
    let time = dt.time();
    Utc.from_utc_datetime(&date.and_time(time)).timestamp_millis()
}

/// Whole units between `subtrahend` and `minuend`, matching Joda
/// `DateTimeField.getDifferenceAsLong`: the largest `d` (toward zero) such
/// that adding `d` units to `subtrahend` does not pass `minuend`.
pub fn unit_difference(minuend_ms: i64, subtrahend_ms: i64, unit: TimeUnit) -> i64 {
    if let Some(width) = unit.fixed_millis() {
        return (minuend_ms - subtrahend_ms) / width; // truncates toward zero, like Java
    }
    let dt_min = utc(minuend_ms);
    let dt_sub = utc(subtrahend_ms);
    let mut estimate = match unit {
        TimeUnit::Months => {
            (i64::from(dt_min.year()) * 12 + i64::from(dt_min.month0()))
                - (i64::from(dt_sub.year()) * 12 + i64::from(dt_sub.month0()))
        }
        TimeUnit::Years => i64::from(dt_min.year()) - i64::from(dt_sub.year()),
        _ => unreachable!(),
    };
    // Correct the estimate so add(subtrahend, d) <= minuend < add(subtrahend, d+1)
    // (mirrored for negative differences).
    if minuend_ms >= subtrahend_ms {
        while add_units(subtrahend_ms, unit, estimate) > minuend_ms {
            estimate -= 1;
        }
    } else {
        while add_units(subtrahend_ms, unit, estimate) < minuend_ms {
            estimate += 1;
        }
    }
    estimate
}

fn days_in_month(year: i32, month: u32) -> u32 {
    let (ny, nm) = if month == 12 { (year + 1, 1) } else { (year, month + 1) };
    NaiveDate::from_ymd_opt(ny, nm, 1)
        .expect("valid date")
        .pred_opt()
        .expect("valid date")
        .day()
}

/// Align a range start time to a sampling-unit boundary, replicating
/// `RangeAggregator.alignRangeBoundary` (including the DAYS..SECONDS
/// fallthrough that aligns all of them to start of day).
pub fn align_range_boundary(ts_ms: i64, unit: TimeUnit) -> i64 {
    let dt = utc(ts_ms);
    let dt = match unit {
        TimeUnit::Years => start_of_day(dt.with_month(1).unwrap().with_day(1).unwrap()),
        TimeUnit::Months => start_of_day(dt.with_day(1).unwrap()),
        TimeUnit::Weeks => {
            let days_from_monday = i64::from(dt.weekday().num_days_from_monday());
            start_of_day(dt - Duration::days(days_from_monday))
        }
        TimeUnit::Days | TimeUnit::Hours | TimeUnit::Minutes | TimeUnit::Seconds => {
            start_of_day(dt)
        }
        TimeUnit::Milliseconds => dt.with_nanosecond(0).unwrap(),
    };
    dt.timestamp_millis()
}

fn start_of_day(dt: DateTime<Utc>) -> DateTime<Utc> {
    dt.with_hour(0)
        .unwrap()
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap()
}

/// Range bucketing, replicating `RangeAggregator.getStartRange`/`getEndRange`.
#[derive(Debug, Clone, Copy)]
pub struct RangeCalc {
    pub start_time_ms: i64,
    pub sampling: Sampling,
}

impl RangeCalc {
    /// `start_time_ms` is the query start; pass it through
    /// [`align_range_boundary`] first when `align_sampling` is set.
    pub fn new(start_time_ms: i64, sampling: Sampling) -> Self {
        RangeCalc { start_time_ms, sampling }
    }

    pub fn start_range(&self, ts_ms: i64) -> i64 {
        let periods = unit_difference(ts_ms, self.start_time_ms, self.sampling.unit)
            / self.sampling.value;
        add_units(self.start_time_ms, self.sampling.unit, periods * self.sampling.value)
    }

    pub fn end_range(&self, ts_ms: i64) -> i64 {
        let periods = unit_difference(ts_ms, self.start_time_ms, self.sampling.unit)
            / self.sampling.value;
        add_units(
            self.start_time_ms,
            self.sampling.unit,
            (periods + 1) * self.sampling.value,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = 3_600_000;
    const DAY: i64 = 86_400_000;

    #[test]
    fn fixed_width_ranges() {
        // 5-minute buckets anchored at t=1000
        let calc = RangeCalc::new(1_000, Sampling::new(5, TimeUnit::Minutes));
        assert_eq!(calc.start_range(1_000), 1_000);
        assert_eq!(calc.end_range(1_000), 1_000 + 5 * 60_000);
        assert_eq!(calc.start_range(1_000 + 7 * 60_000), 1_000 + 5 * 60_000);
    }

    #[test]
    fn month_ranges_follow_calendar() {
        // 2026-01-01T00:00:00Z
        let jan1 = 1_767_225_600_000;
        let calc = RangeCalc::new(jan1, Sampling::new(1, TimeUnit::Months));
        // A point on Feb 15 buckets to [Feb 1, Mar 1)
        let feb15 = jan1 + 45 * DAY;
        assert_eq!(calc.start_range(feb15), jan1 + 31 * DAY);
        assert_eq!(calc.end_range(feb15), jan1 + (31 + 28) * DAY);
    }

    #[test]
    fn month_add_clamps_day() {
        // 2026-01-31 + 1 month = 2026-02-28
        let jan31 = 1_767_225_600_000 + 30 * DAY;
        let feb28 = 1_767_225_600_000 + (31 + 27) * DAY;
        assert_eq!(add_units(jan31, TimeUnit::Months, 1), feb28);
    }

    #[test]
    fn align_hours_goes_to_start_of_day() {
        // Java fallthrough: HOURS alignment lands on midnight, not top of hour.
        let ts = 1_767_225_600_000 + 5 * HOUR + 123;
        assert_eq!(align_range_boundary(ts, TimeUnit::Hours), 1_767_225_600_000);
    }

    #[test]
    fn align_week_goes_to_monday() {
        // 2026-01-01 is a Thursday; the preceding Monday is 2025-12-29.
        let thu = 1_767_225_600_000;
        assert_eq!(align_range_boundary(thu, TimeUnit::Weeks), thu - 3 * DAY);
    }

    #[test]
    fn negative_difference_truncates_toward_zero() {
        assert_eq!(unit_difference(-1_500, 0, TimeUnit::Seconds), -1);
        assert_eq!(unit_difference(1_500, 0, TimeUnit::Seconds), 1);
    }
}
