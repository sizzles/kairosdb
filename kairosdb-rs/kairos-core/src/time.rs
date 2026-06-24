//! Time units, sampling, and range arithmetic.
//!
//! The range math reproduces `org.kairosdb.core.aggregator.RangeAggregator`
//! exactly, including its quirks (e.g. `align_sampling` with an HOURS unit
//! aligns to the start of the *day*, due to the Java switch fallthrough).
//! Calendar math honors a per-query time zone (Java `TimezoneAware`),
//! defaulting to UTC.

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, TimeZone, Timelike};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

pub const UTC: Tz = chrono_tz::UTC;

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

fn in_tz(ts_ms: i64, tz: Tz) -> DateTime<Tz> {
    tz.timestamp_millis_opt(ts_ms).single().unwrap_or_else(|| {
        // DST gaps have no single representation; lean on the latest.
        tz.timestamp_millis_opt(ts_ms)
            .latest()
            .expect("valid ms timestamp")
    })
}

/// Add `n` units to a timestamp, matching Joda `DateTimeField.add` (months
/// clamp the day-of-month: Jan 31 + 1 month = Feb 28).
pub fn add_units(ts_ms: i64, unit: TimeUnit, n: i64) -> i64 {
    add_units_tz(ts_ms, unit, n, UTC)
}

pub fn add_units_tz(ts_ms: i64, unit: TimeUnit, n: i64, tz: Tz) -> i64 {
    if let Some(width) = unit.fixed_millis() {
        return ts_ms + n * width;
    }
    let dt = in_tz(ts_ms, tz);
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
    match tz.from_local_datetime(&date.and_time(time)) {
        chrono::LocalResult::Single(dt) | chrono::LocalResult::Ambiguous(dt, _) => {
            dt.timestamp_millis()
        }
        // The local time falls into a DST gap; shift forward an hour.
        chrono::LocalResult::None => tz
            .from_local_datetime(&(date.and_time(time) + Duration::hours(1)))
            .earliest()
            .expect("valid shifted time")
            .timestamp_millis(),
    }
}

/// Whole units between `subtrahend` and `minuend`, matching Joda
/// `DateTimeField.getDifferenceAsLong`: the largest `d` (toward zero) such
/// that adding `d` units to `subtrahend` does not pass `minuend`.
pub fn unit_difference(minuend_ms: i64, subtrahend_ms: i64, unit: TimeUnit) -> i64 {
    unit_difference_tz(minuend_ms, subtrahend_ms, unit, UTC)
}

pub fn unit_difference_tz(minuend_ms: i64, subtrahend_ms: i64, unit: TimeUnit, tz: Tz) -> i64 {
    if let Some(width) = unit.fixed_millis() {
        return (minuend_ms - subtrahend_ms) / width; // truncates toward zero, like Java
    }
    let dt_min = in_tz(minuend_ms, tz);
    let dt_sub = in_tz(subtrahend_ms, tz);
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
        while add_units_tz(subtrahend_ms, unit, estimate, tz) > minuend_ms {
            estimate -= 1;
        }
    } else {
        while add_units_tz(subtrahend_ms, unit, estimate, tz) < minuend_ms {
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
    align_range_boundary_tz(ts_ms, unit, UTC)
}

pub fn align_range_boundary_tz(ts_ms: i64, unit: TimeUnit, tz: Tz) -> i64 {
    let dt = in_tz(ts_ms, tz);
    let date = dt.date_naive();
    // All field manipulation happens on Naive types, which have no DST gaps,
    // so the .unwrap()s here are infallible (month 1 / day 1 / 00:00:00 always
    // exist). The single local->instant resolution at the end is the only
    // place a DST gap can appear, and resolve_local handles it.
    let naive: NaiveDateTime = match unit {
        TimeUnit::Years => date
            .with_day(1)
            .unwrap()
            .with_month(1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        TimeUnit::Months => date.with_day(1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
        TimeUnit::Weeks => {
            let days_from_monday = i64::from(dt.weekday().num_days_from_monday());
            (date - Duration::days(days_from_monday))
                .and_hms_opt(0, 0, 0)
                .unwrap()
        }
        TimeUnit::Days | TimeUnit::Hours | TimeUnit::Minutes | TimeUnit::Seconds => {
            date.and_hms_opt(0, 0, 0).unwrap()
        }
        TimeUnit::Milliseconds => dt.naive_local().with_nanosecond(0).unwrap(),
    };
    resolve_local(naive, tz).timestamp_millis()
}

/// Resolve a local naive time to an instant, shifting forward out of a DST
/// gap (mirrors Joda's withMillisOfDay(0) behavior and `add_units_tz`).
fn resolve_local(naive: NaiveDateTime, tz: Tz) -> DateTime<Tz> {
    match tz.from_local_datetime(&naive) {
        chrono::LocalResult::Single(d) | chrono::LocalResult::Ambiguous(d, _) => d,
        chrono::LocalResult::None => tz
            .from_local_datetime(&(naive + Duration::hours(1)))
            .earliest()
            .expect("valid shifted local time"),
    }
}

/// Range bucketing, replicating `RangeAggregator.getStartRange`/`getEndRange`.
#[derive(Debug, Clone, Copy)]
pub struct RangeCalc {
    pub start_time_ms: i64,
    pub sampling: Sampling,
    pub tz: Tz,
}

impl RangeCalc {
    /// `start_time_ms` is the query start; pass it through
    /// [`align_range_boundary_tz`] first when `align_sampling` is set.
    pub fn new(start_time_ms: i64, sampling: Sampling) -> Self {
        Self::new_tz(start_time_ms, sampling, UTC)
    }

    pub fn new_tz(start_time_ms: i64, sampling: Sampling, tz: Tz) -> Self {
        RangeCalc { start_time_ms, sampling, tz }
    }

    pub fn start_range(&self, ts_ms: i64) -> i64 {
        let periods = unit_difference_tz(ts_ms, self.start_time_ms, self.sampling.unit, self.tz)
            / self.sampling.value;
        add_units_tz(
            self.start_time_ms,
            self.sampling.unit,
            periods * self.sampling.value,
            self.tz,
        )
    }

    pub fn end_range(&self, ts_ms: i64) -> i64 {
        let periods = unit_difference_tz(ts_ms, self.start_time_ms, self.sampling.unit, self.tz)
            / self.sampling.value;
        add_units_tz(
            self.start_time_ms,
            self.sampling.unit,
            (periods + 1) * self.sampling.value,
            self.tz,
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

    #[test]
    fn align_does_not_panic_in_dst_gap() {
        // São Paulo sprang forward at 2018-11-04 00:00 (midnight does not
        // exist locally). This used to panic on with_hour(0).unwrap().
        let sp = chrono_tz::America::Sao_Paulo;
        // 2018-11-04 12:00 local, an instant on the transition day.
        let ts = sp
            .with_ymd_and_hms(2018, 11, 4, 12, 0, 0)
            .single()
            .unwrap()
            .timestamp_millis();
        for unit in [
            TimeUnit::Days,
            TimeUnit::Hours,
            TimeUnit::Minutes,
            TimeUnit::Weeks,
            TimeUnit::Months,
            TimeUnit::Years,
        ] {
            let aligned = align_range_boundary_tz(ts, unit, sp);
            // Resolves to the first valid instant of the day (01:00 local,
            // since 00:00 was skipped) rather than crashing.
            assert!(aligned <= ts, "{unit:?} boundary must not be after the point");
        }
    }
}
