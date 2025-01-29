use std::fmt::{Debug, Formatter};
use std::time::{Duration, SystemTime};

/// DateTime in litedb bson
///
/// This can represent same value as C# [DateTime].
///
/// This represents number of 100 nano seconds since 0001-01-01 00:00:00 UTC
/// This can represent 0001-01-01 00:00:00 ~ 9999-12-31 23:59:59.99999999
///
/// [DateTime]: https://learn.microsoft.com/en-us/dotnet/api/system.datetime
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct DateTime(u64);

const MAX_TICKS: u64 = 3155378975999999999;
/// The tick represents unix epoc time. Will be used for time conversation
const TICKS_UNIX_EPOC: u64 = 621355968000000000;
const TICKS_AFTER_UNIX_EPOC: u64 = MAX_TICKS - TICKS_UNIX_EPOC;

const NANOS_PER_TICK: u128 = 100;
const SECONDS_PER_MINUTE: u64 = 60;
const MINUTES_PER_HOUR: u64 = 60;
const HOURS_PER_DAY: u64 = 24;

const DAYS_PER_NORMAL_YEAR: u32 = 365;
const DAYS_PER_4_YEAR: u32 = DAYS_PER_NORMAL_YEAR * 4 + 1; // there is one leap year per 4 year
const DAYS_PER_NORMAL_100_YEAR: u32 = DAYS_PER_4_YEAR * 25 - 1; // there is one missing leap year per 100 year
const DAYS_PER_400_YEAR: u32 = DAYS_PER_NORMAL_100_YEAR * 4 + 1; // there is one extra leap year per 400 year.

const TICKS_PER_MILLISECOND: u64 = 10_000;
const TICKS_PER_SECOND: u64 = 10_000_000;

impl DateTime {
    /// The Minimum value of DateTime.
    /// This represents 0001-01-01T00:00:00.00000000 UTC in Proleptic Gregorian Calender.
    pub const MIN: DateTime = DateTime(0);
    /// The Maximum value of DateTime.
    /// This represents 9999-12-31 23:59:59.99999999 UTC in Proleptic Gregorian Calender.
    pub const MAX: DateTime = DateTime(MAX_TICKS);

    /// Create new DateTime represents now
    pub fn now() -> Self {
        // current time very unlikey to not exceed MAX_TICKS / MIN_TICKS so unwrap here.
        Self::from_system(SystemTime::now()).unwrap()
    }

    /// Creates new DateTime represents exactly the same time as the [`SystemTime`]
    ///
    /// Precision smaller than 100 nanoseconds will be discarded.
    ///
    /// If the time cannot be represented with this type, like before 0001 year or after 9999 year,
    /// this function will return `None`.
    pub fn from_system(system: SystemTime) -> Option<Self> {
        let ticks = match system.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(duration) => {
                // the time is after unix epoc
                let nanos_since_epoc = duration.as_nanos();
                let ticks_since_epoc = nanos_since_epoc / NANOS_PER_TICK;

                if ticks_since_epoc > TICKS_AFTER_UNIX_EPOC as u128 {
                    return None;
                }

                TICKS_UNIX_EPOC + ticks_since_epoc as u64
            }
            Err(e) => {
                let duration = e.duration();

                // use div_ceil
                let nanos_until_epoc = duration.as_nanos();
                let ticks_until_epoc = nanos_until_epoc.div_ceil(NANOS_PER_TICK);

                if ticks_until_epoc > TICKS_UNIX_EPOC as u128 {
                    return None;
                }

                TICKS_UNIX_EPOC - ticks_until_epoc as u64
            }
        };
        Some(DateTime(ticks))
    }

    /// Create new DateTime from ticks
    ///
    /// If the tick is larger than [Self::MAX], returns `None`.
    pub const fn from_ticks(ticks: u64) -> Option<DateTime> {
        if ticks > MAX_TICKS {
            None
        } else {
            Some(DateTime(ticks))
        }
    }

    /// Get the total ticks since 0001-01-01 00:00:00
    ///
    /// One tick is 100 nanoseconds
    pub fn ticks(&self) -> u64 {
        self.0
    }

    /// Get the SystemTime that represents the same time as this `DateTime`
    ///
    /// If the time cannot be represented with `SystemTime`, this will return `None`.
    pub fn to_system_time(&self) -> Option<SystemTime> {
        let ticks_since_epoc = self.ticks() as i64 - TICKS_UNIX_EPOC as i64;

        if ticks_since_epoc < 0 {
            // time is before unix epoc.
            let ticks_until_epoc = -ticks_since_epoc as u64;
            let secs_until_epoc = ticks_until_epoc / TICKS_PER_SECOND;
            let sub_nano = ticks_until_epoc % TICKS_PER_SECOND;
            let dur_until_epoc = Duration::new(secs_until_epoc, sub_nano as u32);
            SystemTime::UNIX_EPOCH.checked_sub(dur_until_epoc)
        } else {
            // time is after unix epoc.
            let ticks_since_epoc = ticks_since_epoc as u64;
            let secs_since_ticks = ticks_since_epoc / TICKS_PER_SECOND;
            let sub_nano = ticks_since_epoc % TICKS_PER_SECOND;
            let dur_since_epoc = Duration::new(secs_since_ticks, sub_nano as u32);
            SystemTime::UNIX_EPOCH.checked_add(dur_since_epoc)
        }
    }

    pub(crate) fn from_unix_milliseconds(unix_milliseconds: i64) -> Option<DateTime> {
        let total_ticks = unix_milliseconds.checked_mul(TICKS_PER_MILLISECOND as i64)?;
        let total_ticks = u64::try_from(total_ticks).ok()?;
        let total_ticks = total_ticks.checked_add(TICKS_UNIX_EPOC)?;
        DateTime::from_ticks(total_ticks)
    }

    pub(crate) fn as_unix_milliseconds(&self) -> i64 {
        let millis = (self.ticks() / TICKS_PER_MILLISECOND) as i64;
        let unix_epoc = (TICKS_UNIX_EPOC / TICKS_PER_MILLISECOND) as i64;

        millis - unix_epoc
    }

    pub const fn parse_rfc3339(s: &str) -> Option<DateTime> {
        let bytes = s.as_bytes();
        if bytes.len() < 19 {
            return None;
        }

        macro_rules! slice {
            ($bytes: ident, $start: literal, $end: literal) => {{
                const LEN: usize = $end - $start + 1;
                let mut result = [0u8; LEN];
                let mut i = 0;
                while i < LEN {
                    result[i] = $bytes[$start + i];
                    i += 1;
                }
                result
            }};
        }

        let year_part = slice!(bytes, 0, 3);
        let hyphen0 = bytes[4];
        let month_part = slice!(bytes, 5, 6);
        let hyphen1 = bytes[7];
        let day_part = slice!(bytes, 8, 9);
        let t = bytes[10];
        let hour_part = slice!(bytes, 11, 12);
        let colon0 = bytes[13];
        let minute_part = slice!(bytes, 14, 15);
        let colon1 = bytes[16];
        let second_part = slice!(bytes, 17, 18);

        if hyphen0 != b'-' || hyphen1 != b'-' || t != b'T' || colon0 != b':' || colon1 != b':' {
            return None;
        }

        macro_rules! parse_u64 {
            ($bytes: expr) => {{
                let Ok(s) = std::str::from_utf8($bytes) else {
                    return None;
                };
                let Ok(d) = u64::from_str_radix(s, 10) else {
                    return None;
                };
                d
            }};
        }

        let year = parse_u64!(&year_part);
        let month = parse_u64!(&month_part);
        let day = parse_u64!(&day_part);
        let hour = parse_u64!(&hour_part);
        let minute = parse_u64!(&minute_part);
        let second = parse_u64!(&second_part);

        if year < 1 || year > 9999 {
            return None;
        }
        if month < 1 || month > 12 {
            return None;
        }

        let is_leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let max_days = if is_leap {
            [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        } else {
            [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        };
        if day < 1 || day > max_days[(month - 1) as usize] {
            return None;
        }

        if hour > 23 {
            return None;
        }
        if minute > 59 {
            return None;
        }
        if second > 59 {
            return None;
        }

        let ticks = if bytes.len() == 19 {
            0
        } else {
            // .XXXXX
            if bytes.len() < 21 {
                return None;
            }
            if bytes[19] != b'.' {
                return None;
            }
            //let subsec_part = &bytes[21..];
            let (_, subsec_part) = bytes.split_at(20);
            let mut number_part = *b"0000000";
            if subsec_part.len() > number_part.len() {
                return None; // we cannot expres the time
            }

            //number_part[..subsec_part.len()].copy_from_slice(subsec_part);
            let mut i = 0;
            while i < subsec_part.len() {
                number_part[i] = subsec_part[i];
                i += 1;
            }

            let subsec = parse_u64!(&subsec_part);
            debug_assert!(subsec < TICKS_PER_SECOND);
            subsec
        };

        let in_day_seconds = hour * (60 * 60) + minute * 60 + second;
        let in_day_ticks = in_day_seconds * TICKS_PER_SECOND;

        let days = {
            // leap years are last of 400/100/4 year0
            let year0 = year - 1;
            let number_of_400_years = year0 / 400;
            let years_in_400_years = year0 % 400;
            let number_of_100_years = years_in_400_years / 100;
            let years_in_100_years = years_in_400_years % 100;
            let number_of_4_years = years_in_100_years / 4;
            let years_in_4_years = years_in_100_years % 4;

            let year_days = number_of_400_years * DAYS_PER_400_YEAR as u64
                + number_of_100_years * DAYS_PER_NORMAL_100_YEAR as u64
                + number_of_4_years * DAYS_PER_4_YEAR as u64
                + years_in_4_years * DAYS_PER_NORMAL_YEAR as u64;

            let month_start = if is_leap {
                &[0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
            } else {
                &[0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
            };

            year_days + month_start[(month - 1) as usize] + (day - 1)
        };
        let day_ticks = days * (24 * 60 * 60 * TICKS_PER_SECOND);

        DateTime::from_ticks(day_ticks + in_day_ticks + ticks)
    }
}

/// The `Debug` for `DateTime` will show time in ISO 8601 extended format
impl Debug for DateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let ticks = self.ticks();

        let sub_ticks = ticks % TICKS_PER_SECOND;
        let total_seconds = ticks / TICKS_PER_SECOND;

        let seconds = total_seconds % SECONDS_PER_MINUTE;
        let total_minutes = total_seconds / SECONDS_PER_MINUTE;

        let minutes = total_minutes % MINUTES_PER_HOUR;
        let total_hours = total_minutes / MINUTES_PER_HOUR;

        let hours = total_hours % HOURS_PER_DAY;
        let total_days = total_hours / HOURS_PER_DAY;

        let (year, is_leap, days_in_year) = days_to_year_and_day_in_year(total_days as u32);
        let (month, day) = day_in_year_to_month_day(days_in_year, is_leap);

        return write!(
            f,
            "{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}.{sub_ticks:07}"
        );

        fn days_to_year_and_day_in_year(days: u32) -> (u32, bool, u32) {
            let number_of_400_years = days / DAYS_PER_400_YEAR;
            let days_in_400_years = days % DAYS_PER_400_YEAR;

            let mut number_of_100_years = days_in_400_years / DAYS_PER_NORMAL_100_YEAR;
            // last 100 year period has one extra day so decrement if 4
            if number_of_100_years == 4 {
                number_of_100_years = 3;
            }
            let days_in_100_year =
                days_in_400_years - number_of_100_years * DAYS_PER_NORMAL_100_YEAR;

            let number_of_4_year = days_in_100_year / DAYS_PER_4_YEAR;
            let days_in_4_year = days_in_100_year % DAYS_PER_4_YEAR;

            let mut number_of_year = days_in_4_year / DAYS_PER_NORMAL_YEAR;
            if number_of_year == 4 {
                number_of_year = 3;
            }
            let days_in_year = days_in_4_year - number_of_year * DAYS_PER_NORMAL_YEAR;

            let year = number_of_400_years * 400
                + number_of_100_years * 100
                + number_of_4_year * 4
                + number_of_year
                + 1;
            // since it's 0-indexed instead of 1-indexed, we have different repr
            let is_leap =
                number_of_year == 3 && (number_of_4_year != 24 || number_of_100_years == 3);

            (year, is_leap, days_in_year)
        }

        fn day_in_year_to_month_day(days: u32, leap: bool) -> (u32, u32) {
            let mut estimated = days / 32 + 1; // all month has < 32 days per month
            let days_to_month = if leap {
                &[0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366]
            } else {
                &[0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365]
            };

            while days >= days_to_month[estimated as usize] {
                estimated += 1;
            }

            let day = days - days_to_month[estimated as usize - 1] + 1; // +1 for one-based

            (estimated, day)
        }
    }
}

#[test]
fn test_debug() {
    macro_rules! time {
        ($($tt: tt)*) => {
            DateTime::from_system(SystemTime::from(::time::macros::datetime!($($tt)*))).unwrap()
        };
    }

    assert_eq!(
        format!("{:?}", time!(0001-01-01 00:00:00.0000000 UTC)),
        "0001-01-01T00:00:00.0000000"
    );
    assert_eq!(
        format!("{:?}", time!(0001-01-01 00:00:00.0000001 UTC)),
        "0001-01-01T00:00:00.0000001"
    );

    // first leap year
    assert_eq!(
        format!("{:?}", time!(0004-02-28 12:34:56.7890001 UTC)),
        "0004-02-28T12:34:56.7890001"
    );
    assert_eq!(
        format!("{:?}", time!(0004-02-29 12:34:56.7890001 UTC)),
        "0004-02-29T12:34:56.7890001"
    );
    assert_eq!(
        format!("{:?}", time!(0004-03-01 12:34:56.7890001 UTC)),
        "0004-03-01T12:34:56.7890001"
    );

    // multiple of 100
    assert_eq!(
        format!("{:?}", time!(1900-02-28 12:34:56.7890001 UTC)),
        "1900-02-28T12:34:56.7890001"
    );
    //assert_eq!(format!("{:?}", time!(1900-02-29 12:34:56.7890001 UTC)), "1900-02-29T12:34:56.7890001");
    assert_eq!(
        format!("{:?}", time!(1900-03-01 12:34:56.7890001 UTC)),
        "1900-03-01T12:34:56.7890001"
    );

    // multiple of 400
    assert_eq!(
        format!("{:?}", time!(2000-02-28 12:34:56.7890001 UTC)),
        "2000-02-28T12:34:56.7890001"
    );
    assert_eq!(
        format!("{:?}", time!(2000-02-29 12:34:56.7890001 UTC)),
        "2000-02-29T12:34:56.7890001"
    );
    assert_eq!(
        format!("{:?}", time!(2000-03-01 12:34:56.7890001 UTC)),
        "2000-03-01T12:34:56.7890001"
    );

    // today as of writing
    assert_eq!(
        format!("{:?}", time!(2025-01-25 11:26:54.1234567 UTC)),
        "2025-01-25T11:26:54.1234567"
    );

    assert_eq!(
        format!("{:?}", DateTime::MIN),
        "0001-01-01T00:00:00.0000000"
    );
    assert_eq!(
        format!("{:?}", DateTime::MAX),
        "9999-12-31T23:59:59.9999999"
    );

    // conversation test
    // nanoseconds before Unix Epoc
    assert_eq!(
        format!("{:?}", time!(0001-01-01 00:00:00.000000000 UTC)),
        "0001-01-01T00:00:00.0000000"
    );
    assert_eq!(
        format!("{:?}", time!(0001-01-01 00:00:00.000000100 UTC)),
        "0001-01-01T00:00:00.0000001"
    );
    assert_eq!(
        format!("{:?}", time!(0001-01-01 00:00:00.000000199 UTC)),
        "0001-01-01T00:00:00.0000001"
    );
    // nanoseconds after unix epoc
    assert_eq!(
        format!("{:?}", time!(2000-01-01 00:00:00.000000000 UTC)),
        "2000-01-01T00:00:00.0000000"
    );
    assert_eq!(
        format!("{:?}", time!(2000-01-01 00:00:00.000000100 UTC)),
        "2000-01-01T00:00:00.0000001"
    );
    assert_eq!(
        format!("{:?}", time!(2000-01-01 00:00:00.000000199 UTC)),
        "2000-01-01T00:00:00.0000001"
    );
}

#[test]
fn test_parse() {
    //const { panic!(concat!("test", stringify!(010))) };
    assert_eq!(
        DateTime::parse_rfc3339("2020-05-06T09:29:10.8350000")
            .unwrap()
            .ticks(),
        637243541508350000
    );
    assert_eq!(
        DateTime::parse_rfc3339("1999-05-06T09:29:10.8350000")
            .unwrap()
            .ticks(),
        630615797508350000
    );
}
