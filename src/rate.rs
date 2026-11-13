// SPDX-License-Identifier: LicenseRef-Reticulum
//! The upstream rate limit.
//!
//! rrcd keeps a token bucket per link: `rate_limit_msgs_per_minute` tokens,
//! full when the link is established, refilled continuously, one token per
//! packet whatever its type, and a packet that finds the bucket empty is
//! answered with `ERROR "rate limited"` and dropped (`rrcd/session.py`
//! `refill_and_take`, `router.py:80-91`). The bouncer keeps the same bucket
//! for its one upstream link, a little smaller, and holds a packet back
//! until a token is there instead of losing it.

use std::time::{Duration, Instant};

/// The share of the hub's limit the bouncer uses, so that delivery jitter
/// (two packets sent a token apart arriving closer together) cannot empty
/// the hub's bucket before the bouncer's.
const HEADROOM: f64 = 0.9;

#[derive(Debug, Clone)]
pub struct TokenBucket {
    capacity: f64,
    per_second: f64,
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    /// A full bucket for a hub that allows `per_minute` packets a minute.
    pub fn new(per_minute: u64, now: Instant) -> Self {
        let capacity = Self::capacity_for(per_minute);
        Self {
            capacity,
            per_second: capacity / 60.0,
            tokens: capacity,
            last: now,
        }
    }

    fn capacity_for(per_minute: u64) -> f64 {
        // rrcd treats a limit below 1 as 1.
        (per_minute.max(1) as f64 * HEADROOM).max(1.0)
    }

    /// Change the limit (when the WELCOME arrives), keeping the tokens
    /// already spent spent.
    pub fn set_limit(&mut self, per_minute: u64, now: Instant) {
        self.refill(now);
        self.capacity = Self::capacity_for(per_minute);
        self.per_second = self.capacity / 60.0;
        self.tokens = self.tokens.min(self.capacity);
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.per_second).min(self.capacity);
        self.last = now;
    }

    /// Take a token, or say how long until there is one.
    pub fn take(&mut self, now: Instant) -> Result<(), Duration> {
        self.refill(now);
        // The wait below is rounded to a nanosecond; waiting exactly that
        // long must be enough.
        if self.tokens + 1e-6 >= 1.0 {
            self.tokens = (self.tokens - 1.0).max(0.0);
            Ok(())
        } else {
            Err(Duration::from_secs_f64((1.0 - self.tokens) / self.per_second))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_full_bucket_then_one_token_per_interval() {
        let start = Instant::now();
        // 60 a minute, 54 with the headroom: 0.9 tokens a second.
        let mut bucket = TokenBucket::new(60, start);
        for _ in 0..54 {
            assert!(bucket.take(start).is_ok());
        }
        let wait = bucket.take(start).unwrap_err();
        assert!((wait.as_secs_f64() - 1.0 / 0.9).abs() < 0.01, "{wait:?}");
        assert!(bucket.take(start + wait / 2).is_err());
        assert!(bucket.take(start + wait).is_ok());
        assert!(bucket.take(start + wait).is_err());
    }

    #[test]
    fn it_never_holds_more_than_the_limit() {
        let start = Instant::now();
        let mut bucket = TokenBucket::new(10, start);
        let later = start + Duration::from_secs(3600);
        let taken = (0..100).take_while(|_| bucket.take(later).is_ok()).count();
        assert_eq!(taken, 9);
    }

    #[test]
    fn a_lower_limit_from_the_welcome_applies_at_once() {
        let start = Instant::now();
        let mut bucket = TokenBucket::new(240, start);
        bucket.take(start).unwrap();
        bucket.set_limit(5, start);
        // 4.5 tokens: four packets now, the fifth after half a token's time.
        let taken = (0..10).take_while(|_| bucket.take(start).is_ok()).count();
        assert_eq!(taken, 4);
        // A limit of 0 is taken as 1, as rrcd does.
        bucket.set_limit(0, start);
        assert!(bucket.take(start + Duration::from_secs(67)).is_ok());
    }
}
