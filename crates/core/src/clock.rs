//! Each sandbox has its own clock so tests can freeze or move time.

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ClockMode {
    /// Wall-clock time.
    Real,
    /// Stuck at a fixed instant (Unix milliseconds).
    Frozen { at_ms: i64 },
    /// Wall-clock time shifted by an offset.
    Offset { offset_ms: i64 },
}

#[derive(Debug)]
pub struct Clock {
    mode: Mutex<ClockMode>,
}

impl Default for Clock {
    fn default() -> Self {
        Self {
            mode: Mutex::new(ClockMode::Real),
        }
    }
}

impl Clock {
    pub fn now_ms(&self) -> i64 {
        match *self.mode.lock().expect("clock lock") {
            ClockMode::Real => wall_ms(),
            ClockMode::Frozen { at_ms } => at_ms,
            ClockMode::Offset { offset_ms } => wall_ms() + offset_ms,
        }
    }

    /// Unix seconds, the `date` of TL objects.
    pub fn now(&self) -> i32 {
        (self.now_ms() / 1000) as i32
    }

    pub fn mode(&self) -> ClockMode {
        *self.mode.lock().expect("clock lock")
    }

    pub fn set_mode(&self, mode: ClockMode) {
        *self.mode.lock().expect("clock lock") = mode;
    }

    /// Move time forward: a frozen clock jumps, a running clock gains an offset.
    pub fn advance(&self, by_ms: i64) {
        let mut mode = self.mode.lock().expect("clock lock");
        *mode = match *mode {
            ClockMode::Real => ClockMode::Offset { offset_ms: by_ms },
            ClockMode::Frozen { at_ms } => ClockMode::Frozen {
                at_ms: at_ms + by_ms,
            },
            ClockMode::Offset { offset_ms } => ClockMode::Offset {
                offset_ms: offset_ms + by_ms,
            },
        };
    }
}

fn wall_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_clock_moves_only_when_advanced() {
        let clock = Clock::default();
        clock.set_mode(ClockMode::Frozen { at_ms: 1_000_000 });
        assert_eq!(clock.now_ms(), 1_000_000);
        clock.advance(5_000);
        assert_eq!(clock.now(), 1_005);
    }
}
