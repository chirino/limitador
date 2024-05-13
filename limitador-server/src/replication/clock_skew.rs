use std::time::{Duration, SystemTime};

#[derive(Copy, Clone, Debug)]
pub enum ClockSkew {
    None(),
    Slow(Duration),
    Fast(Duration),
}

impl ClockSkew {
    pub fn new(local: SystemTime, remote: SystemTime) -> ClockSkew {
        if local == remote {
            return ClockSkew::None();
        } else if local.gt(&remote) {
            ClockSkew::Slow(local.duration_since(remote).unwrap())
        } else {
            ClockSkew::Fast(remote.duration_since(local).unwrap())
        }
    }

    // pub fn remote(&self, time: SystemTime) -> SystemTime {
    //     match self {
    //         ClockSkew::None() => time,
    //         ClockSkew::Slow(duration) => time - *duration,
    //         ClockSkew::Fast(duration) => time + *duration,
    //     }
    // }
    //
    // pub fn remote_now(&self) -> SystemTime {
    //     self.remote(SystemTime::now())
    // }

}

impl std::fmt::Display for ClockSkew {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ClockSkew::None() => write!(f, "remote time is the same"),
            ClockSkew::Slow(duration) => write!(f, "remote time is slow by {}ms", duration.as_millis()),
            ClockSkew::Fast(duration) => write!(f, "remote time is fast by {}ms", duration.as_millis()),
        }
    }
}