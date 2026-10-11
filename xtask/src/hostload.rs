//! Whole-host CPU use around a bench run. The agent's own saturation check only sees its own
//! process; when the client and the target share a machine with other busy processes, a run can
//! miss its rate with a "healthy" client that simply got no CPU. These lines say so.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use sysinfo::{CpuRefreshKind, RefreshKind, System};

/// Host CPU busy share is sampled over windows of this length.
const WINDOW: Duration = Duration::from_secs(2);
/// Busy share (0-100) during the hold from which a missed rate is blamed on the host CPU.
pub const BUSY_WARN_PCT: f32 = 90.0;
/// Busy share before the run (nothing of ours running yet) from which the same is said: with
/// half the host taken by others, client + target cannot both get a fair core count. A heuristic
/// hint on a FAIL only; it never changes the verdict.
pub const BUSY_BEFORE_WARN_PCT: f32 = 45.0;

fn system() -> System {
    System::new_with_specifics(
        RefreshKind::nothing().with_cpu(CpuRefreshKind::nothing().with_cpu_usage()),
    )
}

/// Average host CPU busy share (0-100, all cores) over `d`.
pub fn busy_over(d: Duration) -> f32 {
    let mut s = system();
    s.refresh_cpu_usage();
    std::thread::sleep(d.max(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL));
    s.refresh_cpu_usage();
    s.global_cpu_usage()
}

/// Samples the host CPU in the background until `finish`.
pub struct Sampler {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<Vec<(Duration, f32)>>,
}

impl Sampler {
    pub fn start() -> Sampler {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let handle = std::thread::spawn(move || {
            let t0 = Instant::now();
            let mut s = system();
            s.refresh_cpu_usage();
            let mut out = Vec::new();
            while !flag.load(Ordering::Relaxed) {
                let from = t0.elapsed();
                std::thread::sleep(WINDOW);
                s.refresh_cpu_usage();
                out.push((from, s.global_cpu_usage()));
            }
            out
        });
        Sampler { stop, handle }
    }

    /// Mean busy share of the windows that started at least `skip` after `start` (the ramp).
    pub fn finish(self, skip: Duration) -> Option<f32> {
        self.stop.store(true, Ordering::Relaxed);
        let samples = self.handle.join().ok()?;
        mean_after(&samples, skip)
    }
}

pub fn mean_after(samples: &[(Duration, f32)], skip: Duration) -> Option<f32> {
    let v: Vec<f32> = samples
        .iter()
        .filter(|(at, _)| *at >= skip)
        .map(|(_, p)| *p)
        .collect();
    (!v.is_empty()).then(|| v.iter().sum::<f32>() / v.len() as f32)
}

/// Report lines for the host load before the run and during the hold. `cpu_bound`: the bench
/// missed a target that needs CPU (a rate), so a busy host is named as a likely cause.
pub fn lines(before: f32, during: Option<f32>, cpu_bound: bool) -> Vec<String> {
    let mut l = vec![format!(
        "host CPU busy before the bench: {before:.0}% (other processes)"
    )];
    if let Some(d) = during {
        l.push(format!(
            "host CPU busy during the hold: {d:.0}% (client + target + others)"
        ));
    }
    let busy = before >= BUSY_BEFORE_WARN_PCT || during.is_some_and(|d| d >= BUSY_WARN_PCT);
    if cpu_bound && busy {
        l.push(
            "host CPU limited: the client and the target only got what other processes left; \
             rerun on an idle host or with the target on another machine (--target)"
                .into(),
        );
    }
    l
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mean_skips_the_ramp() {
        let s = |t: u64, p: f32| (Duration::from_secs(t), p);
        let samples = [s(0, 10.0), s(2, 20.0), s(4, 60.0), s(6, 80.0)];
        assert_eq!(mean_after(&samples, Duration::from_secs(4)), Some(70.0));
        assert_eq!(mean_after(&samples, Duration::from_secs(9)), None);
    }

    #[test]
    fn warns_when_the_host_is_busy() {
        assert_eq!(lines(10.0, Some(50.0), true).len(), 2);
        let busy = lines(93.0, Some(100.0), true);
        assert!(busy.iter().any(|l| l.starts_with("host CPU limited")));
        assert!(lines(10.0, Some(95.0), true)
            .iter()
            .any(|l| l.starts_with("host CPU limited")));
        // a miss that is not about CPU (ports, connections held) gets the numbers only
        assert_eq!(lines(93.0, Some(100.0), false).len(), 2);
    }
}
