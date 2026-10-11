use anyhow::{bail, Context, Result};
use loadfleet_engine::{run_local, short_id, CancellationToken, LocalOpts};
use loadfleet_metrics::{to_csv, to_json, to_markdown, SeriesPoint};
use loadfleet_plan::{AllowList, Limits, Plan};
use loadfleet_scenario::{build, check_text, prepare_plan, AllowGuard, BuildCtx};
use std::path::{Path, PathBuf};

pub struct RunArgs {
    pub plan: PathBuf,
    pub out: Option<PathBuf>,
    pub allow: Vec<String>,
    pub quiet: bool,
}

/// Runs a plan locally and writes `report.json`, `report.md`, `series.csv`. Returns the process
/// exit code: 0 completed, 2 stopped by a rule, 130 interrupted, 1 failed.
pub async fn run(a: RunArgs) -> Result<i32> {
    let text =
        std::fs::read_to_string(&a.plan).with_context(|| format!("read {}", a.plan.display()))?;
    let mut plan = match check_text(&text, &Limits::default()) {
        Ok(p) => p,
        Err(errs) => {
            for e in errs {
                eprintln!("plan error: {e}");
            }
            bail!("invalid plan");
        }
    };
    prepare_plan(&mut plan, a.plan.parent().unwrap_or(Path::new(".")))
        .map_err(anyhow::Error::msg)?;
    let local = if a.allow.is_empty() {
        None
    } else {
        Some(AllowList::parse(&a.allow).map_err(anyhow::Error::msg)?)
    };
    // A local run has no server: plan ∩ --allow.
    let guard = AllowGuard::layered(&plan.target.allow, None, local.as_ref())
        .map_err(anyhow::Error::msg)?;
    check_endpoints(&plan, &guard)?;
    let run_id = short_id();
    let max_vus = plan.load.max_vus.unwrap_or(1000);
    let scenario = build(
        &plan.scenario,
        BuildCtx {
            guard,
            run_id: run_id.clone(),
            agent: "local".into(),
            max_vus,
        },
    )
    .map_err(anyhow::Error::msg)?;
    let fds = loadfleet_health::raise_fd_limit();
    if !a.quiet {
        eprintln!("loadfleet run {} ({run_id}), fd limit {fds}", plan.name);
    }
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    let shutdown = crate::signal::shutdown();
    tokio::spawn(async move {
        shutdown.await;
        c.cancel();
    });
    let quiet = a.quiet;
    let opts = LocalOpts {
        run_id,
        agent: "local".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        on_second: Some(Box::new(move |p| {
            if !quiet {
                eprintln!("{}", progress_line(p));
            }
        })),
    };
    // `run_local` takes the only handle to the scenario and drops it (closing pooled connections).
    let report = run_local(&plan, scenario, opts, cancel).await;
    let out = a.out.unwrap_or_else(|| default_out(&plan.name));
    std::fs::create_dir_all(&out).with_context(|| format!("create {}", out.display()))?;
    std::fs::write(out.join("report.json"), to_json(&report))?;
    std::fs::write(
        out.join("report.md"),
        to_markdown(&report, &plan.exclude_stages),
    )?;
    std::fs::write(out.join("series.csv"), to_csv(&report))?;
    eprintln!(
        "{}\nreport: {}",
        report.meta.verdict,
        out.join("report.md").display()
    );
    Ok(exit_code(&report.meta.status))
}

/// Every endpoint the plan connects to must survive the `--allow` narrowing; refusing up front
/// beats a run in which every connect fails with `blocked:*`.
fn check_endpoints(plan: &Plan, guard: &AllowGuard) -> Result<()> {
    for (field, endpoint) in plan.scenario.endpoints() {
        let host = AllowList::host_of(&endpoint).map_err(anyhow::Error::msg)?;
        if guard.check(&host).is_err() {
            bail!("{field}: host {host} is not permitted by --allow");
        }
    }
    Ok(())
}

fn exit_code(status: &str) -> i32 {
    match status {
        "completed" => 0,
        "stopped" => 2,
        "interrupted" => 130,
        _ => 1,
    }
}

fn default_out(name: &str) -> PathBuf {
    let ts = humantime::format_rfc3339_seconds(std::time::SystemTime::now())
        .to_string()
        .replace([':', 'T', 'Z'], "-");
    PathBuf::from("reports").join(format!("{name}-{}", ts.trim_end_matches('-')))
}

fn progress_line(p: &SeriesPoint) -> String {
    let lat: Vec<String> = p
        .latency
        .iter()
        .map(|(k, l)| format!("{k} p95 {:.1}ms", l.p95_ms))
        .collect();
    format!(
        "t={:>4}s vus={:>6} ops={:>7} err={:>5} cpu={:>3.0}% {}",
        p.t_s,
        p.gauges.get("vus").copied().unwrap_or(0),
        p.ops,
        p.errors,
        p.cpu_max,
        lat.join(" ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes() {
        assert_eq!(exit_code("completed"), 0);
        assert_eq!(exit_code("stopped"), 2);
        assert_eq!(exit_code("interrupted"), 130);
        assert_eq!(exit_code("failed"), 1);
    }

    #[test]
    fn default_out_dir_has_no_colons() {
        let p = default_out("x");
        let s = p.to_string_lossy();
        assert!(s.starts_with("reports/x-20") && !s.contains(':'), "{s}");
    }
}
