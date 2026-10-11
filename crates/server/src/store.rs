use anyhow::Result;
use loadfleet_metrics::{LatSummary, RunMeta, RunReport, SeriesPoint, StageSummary};
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension, Params};
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY, name TEXT NOT NULL, started_ms INTEGER NOT NULL, status TEXT NOT NULL, meta_json TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS runs_started ON runs(started_ms DESC);
CREATE TABLE IF NOT EXISTS stages (
  run_id TEXT NOT NULL, idx INTEGER NOT NULL, summary_json TEXT NOT NULL, PRIMARY KEY (run_id, idx));
CREATE TABLE IF NOT EXISTS series (
  run_id TEXT NOT NULL, t_s INTEGER NOT NULL, width_s INTEGER NOT NULL DEFAULT 1, point_json TEXT NOT NULL,
  PRIMARY KEY (run_id, t_s));
CREATE TABLE IF NOT EXISTS agents_seen (
  name TEXT PRIMARY KEY, last_ms INTEGER NOT NULL, info_json TEXT NOT NULL, last_saturation_vus INTEGER);
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;
";

/// Bump when the schema changes and add a step to `migrate`.
const SCHEMA_VERSION: i64 = 1;

fn migrate(c: &Connection) -> Result<()> {
    let v: i64 = c.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    anyhow::ensure!(
        v <= SCHEMA_VERSION,
        "database schema v{v} is newer than this build (v{SCHEMA_VERSION})"
    );
    // v0 -> v1: the initial schema, already created by SCHEMA. Future steps go here, in order.
    if v < SCHEMA_VERSION {
        c.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION};"))?;
    }
    Ok(())
}

/// SQLite-backed persistence for runs, stages, time series, seen agents and settings.
/// Cheap to clone; all work runs on the blocking pool behind one connection.
#[derive(Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
}

/// Runs a query returning `(key, json)` rows and decodes the JSON. Rows that fail to decode are
/// logged (table + key) and counted, never hidden: the second value is the number skipped.
fn json_rows<T: DeserializeOwned>(
    c: &Connection,
    table: &str,
    sql: &str,
    p: impl Params,
) -> Result<(Vec<T>, usize)> {
    let mut st = c.prepare(sql)?;
    let rows = st.query_map(p, |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    let (mut out, mut skipped) = (Vec::new(), 0);
    for row in rows {
        let (key, json) = row?;
        match serde_json::from_str(&json) {
            Ok(v) => out.push(v),
            Err(e) => {
                skipped += 1;
                tracing::warn!(table, key, error = %e, "skipping undecodable store row");
            }
        }
    }
    Ok((out, skipped))
}

/// Run metas for `sql` (columns: id, name, started_ms, status, meta_json). A row whose JSON cannot
/// be decoded is returned as a placeholder (verdict "unreadable") instead of vanishing.
fn run_metas(c: &Connection, sql: &str, p: impl Params) -> Result<Vec<RunMeta>> {
    let mut st = c.prepare(sql)?;
    let rows = st.query_map(p, |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, i64>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, String>(4)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (id, name, started_ms, status, json) = row?;
        out.push(serde_json::from_str(&json).unwrap_or_else(|e| {
            tracing::warn!(table = "runs", key = %id, error = %e, "undecodable run row, returning placeholder");
            RunMeta { id, name, started_ms, status, verdict: "unreadable".into(), ..Default::default() }
        }));
    }
    Ok(out)
}

const RUN_COLS: &str = "SELECT id, name, started_ms, status, meta_json FROM runs";

impl Store {
    pub fn open(path: &Path) -> Result<Store> {
        Self::init(Connection::open(path)?)
    }

    pub fn memory() -> Result<Store> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(c: Connection) -> Result<Store> {
        c.execute_batch(SCHEMA)?;
        migrate(&c)?;
        Ok(Store {
            conn: Arc::new(Mutex::new(c)),
        })
    }

    async fn call<R: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<R> + Send + 'static,
    ) -> Result<R> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || f(&mut conn.lock())).await?
    }

    pub async fn create_run(&self, m: &RunMeta) -> Result<()> {
        let (id, name, started, status, json) = (
            m.id.clone(),
            m.name.clone(),
            m.started_ms,
            m.status.clone(),
            serde_json::to_string(m)?,
        );
        self.call(move |c| {
            c.execute(
                "INSERT INTO runs (id, name, started_ms, status, meta_json) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![id, name, started, status, json],
            )?;
            Ok(())
        })
        .await
    }

    /// Whether a run with this id is stored (run ids are short and may collide).
    pub async fn run_exists(&self, run_id: &str) -> Result<bool> {
        let id = run_id.to_string();
        self.call(move |c| {
            Ok(
                c.query_row("SELECT 1 FROM runs WHERE id = ?1", params![id], |_| Ok(()))
                    .optional()?
                    .is_some(),
            )
        })
        .await
    }

    pub async fn update_run(&self, m: &RunMeta) -> Result<()> {
        let (id, status, started, json) = (
            m.id.clone(),
            m.status.clone(),
            m.started_ms,
            serde_json::to_string(m)?,
        );
        self.call(move |c| {
            let n = c.execute(
                "UPDATE runs SET status = ?2, started_ms = ?3, meta_json = ?4 WHERE id = ?1",
                params![id, status, started, json],
            )?;
            anyhow::ensure!(n == 1, "update_run: unknown run {id}");
            Ok(())
        })
        .await
    }

    pub async fn put_stage(&self, run_id: &str, s: &StageSummary) -> Result<()> {
        let (id, idx, json) = (
            run_id.to_string(),
            s.index as i64,
            serde_json::to_string(s)?,
        );
        self.call(move |c| {
            c.execute(
                "INSERT OR REPLACE INTO stages (run_id, idx, summary_json) VALUES (?1, ?2, ?3)",
                params![id, idx, json],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn put_series(&self, run_id: &str, pts: &[SeriesPoint]) -> Result<()> {
        let id = run_id.to_string();
        let rows: Vec<(i64, String)> = pts
            .iter()
            .map(|p| Ok((p.t_s, serde_json::to_string(p)?)))
            .collect::<Result<_>>()?;
        self.call(move |c| {
            let tx = c.transaction()?;
            {
                let mut st = tx.prepare(
                    "INSERT OR REPLACE INTO series (run_id, t_s, width_s, point_json) VALUES (?1, ?2, 1, ?3)",
                )?;
                for (t, j) in &rows {
                    st.execute(params![id, t, j])?;
                }
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Newest first. A run whose stored JSON is unreadable is listed as a placeholder
    /// (verdict "unreadable") rather than hidden.
    pub async fn list_runs(&self, limit: u32) -> Result<Vec<RunMeta>> {
        self.call(move |c| {
            run_metas(
                c,
                &format!("{RUN_COLS} ORDER BY started_ms DESC LIMIT ?1"),
                [limit],
            )
        })
        .await
    }

    pub async fn report(&self, run_id: &str) -> Result<Option<RunReport>> {
        let id = run_id.to_string();
        self.call(move |c| {
            let Some(meta) = run_metas(c, &format!("{RUN_COLS} WHERE id = ?1"), [&id])?.pop() else {
                return Ok(None);
            };
            let (stages, _) = json_rows(
                c,
                "stages",
                "SELECT run_id || ':' || idx, summary_json FROM stages WHERE run_id = ?1 ORDER BY idx",
                [&id],
            )?;
            let (series, _) = json_rows(
                c,
                "series",
                "SELECT run_id || ':' || t_s, point_json FROM series WHERE run_id = ?1 ORDER BY t_s",
                [&id],
            )?;
            Ok(Some(RunReport { meta, stages, series }))
        })
        .await
    }

    pub async fn series_since(&self, run_id: &str, since_s: i64) -> Result<Vec<SeriesPoint>> {
        let id = run_id.to_string();
        self.call(move |c| {
            Ok(json_rows(
                c,
                "series",
                "SELECT run_id || ':' || t_s, point_json FROM series WHERE run_id = ?1 AND t_s > ?2 ORDER BY t_s",
                params![id, since_s],
            )?
            .0)
        })
        .await
    }

    pub async fn agent_seen(&self, name: &str, info_json: &str, last_ms: i64) -> Result<()> {
        let (n, i) = (name.to_string(), info_json.to_string());
        self.call(move |c| {
            c.execute(
                "INSERT INTO agents_seen (name, last_ms, info_json) VALUES (?1, ?2, ?3)
                 ON CONFLICT(name) DO UPDATE SET last_ms = excluded.last_ms, info_json = excluded.info_json",
                params![n, last_ms, i],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn set_saturation(&self, name: &str, vus: u64) -> Result<()> {
        let n = name.to_string();
        self.call(move |c| {
            c.execute(
                "UPDATE agents_seen SET last_saturation_vus = ?2 WHERE name = ?1",
                params![n, i64::try_from(vus).unwrap_or(i64::MAX)],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn saturation(&self, name: &str) -> Result<Option<u64>> {
        let n = name.to_string();
        self.call(move |c| {
            let v: Option<Option<i64>> = c
                .query_row(
                    "SELECT last_saturation_vus FROM agents_seen WHERE name = ?1",
                    [n],
                    |r| r.get(0),
                )
                .optional()?;
            Ok(v.flatten().map(|x| x as u64))
        })
        .await
    }

    pub async fn setting(&self, key: &str) -> Result<Option<String>> {
        let k = key.to_string();
        self.call(move |c| {
            Ok(
                c.query_row("SELECT value FROM settings WHERE key = ?1", [k], |r| {
                    r.get(0)
                })
                .optional()?,
            )
        })
        .await
    }

    pub async fn set_setting(&self, key: &str, value: &str) -> Result<()> {
        let (k, v) = (key.to_string(), value.to_string());
        self.call(move |c| {
            c.execute(
                "INSERT INTO settings (key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params![k, v],
            )?;
            Ok(())
        })
        .await
    }

    /// Marks every `running`/`pending` run as failed (the server died mid-run). Returns how many.
    /// A run whose stored JSON cannot be decoded is still failed (status column only).
    pub async fn fail_unfinished_runs(&self) -> Result<usize> {
        self.call(move |c| {
            let tx = c.transaction()?;
            let ids: Vec<(String, String)> = {
                let mut st = tx.prepare("SELECT id, meta_json FROM runs WHERE status IN ('running', 'pending')")?;
                let rows = st.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
                rows.collect::<rusqlite::Result<_>>()?
            };
            for (id, json) in &ids {
                match serde_json::from_str::<RunMeta>(json) {
                    Ok(mut m) => {
                        m.status = "failed".into();
                        m.verdict = "server restarted during the run".into();
                        tx.execute(
                            "UPDATE runs SET status = 'failed', meta_json = ?2 WHERE id = ?1",
                            params![id, serde_json::to_string(&m)?],
                        )?;
                    }
                    Err(e) => {
                        tracing::warn!(table = "runs", key = %id, error = %e, "failing an unfinished run with unreadable metadata");
                        tx.execute("UPDATE runs SET status = 'failed' WHERE id = ?1", [id])?;
                    }
                }
            }
            tx.commit()?;
            Ok(ids.len())
        })
        .await
    }

    /// Merges 1 s points of finished runs started before `older_than_ms` into 10 s points (ops/errors
    /// summed, gauges/cpu/lag/latency take the bucket maximum). Returns the number of rows replaced.
    /// Runs still `running`/`pending` are never thinned, and a run with any undecodable point is
    /// left untouched. Each run is thinned in its own transaction so a live run is not stalled.
    pub async fn thin_series(&self, older_than_ms: i64) -> Result<usize> {
        self.call(move |c| {
            let ids: Vec<String> = {
                let mut st = c.prepare(
                    "SELECT id FROM runs WHERE started_ms < ?1 AND status NOT IN ('running', 'pending')",
                )?;
                let rows = st.query_map([older_than_ms], |r| r.get(0))?;
                rows.collect::<rusqlite::Result<_>>()?
            };
            let mut replaced = 0;
            for id in ids {
                let tx = c.transaction()?;
                let (pts, skipped): (Vec<SeriesPoint>, usize) = json_rows(
                    &tx,
                    "series",
                    "SELECT run_id || ':' || t_s, point_json FROM series WHERE run_id = ?1 AND width_s = 1 ORDER BY t_s",
                    [&id],
                )?;
                if skipped > 0 {
                    tracing::warn!(run = %id, skipped, "not thinning a run with undecodable series rows");
                    continue;
                }
                if pts.is_empty() {
                    continue;
                }
                let buckets = bucket(&pts);
                tx.execute("DELETE FROM series WHERE run_id = ?1 AND width_s = 1", [&id])?;
                for b in buckets.values() {
                    tx.execute(
                        "INSERT OR REPLACE INTO series (run_id, t_s, width_s, point_json) VALUES (?1, ?2, 10, ?3)",
                        params![id, b.t_s, serde_json::to_string(b)?],
                    )?;
                }
                tx.commit()?;
                replaced += pts.len();
            }
            Ok(replaced)
        })
        .await
    }
}

fn bucket(pts: &[SeriesPoint]) -> BTreeMap<i64, SeriesPoint> {
    let mut buckets: BTreeMap<i64, SeriesPoint> = BTreeMap::new();
    for p in pts {
        let t0 = p.t_s.div_euclid(10) * 10;
        let b = buckets.entry(t0).or_insert_with(|| SeriesPoint {
            t_s: t0,
            ops: 0,
            errors: 0,
            gauges: BTreeMap::new(),
            latency: BTreeMap::new(),
            cpu_max: 0.0,
            lag_p95_max: 0.0,
            agents: 0,
        });
        b.ops += p.ops;
        b.errors += p.errors;
        b.cpu_max = b.cpu_max.max(p.cpu_max);
        b.lag_p95_max = b.lag_p95_max.max(p.lag_p95_max);
        b.agents = b.agents.max(p.agents);
        for (k, v) in &p.gauges {
            let g = b.gauges.entry(k.clone()).or_insert(*v);
            *g = (*g).max(*v);
        }
        for (k, l) in &p.latency {
            // no histograms survive per second, so a thinned point keeps the bucket's worst percentiles
            let e = b
                .latency
                .entry(k.clone())
                .or_insert(LatSummary { n: 0, ..*l });
            e.n += l.n;
            e.p50_ms = e.p50_ms.max(l.p50_ms);
            e.p95_ms = e.p95_ms.max(l.p95_ms);
            e.p99_ms = e.p99_ms.max(l.p99_ms);
            e.max_ms = e.max_ms.max(l.max_ms);
        }
    }
    buckets
}
