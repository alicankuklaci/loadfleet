//! Starts runs: validates the plan (allow-list check #1), picks ready agents, splits shares,
//! persists the run, sends assignments and hands the run to its aggregator task.
use crate::runtask::{run_task, RunCtx, RunEvent};
use crate::share::{split_share, weights_for};
use crate::{AgentView, Hub, HubEvent, ServerConfig, Store};
use loadfleet_metrics::{AgentMeta, RunMeta};
use loadfleet_plan::{AllowList, Limits, Model, Plan};
use loadfleet_protocol::{now_ms, AgentMsg, Assignment, Readiness, ServerMsg};
use parking_lot::Mutex;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, Serialize)]
pub enum StartError {
    Invalid(Vec<String>),
    NotEnoughAgents {
        need: usize,
        ready: usize,
        not_ready: Vec<String>,
    },
    Busy(String),
    ConnLimit {
        agent: String,
        share: u64,
        limit: u64,
    },
    Internal(String),
}

impl std::fmt::Display for StartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StartError::Invalid(e) => write!(f, "invalid plan: {}", e.join("; ")),
            StartError::NotEnoughAgents {
                need,
                ready,
                not_ready,
            } => write!(
                f,
                "need {need} ready agents, have {ready} (not ready: {})",
                not_ready.join(", ")
            ),
            StartError::Busy(id) => write!(f, "run {id} is still active"),
            StartError::ConnLimit {
                agent,
                share,
                limit,
            } => write!(f, "agent {agent} would get {share} VUs, limit {limit}"),
            StartError::Internal(e) => write!(f, "internal error: {e}"),
        }
    }
}

struct Active {
    id: String,
    tx: mpsc::UnboundedSender<RunEvent>,
}

type ActiveSlot = Arc<Mutex<Option<Active>>>;

/// Fresh ids tried when a generated run id is already stored.
const RUN_ID_ATTEMPTS: usize = 5;

/// Holds the coordinator's single active-run slot for one run; dropping it frees the slot
/// (also when the run task panics, so the server can never stay "busy" forever).
pub(crate) struct ActiveGuard {
    slot: ActiveSlot,
    id: String,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        let mut a = self.slot.lock();
        if a.as_ref().is_some_and(|x| x.id == self.id) {
            *a = None;
        }
    }
}

#[derive(Clone)]
pub struct Coordinator {
    hub: Hub,
    store: Store,
    cfg: Arc<ServerConfig>,
    active: ActiveSlot,
}

impl Coordinator {
    pub fn new(hub: Hub, store: Store, cfg: Arc<ServerConfig>) -> Coordinator {
        Coordinator {
            hub,
            store,
            cfg,
            active: Arc::new(Mutex::new(None)),
        }
    }

    pub fn active(&self) -> Option<String> {
        self.active.lock().as_ref().map(|a| a.id.clone())
    }

    pub fn stop_run(&self, run_id: &str, reason: &str) -> bool {
        match self.active.lock().as_ref() {
            Some(a) if a.id == run_id => a.tx.send(RunEvent::Stop(reason.to_string())).is_ok(),
            _ => false,
        }
    }

    /// parse -> validate (server limits and allow-list) -> scenario checks, then prepare.
    pub(crate) fn check(&self, plan_text: &str) -> Result<Plan, StartError> {
        let server_allow = AllowList::parse(&self.cfg.allow)
            .map_err(|e| StartError::Internal(format!("server allow-list: {e}")))?;
        let limits = Limits {
            max_duration: self.cfg.max_duration,
            server_allow: Some(server_allow),
        };
        let mut plan = loadfleet_scenario::check_text(plan_text, &limits)
            .map_err(|errs| StartError::Invalid(errs.iter().map(|e| e.to_string()).collect()))?;
        // relative plan paths resolve against the data dir, never the server's cwd
        loadfleet_scenario::prepare_plan(&mut plan, &self.cfg.data_dir())
            .map_err(|e| StartError::Invalid(vec![e]))?;
        Ok(plan)
    }

    pub async fn start_run(&self, plan_text: &str) -> Result<String, StartError> {
        let plan = self.check(plan_text)?;
        // reserve the single active slot in one lock acquisition
        let run_id = loadfleet_engine::short_id();
        let (tx, rx) = mpsc::unbounded_channel();
        {
            let mut a = self.active.lock();
            if let Some(cur) = a.as_ref() {
                return Err(StartError::Busy(cur.id.clone()));
            }
            *a = Some(Active {
                id: run_id.clone(),
                tx,
            });
        }
        // every early return below drops the guard, which frees the slot again
        let guard = ActiveGuard {
            slot: self.active.clone(),
            id: run_id.clone(),
        };
        self.launch(plan, plan_text, run_id, rx, guard).await
    }

    async fn launch(
        &self,
        plan: Plan,
        plan_text: &str,
        run_id: String,
        rx: mpsc::UnboundedReceiver<RunEvent>,
        mut guard: ActiveGuard,
    ) -> Result<String, StartError> {
        let (ready, not_ready): (Vec<AgentView>, Vec<AgentView>) =
            self.hub.agents().into_iter().partition(|a| {
                a.busy_run.is_none()
                    && matches!(a.readiness, Some(Readiness::Ready) | Some(Readiness::Warn))
            });
        let need = plan.agents.min.max(1);
        if ready.len() < need {
            return Err(StartError::NotEnoughAgents {
                need,
                ready: ready.len(),
                not_ready: not_ready.into_iter().map(|a| a.name).collect(),
            });
        }
        let mut info = Vec::new();
        for a in &ready {
            let sat = self.store.saturation(&a.name).await.ok().flatten();
            info.push((a.name.clone(), a.host.cores, sat));
        }
        let weights =
            weights_for(&plan.agents.weights, &info).map_err(|e| StartError::Invalid(vec![e]))?;
        let per_stage: Vec<Vec<u64>> = plan
            .load
            .stages
            .iter()
            .map(|s| split_share(s.target, &weights))
            .collect();
        let shares: Vec<Vec<u64>> = (0..ready.len())
            .map(|i| per_stage.iter().map(|st| st[i]).collect())
            .collect();
        let max_vus = plan.load.max_vus.unwrap_or(1000);
        let mut offsets = Vec::new();
        let mut next = 0u64;
        for (a, share) in ready.iter().zip(&shares) {
            let peak = match plan.load.model {
                Model::Closed => share.iter().copied().max().unwrap_or(0),
                Model::Open => max_vus,
            };
            let limit = self.cfg.max_conns_per_agent.min(a.max_conns);
            if peak > limit {
                return Err(StartError::ConnLimit {
                    agent: a.name.clone(),
                    share: peak,
                    limit,
                });
            }
            offsets.push(next);
            next += peak;
        }
        let seed = plan.seed.unwrap_or_else(rand::random);
        let start_at = now_ms() + self.cfg.start_delay.as_millis() as i64;
        let mut meta = RunMeta {
            id: run_id.clone(),
            name: plan.name.clone(),
            plan_yaml: plan_text.to_string(),
            version: env!("CARGO_PKG_VERSION").into(),
            seed: Some(seed),
            model: match plan.load.model {
                Model::Closed => "closed",
                Model::Open => "open",
            }
            .into(),
            started_ms: start_at,
            status: "running".into(),
            agents: ready
                .iter()
                .zip(&shares)
                .map(|(a, share)| agent_meta(a, share))
                .collect(),
            ..Default::default()
        };
        // persist first: a store failure must not leave agents loaded
        let run_id = self.create_run(&mut meta, &mut guard).await?;
        let mut assigned = Vec::new();
        for (i, a) in ready.iter().enumerate() {
            let asg = Assignment {
                run_id: run_id.clone(),
                plan: plan.clone(),
                allow: self.cfg.allow.clone(),
                stage_targets: shares[i].clone(),
                vu_offset: offsets[i],
                agent_index: i as u32,
                max_vus,
                seed,
                start_at_server_ms: start_at,
                offset_ms: a.offset_ms.unwrap_or(0.0),
            };
            self.hub.mark_busy(&a.id, Some(run_id.clone()));
            if !self.hub.send(&a.id, ServerMsg::Assign(Box::new(asg))) {
                // never assigned: not part of the run
                self.hub.mark_busy(&a.id, None);
                tracing::warn!(run = %run_id, agent = %a.name, "could not send the assignment");
                meta.missing_agents.push(a.name.clone());
                meta.agents.retain(|m| m.name != a.name);
                continue;
            }
            // the socket that got the Assign; a later Joined with another stamp is a reconnect
            let conn = self
                .hub
                .get(&a.id)
                .map_or(a.connected_ms, |v| v.connected_ms);
            assigned.push((a.id.clone(), conn));
        }
        let ctx = RunCtx {
            plan,
            meta,
            agents: assigned,
            start_at_ms: start_at,
            store: self.store.clone(),
            hub: self.hub.clone(),
            cfg: self.cfg.clone(),
            active: guard,
        };
        tokio::spawn(run_task(ctx, rx));
        Ok(run_id)
    }

    /// Stores the new run. Run ids are 24-bit, so an id already in the store is replaced by
    /// a fresh one (the reserved slot follows) a few times before giving up.
    async fn create_run(
        &self,
        meta: &mut RunMeta,
        guard: &mut ActiveGuard,
    ) -> Result<String, StartError> {
        for _ in 0..RUN_ID_ATTEMPTS {
            let err = match self.store.create_run(meta).await {
                Ok(()) => return Ok(meta.id.clone()),
                Err(e) => e,
            };
            if !self.store.run_exists(&meta.id).await.unwrap_or(false) {
                return Err(StartError::Internal(err.to_string()));
            }
            let fresh = loadfleet_engine::short_id();
            tracing::info!(taken = %meta.id, %fresh, "run id already used; picking another");
            if let Some(a) = self.active.lock().as_mut().filter(|a| a.id == guard.id) {
                a.id = fresh.clone();
            }
            guard.id = fresh.clone();
            meta.id = fresh;
        }
        Err(StartError::Internal(format!(
            "no free run id after {RUN_ID_ATTEMPTS} attempts"
        )))
    }

    /// Routes hub events to the active run; records agents in `agents_seen`.
    pub async fn pump(self, mut rx: mpsc::UnboundedReceiver<HubEvent>) {
        while let Some(ev) = rx.recv().await {
            let tx = self
                .active
                .lock()
                .as_ref()
                .map(|a| (a.id.clone(), a.tx.clone()));
            match ev {
                HubEvent::Joined(id) => {
                    let Some(v) = self.hub.get(&id) else { continue };
                    if let Some((_, tx)) = &tx {
                        let _ = tx.send(RunEvent::Joined {
                            agent: id.clone(),
                            connected_ms: v.connected_ms,
                        });
                    }
                    let info = serde_json::to_string(&v).unwrap_or_default();
                    if let Err(e) = self.store.agent_seen(&v.name, &info, now_ms()).await {
                        tracing::warn!(agent = %v.name, error = %e, "agents_seen update failed");
                    }
                }
                HubEvent::Left(id) => {
                    if let Some((_, tx)) = tx {
                        let _ = tx.send(RunEvent::AgentLeft(id));
                    }
                }
                HubEvent::Msg(id, msg) => {
                    let Some((run, tx)) = tx else { continue };
                    let ev = match msg {
                        AgentMsg::Metrics {
                            run_id,
                            seq,
                            rel_ms,
                            snap,
                            health,
                        } if run_id == run => RunEvent::Metrics {
                            agent: id,
                            seq,
                            rel_ms,
                            snap,
                            health,
                        },
                        AgentMsg::Done { run_id, reason } if run_id == run => {
                            RunEvent::Done { agent: id, reason }
                        }
                        AgentMsg::Failed { run_id, error } if run_id == run => {
                            RunEvent::Failed { agent: id, error }
                        }
                        _ => continue,
                    };
                    let _ = tx.send(ev);
                }
            }
        }
    }
}

fn agent_meta(a: &AgentView, share: &[u64]) -> AgentMeta {
    AgentMeta {
        name: a.name.clone(),
        os: a.host.os.clone(),
        arch: a.host.arch.clone(),
        cores: a.host.cores,
        ip: a.ip.clone(),
        offset_ms: a.offset_ms.unwrap_or(0.0),
        stage_targets: share.to_vec(),
    }
}
