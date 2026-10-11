//! Scenario plugins: scenario types that live outside this crate (and may be left out of a
//! build entirely, e.g. a private protocol scenario behind a cargo feature).
//! A binary registers its plugins once at startup; `build`, `check_plan`, `prepare_plan` and
//! the panel templates then dispatch to the plugin whose `kind()` matches `scenario.type`.
use crate::BuildCtx;
use loadfleet_engine::Scenario;
use loadfleet_plan::{FieldError, Plan, Scenario as PlanScenario};
use parking_lot::RwLock;
use std::path::Path;
use std::sync::Arc;

pub trait ScenarioPlugin: Send + Sync + 'static {
    /// The plan's `scenario.type` this plugin serves.
    fn kind(&self) -> &'static str;
    /// Agent side: builds the scenario (and re-checks plan data the agent itself must trust).
    fn build(&self, s: &PlanScenario, ctx: BuildCtx) -> Result<Arc<dyn Scenario>, String>;
    /// Server side: field errors beyond `plan::validate`.
    fn check(&self, _plan: &Plan) -> Vec<FieldError> {
        Vec::new()
    }
    /// Resolves data the plan needs before it is run or shipped (see `prepare_plan`).
    fn prepare(&self, _plan: &mut Plan, _base_dir: &Path) -> Result<(), String> {
        Ok(())
    }
    /// Panel templates: (name, plan YAML).
    fn templates(&self) -> Vec<(&'static str, &'static str)> {
        Vec::new()
    }
}

static PLUGINS: RwLock<Vec<Arc<dyn ScenarioPlugin>>> = RwLock::new(Vec::new());

/// Registers `p`, replacing an earlier plugin of the same kind (so calling it twice is harmless).
pub fn register(p: Arc<dyn ScenarioPlugin>) {
    let mut g = PLUGINS.write();
    g.retain(|x| x.kind() != p.kind());
    g.push(p);
}

pub(crate) fn plugin(kind: &str) -> Option<Arc<dyn ScenarioPlugin>> {
    PLUGINS.read().iter().find(|p| p.kind() == kind).cloned()
}

/// The registered plugins' panel templates, in registration order.
pub fn plugin_templates() -> Vec<(&'static str, &'static str)> {
    PLUGINS.read().iter().flat_map(|p| p.templates()).collect()
}

pub(crate) fn missing(kind: &str) -> String {
    format!("scenario type '{kind}' is not built into this loadfleet")
}
