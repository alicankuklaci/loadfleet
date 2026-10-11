//! loadfleet plan: test plan model (YAML/JSON), validation, defaults, target allow-list.
mod allow;
mod checks;
mod model;
mod validate;

pub use allow::AllowList;
pub use model::*;
pub use validate::{validate, FieldError, Limits};

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("plan parse error: {0}")]
    Parse(String),
}

/// Parses a plan from YAML (JSON is valid YAML, so both work).
pub fn parse(text: &str) -> Result<Plan, PlanError> {
    serde_yaml::from_str(text).map_err(|e| PlanError::Parse(e.to_string()))
}

/// Parse + validate in one step: the plan-level half of the shared pipeline
/// (`parse` -> `validate(limits)` -> scenario `check_plan`) every entry point must run.
pub fn parse_and_validate(text: &str, limits: &Limits) -> Result<Plan, Vec<FieldError>> {
    let plan = parse(text).map_err(|e| {
        vec![FieldError {
            field: "plan".into(),
            message: e.to_string(),
        }]
    })?;
    validate(&plan, limits)?;
    Ok(plan)
}
