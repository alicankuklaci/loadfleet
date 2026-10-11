use crate::check_plan;
use loadfleet_plan::{parse_and_validate, FieldError, Limits, Plan, Scenario};
use std::path::Path;

/// The shared entry pipeline every way of starting a run goes through (CLI `run`, server API,
/// panel): `parse` -> `validate(limits)` -> scenario `check_plan`. All errors come back together
/// as field errors; the plan is returned only when all three pass.
pub fn check_text(text: &str, limits: &Limits) -> Result<Plan, Vec<FieldError>> {
    let plan = parse_and_validate(text, limits)?;
    check_plan(&plan)?;
    Ok(plan)
}

/// Resolves data a plan needs before it is run or shipped to agents (e.g. a plugin reading input
/// files named in the plan). Relative paths resolve against `base_dir`: the plan file's
/// directory for `loadfleet run`, the server's data directory (`ServerConfig::data_dir`) for the
/// server. No-op for http/tcp/ws; a plugin type that is not built in is an error.
pub fn prepare_plan(plan: &mut Plan, base_dir: &Path) -> Result<(), String> {
    match plan.scenario {
        Scenario::Http { .. } | Scenario::Tcp { .. } | Scenario::Ws { .. } => Ok(()),
        ref other => {
            let kind = other.kind();
            match crate::plugin::plugin(kind) {
                Some(p) => p.prepare(plan, base_dir),
                None => Err(crate::plugin::missing(kind)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PLAN: &str = r#"
name: t
target: { allow: ["127.0.0.1"] }
scenario: { type: http, http: { url: "URL" } }
load: { stages: [{target: 1, ramp: 0s, hold: 1s}] }
"#;

    #[test]
    fn check_text_runs_all_three_stages() {
        assert!(check_text(
            &PLAN.replace("URL", "http://127.0.0.1:1/"),
            &Limits::default()
        )
        .is_ok());
        let parse = check_text("name: [", &Limits::default()).unwrap_err();
        assert_eq!(parse[0].field, "plan");
        let validate =
            check_text(&PLAN.replace("URL", "http://10.9.9.9/"), &Limits::default()).unwrap_err();
        assert!(
            validate[0].message.contains("not in target.allow"),
            "{validate:?}"
        );
        let tpl = check_text(
            &PLAN.replace("URL", "http://127.0.0.1:1/{{nope"),
            &Limits::default(),
        )
        .unwrap_err();
        assert!(
            tpl.iter().any(|e| e.field == "scenario.http.url"),
            "{tpl:?}"
        );
    }
}
