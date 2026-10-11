use crate::{AllowGuard, Template};
use loadfleet_plan::{FieldError, Plan, Scenario as PlanScenario};
use std::sync::Arc;

pub struct BuildCtx {
    pub guard: AllowGuard,
    pub run_id: String,
    pub agent: String,
    /// Open model in-flight cap; also the default HTTP pool size.
    pub max_vus: u64,
}

pub fn build(
    s: &PlanScenario,
    ctx: BuildCtx,
) -> Result<Arc<dyn loadfleet_engine::Scenario>, String> {
    match s {
        PlanScenario::Http { http } => {
            let pool = http.connections.unwrap_or(ctx.max_vus).max(1) as usize;
            Ok(Arc::new(crate::http::HttpScenario::new(
                http,
                ctx.guard,
                &ctx.run_id,
                &ctx.agent,
                pool,
            )?))
        }
        PlanScenario::Tcp { tcp } => Ok(Arc::new(crate::tcp::TcpScenario::new(
            tcp,
            ctx.guard,
            &ctx.run_id,
            &ctx.agent,
        )?)),
        PlanScenario::Ws { ws } => Ok(Arc::new(crate::ws::WsScenario::new(
            ws,
            ctx.guard,
            &ctx.run_id,
            &ctx.agent,
        )?)),
        other => match crate::plugin::plugin(other.kind()) {
            Some(p) => p.build(other, ctx),
            None => Err(crate::plugin::missing(other.kind())),
        },
    }
}

/// Server-side checks that need scenario knowledge: templates compile (env is NOT resolved here)
/// and the connect host of every URL is static.
pub fn check_plan(p: &Plan) -> Result<(), Vec<FieldError>> {
    let mut errs = Vec::new();
    let mut tpl = |field: &str, text: &str| {
        if let Err(e) = Template::compile(text) {
            errs.push(FieldError {
                field: field.into(),
                message: e,
            });
        }
    };
    let mut host_errs = Vec::new();
    match &p.scenario {
        PlanScenario::Http { http } => {
            tpl("scenario.http.url", &http.url);
            for (k, v) in &http.headers {
                tpl(&format!("scenario.http.headers.{k}"), v);
            }
            if let Some(b) = &http.body {
                tpl("scenario.http.body", b);
            }
            static_host("scenario.http.url", &http.url, &mut host_errs);
        }
        PlanScenario::Ws { ws } => {
            for (k, v) in &ws.headers {
                tpl(&format!("scenario.ws.headers.{k}"), v);
            }
            for (list, steps) in [("on_open", &ws.on_open), ("steps", &ws.steps)] {
                for (i, st) in steps.iter().enumerate() {
                    if let loadfleet_plan::WsStep::Send(t) = st {
                        tpl(&format!("scenario.ws.{list}[{i}]"), t);
                    }
                }
            }
            static_host("scenario.ws.url", &ws.url, &mut host_errs);
        }
        PlanScenario::Tcp { tcp } => {
            if let Some(loadfleet_plan::Payload::Text(t)) = &tcp.send {
                tpl("scenario.tcp.send", t);
            }
        }
        other => match crate::plugin::plugin(other.kind()) {
            Some(plug) => errs.extend(plug.check(p)),
            None => errs.push(FieldError {
                field: "scenario.type".into(),
                message: crate::plugin::missing(other.kind()),
            }),
        },
    }
    errs.extend(host_errs);
    if errs.is_empty() {
        Ok(())
    } else {
        Err(errs)
    }
}

fn static_host(field: &str, url: &str, errs: &mut Vec<FieldError>) {
    let authority = url
        .split("://")
        .nth(1)
        .unwrap_or(url)
        .split(['/', '?'])
        .next()
        .unwrap_or("");
    if authority.contains("{{") {
        errs.push(FieldError {
            field: field.into(),
            message: "the host part must not contain templates".into(),
        });
    }
}
