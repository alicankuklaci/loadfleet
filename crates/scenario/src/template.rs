#[derive(Debug, Clone, PartialEq)]
enum Part {
    Lit(String),
    Vu,
    Iter,
    Uuid,
    Run,
    Agent,
    Env(String),
}

/// Only environment variables with this prefix can be read by `{{env.NAME}}`: a plan (written by
/// whoever controls the server) must not be able to send an agent host's other variables
/// (cloud credentials, the agent token) to a target.
pub const ENV_VAR_PREFIX: &str = "LOADFLEET_VAR_";

/// `{{vu}} {{iter}} {{uuid}} {{run}} {{agent}} {{env.LOADFLEET_VAR_NAME}}` placeholders.
#[derive(Debug, Clone, PartialEq)]
pub struct Template {
    parts: Vec<Part>,
}

pub struct Vars<'a> {
    pub vu_id: &'a str,
    pub iter: u64,
    pub run: &'a str,
    pub agent: &'a str,
}

impl Template {
    pub fn compile(s: &str) -> Result<Template, String> {
        let mut parts = Vec::new();
        let mut rest = s;
        while let Some(open) = rest.find("{{") {
            if open > 0 {
                parts.push(Part::Lit(rest[..open].to_string()));
            }
            let after = &rest[open + 2..];
            let close = after
                .find("}}")
                .ok_or_else(|| format!("unclosed {{{{ in {s:?}"))?;
            let name = after[..close].trim();
            parts.push(match name {
                "vu" => Part::Vu,
                "iter" => Part::Iter,
                "uuid" => Part::Uuid,
                "run" => Part::Run,
                "agent" => Part::Agent,
                n if n.starts_with("env.") => {
                    let var = &n[4..];
                    if var.len() <= ENV_VAR_PREFIX.len() || !var.starts_with(ENV_VAR_PREFIX) {
                        return Err(format!(
                            "{{{{{n}}}}}: templates can only read agent environment variables \
                             named {ENV_VAR_PREFIX}* (for example {{{{env.{ENV_VAR_PREFIX}API_TOKEN}}}})"
                        ));
                    }
                    Part::Env(var.to_string())
                }
                n => return Err(format!("unknown template variable {{{{{n}}}}}")),
            });
            rest = &after[close + 2..];
        }
        if !rest.is_empty() {
            parts.push(Part::Lit(rest.to_string()));
        }
        Ok(Template { parts })
    }

    /// Resolves `{{env.LOADFLEET_VAR_X}}` from this process' environment (agents only; the
    /// server never does this).
    pub fn with_env(self) -> Result<Template, String> {
        let parts = self
            .parts
            .into_iter()
            .map(|p| match p {
                Part::Env(name) => std::env::var(&name)
                    .map(Part::Lit)
                    .map_err(|_| format!("env {name} is not set on this agent")),
                other => Ok(other),
            })
            .collect::<Result<_, _>>()?;
        Ok(Template { parts })
    }

    pub fn is_static(&self) -> bool {
        self.parts.iter().all(|p| matches!(p, Part::Lit(_)))
    }

    pub fn render(&self, v: &Vars) -> String {
        let mut out = String::new();
        for p in &self.parts {
            match p {
                Part::Lit(s) => out.push_str(s),
                Part::Vu => out.push_str(v.vu_id),
                Part::Iter => out.push_str(&v.iter.to_string()),
                Part::Uuid => out.push_str(&uuid::Uuid::new_v4().to_string()),
                Part::Run => out.push_str(v.run),
                Part::Agent => out.push_str(v.agent),
                Part::Env(name) => out.push_str(&std::env::var(name).unwrap_or_default()),
            }
        }
        out
    }
}
