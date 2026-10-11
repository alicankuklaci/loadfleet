use loadfleet_plan::{parse, validate, Limits};

fn yaml_blocks(md: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur: Option<String> = None;
    for line in md.lines() {
        match (&mut cur, line.trim_start()) {
            (None, l) if l.starts_with("```yaml") => cur = Some(String::new()),
            (Some(b), l) if l.starts_with("```") => {
                out.push(std::mem::take(b));
                cur = None;
            }
            (Some(b), _) => {
                b.push_str(line);
                b.push('\n');
            }
            _ => {}
        }
    }
    out.into_iter()
        .filter(|b| b.trim_start().starts_with("name:"))
        .collect()
}

#[test]
fn every_plan_in_the_docs_is_valid() {
    let root = format!("{}/../..", env!("CARGO_MANIFEST_DIR"));
    let mut n = 0;
    for f in ["README.md", "docs/tr/kullanim.md"] {
        let md = std::fs::read_to_string(format!("{root}/{f}"))
            .unwrap_or_else(|_| panic!("{f} missing"));
        for b in yaml_blocks(&md) {
            let p = parse(&b).unwrap_or_else(|e| panic!("{f}: {e}\n{b}"));
            validate(&p, &Limits::default()).unwrap_or_else(|e| panic!("{f}: {e:?}\n{b}"));
            n += 1;
        }
    }
    assert!(n >= 2, "expected plans in the docs, found {n}");
}
