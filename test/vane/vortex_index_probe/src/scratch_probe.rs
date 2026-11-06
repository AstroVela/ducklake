//! Test-only owner processes and durable collection cutpoints; the coordinator uses real SIGKILL.
use ducklake_vortex_query::scratch::{self, OwnedScratch};
use serde::Deserialize;
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};
use vortex::error::{VortexResult, vortex_bail, vortex_err};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    operation: String,
    data_path: PathBuf,
    parent: PathBuf,
    root: Option<PathBuf>,
    sizes: Vec<usize>,
    max_objects: u64,
    max_bytes: u64,
    stop: Option<String>,
    fail: Option<String>,
}

pub fn run(input: &Path, output: &Path) -> VortexResult<()> {
    let spec: Input = serde_json::from_slice(&fs::read(input)?).map_err(|e| vortex_err!("{e}"))?;
    let root = match &spec.root {
        Some(root) => root.clone(),
        None => scratch::namespace(&spec.parent, &spec.data_path)?,
    };
    let check = |stage: &str| {
        if spec.stop.as_deref() == Some(stage) {
            fs::write(
                output.with_extension("checkpoint.tmp"),
                serde_json::to_vec(&json!({"root":root,"stage":stage}))
                    .map_err(|e| vortex_err!("{e}"))?,
            )?;
            fs::rename(
                output.with_extension("checkpoint.tmp"),
                output.with_extension("checkpoint.json"),
            )?;
            let deadline = Instant::now() + Duration::from_secs(180);
            while !output.with_extension("release").exists() {
                if Instant::now() > deadline {
                    vortex_bail!("Scratch fixture was neither released nor killed");
                }
                thread::sleep(Duration::from_millis(5));
            }
        }
        if spec.fail.as_deref() == Some(stage) {
            vortex_bail!("Injected scratch failure at {stage}");
        }
        Ok(())
    };
    let result = (|| {
        if spec.operation == "owner" {
            if spec.sizes.len() > 16 || spec.sizes.iter().any(|size| *size > 64 * 1024 * 1024) {
                vortex_bail!("Invalid fixture payload sizes");
            }
            let owned = OwnedScratch::create(&spec.parent, &spec.data_path, &check)?;
            let tree = if spec.sizes.is_empty() {
                None
            } else {
                let tree = tempfile::Builder::new()
                    .prefix("fixture-")
                    .tempdir_in(owned.path())?;
                for (i, bytes) in spec.sizes.iter().enumerate() {
                    fs::write(tree.path().join(format!("{i:03}.bin")), vec![0x5a; *bytes])?;
                }
                Some(tree)
            };
            check("payload_ready")?;
            drop(tree);
            owned.close()?;
            Ok(json!({"root":root}))
        } else if spec.operation == "collect" {
            serde_json::to_value(scratch::collect(
                &spec.data_path,
                &root,
                spec.max_objects,
                spec.max_bytes,
                &check,
            )?)
            .map_err(|e| vortex_err!("{e}"))
        } else {
            vortex_bail!("Unknown scratch fixture operation");
        }
    })();
    let report = match result {
        Ok(result) => json!({"ok":true,"result":result}),
        Err(error) => json!({"ok":false,"error":error.to_string()}),
    };
    fs::write(
        output,
        serde_json::to_vec(&report).map_err(|e| vortex_err!("{e}"))?,
    )?;
    Ok(())
}
