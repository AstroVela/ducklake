//! Test-only local-store boundaries; opaque storage objects are not ANN qualification.
use bytes::Bytes;
use ducklake_vortex_query::LIMITS;
use ducklake_vortex_query::lease::IndexLease;
use serde_json::{Value, json};
use std::fs;
use std::path::Path;
use vortex::error::{VortexResult, vortex_err};
use vortex_index::store::LocalIndexStore;
use vortex_index::{IndexMetadata, IndexStore, Snapshot, SourceFile};

fn metadata(generation: &str) -> IndexMetadata {
    IndexMetadata {
        format_version: 1,
        name: "capacity".into(),
        generation: generation.into(),
        backend: "test.capacity".into(),
        backend_version: 1,
        snapshot: Snapshot {
            dataset_id: "capacity".into(),
            version: "one".into(),
            schema_fingerprint: "float32:1".into(),
            files: vec![SourceFile {
                id: 1,
                uri: "test:capacity".into(),
                version: "one".into(),
                row_count: 1,
            }],
        },
        fields: vec!["embedding".into()],
        covered_files: vec![1],
        artifacts: vec![],
    }
}

pub async fn run(root: &Path) -> VortexResult<Value> {
    fs::create_dir(root)?;
    let mut cases = vec![];
    for (i, length) in [64 * 1024 * 1024 - 1, 64 * 1024 * 1024, 64 * 1024 * 1024 + 1]
        .into_iter()
        .enumerate()
    {
        let generation = format!("artifact-{i}");
        let store = LocalIndexStore::create(root, &generation, LIMITS)?;
        match store
            .write("payload.bin", Bytes::from(vec![0x5a; length]))
            .await
        {
            Ok(artifact) => {
                let mut meta = metadata(&generation);
                meta.artifacts.push(artifact.clone());
                let descriptor = store.seal(&meta)?;
                let (opened, actual) =
                    LocalIndexStore::open(root, &descriptor, &meta.snapshot, LIMITS)?;
                let bytes = opened.read(&artifact).await?;
                if actual != meta
                    || bytes.len() != length
                    || bytes.iter().any(|value| *value != 0x5a)
                {
                    return Err(vortex_err!("Opaque capacity artifact changed on reopen"));
                }
                cases.push(json!({"kind":"artifact","bytes":length,"accepted":true}));
            }
            Err(error) => {
                if root
                    .join(&generation)
                    .join("artifacts/payload.bin")
                    .exists()
                {
                    return Err(vortex_err!("Rejected artifact left partial bytes"));
                }
                cases.push(json!({"kind":"artifact","bytes":length,"accepted":false,"error":error.to_string()}));
            }
        }
    }
    for (i, length) in [1024 * 1024 - 1, 1024 * 1024, 1024 * 1024 + 1]
        .into_iter()
        .enumerate()
    {
        let generation = format!("manifest-{i}");
        let store = LocalIndexStore::create(root, &generation, LIMITS)?;
        let artifact = store
            .write("payload.bin", Bytes::from_static(b"abc"))
            .await?;
        let mut meta = metadata(&generation);
        meta.artifacts.push(artifact);
        let initial = serde_json::to_vec(&meta)
            .map_err(|e| vortex_err!("{e}"))?
            .len();
        meta.name.push_str(&"x".repeat(length - initial));
        match store.seal(&meta) {
            Ok(descriptor) => {
                let (_, actual) = LocalIndexStore::open(root, &descriptor, &meta.snapshot, LIMITS)?;
                if actual != meta || descriptor.manifest.size != length as u64 {
                    return Err(vortex_err!("Capacity manifest changed on reopen"));
                }
                cases.push(json!({"kind":"manifest","bytes":length,"accepted":true}));
            }
            Err(error) => {
                if root.join(&generation).join("manifest.json").exists() {
                    return Err(vortex_err!("Rejected seal published a manifest"));
                }
                cases.push(json!({"kind":"manifest","bytes":length,"accepted":false,"error":error.to_string()}));
            }
        }
    }
    let store = LocalIndexStore::create(root, "aggregate", LIMITS)?;
    let mut meta = metadata("aggregate");
    for path in ["first.bin", "second.bin"] {
        meta.artifacts.push(
            store
                .write(path, Bytes::from(vec![0x5a; 32 * 1024 * 1024 + 1]))
                .await?,
        );
    }
    let descriptor = store.seal(&meta)?;
    let leases = root.join("leases");
    fs::create_dir(&leases)?;
    let result =
        IndexLease::open_root(root, &descriptor, &meta.snapshot, &leases, 64 * 1024 * 1024).await;
    let error = result
        .err()
        .ok_or_else(|| vortex_err!("Aggregate artifact budget was bypassed"))?;
    cases.push(json!({"kind":"aggregate","bytes":64*1024*1024+2,"accepted":false,"error":error.to_string()}));
    if fs::read_dir(leases)?.next().is_some() {
        return Err(vortex_err!("Rejected aggregate left a partial lease"));
    }
    Ok(
        json!({"cases":cases,"limits":{"artifact":LIMITS.max_artifact_bytes,"manifest":LIMITS.max_manifest_bytes}}),
    )
}
