//! Explicit sealed-generation builder shared with the validation probe.
use crate::{LIMITS, provider};
use bytes::Bytes;
use serde::Serialize;
use std::{fs, io::Write, path::Path, sync::Arc};
use vortex::{
    error::{VortexResult, vortex_bail, vortex_err},
    session::VortexSession,
};
use vortex_index::store::{LocalGeneration, LocalIndexStore};
use vortex_index::{
    IndexBuildRequest, IndexBuilder, IndexMetadata, IndexProvider, IndexSource, IndexStore,
    Snapshot,
};
use vortex_index_spfresh::{SPFRESH_ID, SpFreshBuildLimits, SpFreshIndexBuilder};
pub async fn build_generation(
    source: Arc<dyn IndexSource>,
    field: String,
    dimension: u32,
    root: &Path,
    session: VortexSession,
) -> VortexResult<(Snapshot, LocalGeneration)> {
    let scratch = tempfile::Builder::new()
        .prefix(".scratch-")
        .tempdir_in(root)?;
    let generation = format!(
        "generation-{}",
        scratch
            .path()
            .file_name()
            .ok_or_else(|| vortex_err!("Scratch name required"))?
            .to_string_lossy()
    );
    let store = Arc::new(LocalIndexStore::create(root, &generation, LIMITS)?);
    let metadata = IndexMetadata {
        format_version: 1,
        name: field.clone(),
        generation,
        backend: SPFRESH_ID.into(),
        backend_version: 1,
        snapshot: source.snapshot().clone(),
        fields: vec![field],
        covered_files: source.snapshot().files.iter().map(|file| file.id).collect(),
        artifacts: vec![],
    };
    let limits = SpFreshBuildLimits {
        max_rows: 100_000,
        max_vector_bytes: 64 * 1024 * 1024,
        ..SpFreshBuildLimits::default()
    };
    let builder = SpFreshIndexBuilder::try_new(scratch.path().to_path_buf(), session, limits)?;
    let options = serde_json::json!({"format_version":1, "dimension":dimension,
        "head_count":64, "posting_page_limit":12, "replicas":4});
    let expected = metadata.clone();
    let metadata = builder
        .build(
            IndexBuildRequest {
                metadata,
                backend_options: Bytes::from(
                    serde_json::to_vec(&options).map_err(|error| vortex_err!("{error}"))?,
                ),
            },
            source,
            Arc::clone(&store) as Arc<dyn IndexStore>,
        )
        .await?;
    let mut identity = metadata.clone();
    identity.artifacts.clear();
    if identity != expected {
        vortex_bail!("Provider changed requested identity or coverage");
    }
    let generation = store.seal(&metadata)?;
    let (verified, reopened) =
        LocalIndexStore::open(root, &generation, &metadata.snapshot, LIMITS)?;
    drop(
        provider(scratch.path())?
            .open(&reopened, Arc::new(verified))
            .await?,
    );
    Ok((metadata.snapshot, generation))
}

pub fn write_reference(reference: &Path, descriptor: &impl Serialize) -> VortexResult<()> {
    let root = reference
        .parent()
        .ok_or_else(|| vortex_err!("Reference parent required"))?;
    let mut pending = tempfile::NamedTempFile::new_in(root)?;
    serde_json::to_writer(&mut pending, descriptor).map_err(|error| vortex_err!("{error}"))?;
    pending.flush()?;
    pending.as_file().sync_all()?;
    pending
        .persist_noclobber(reference)
        .map_err(|error| vortex_err!("{error}"))?;
    fs::File::open(root)?.sync_all()?;
    Ok(())
}
